package com.matano.iceberg
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.s3.event.S3EventNotification
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.iceberg.*
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.slf4j.LoggerFactory
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.*
import java.util.stream.Stream

class LazyConcurrentMap<K, V>(
    private val compute: (K) -> V,
    private val map: ConcurrentHashMap<K, V> = ConcurrentHashMap(),
) : Map<K, V> by map {
    override fun get(key: K): V? = map.getOrPut(key) { compute(key) }
}

class IcebergMetadataHandler : RequestHandler<SQSEvent, SQSBatchResponse> {
    val writer = IcebergMetadataWriter()

    override fun handleRequest(event: SQSEvent, context: Context?): SQSBatchResponse {
        return writer.handle(event)
    }
}

fun main(args: Array<String>) {
}

fun dtToHours(s: String): Int {
    // s: 2022-10-24-11
    val dt = LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val hours = dt.atOffset(ZoneOffset.UTC).toEpochSecond() / 3600
    return hours.toInt()
}

// Note: This function is highly IO bound. It generally only runs with a few files at a time.
// But we need to be handle a large number of files in parallel in case of redrives, failures, etc.
// JVM concurrency story is a mess, we use Async NIO with Netty to make concurrent requests to read
// the parquet file footers (main IO bottleneck). Iceberg commit cannot use asynchronous IO but is
// table level so not as much of a concern.
// TODO: Performance isn't optimally tuned (see Netty options) for Lambda environment (low memory/CPU) but performs decently well (~15s for 1000 files).
// TODO: Perhaps we could avoid most of the IO altogether by including the footer metrics in the SQS event (depends on the size).
class IcebergMetadataWriter {
    private val logger = LoggerFactory.getLogger(this::class.java)
    val icebergCatalog: Catalog by lazy { createIcebergCatalog() }
    val enrichmentMetadataWriter: EnrichmentMetadataWriter = EnrichmentMetadataWriter(loadEnrichmentConfiguration())

    inner class TableObj(tableName: String) {
        val table: Table = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), tableName))
        val lazyAppendFiles = lazy { table.newAppend() }
        fun getAppendFiles(): AppendFiles = lazyAppendFiles.value

        val lazyOverwriteFiles = lazy { table.newOverwrite() }
        fun getOverwrite(): OverwriteFiles = lazyOverwriteFiles.value

        var sqsMessageIds = mutableSetOf<String>()
    }

    fun parseObjectKey(key: String): Pair<String, String> {
        // lake/TABLE_NAME/data/ts_hour=2022-07-05/<file>.parquet
        val parts = key.split("/")
        val tableName = parts[1]
        val partitionValue = parts[3].substring(8)
        val intPartitionvalue = dtToHours(partitionValue)
        val partitionPath = "ts_hour=$intPartitionvalue"
        return Pair(tableName, partitionPath)
    }

    fun handle(sqsEvent: SQSEvent): SQSBatchResponse {
        val tableObjs = LazyConcurrentMap<String, TableObj>({ name -> TableObj(name) })
        val failures = mutableSetOf<String>()
        val processFutures = mutableListOf<CompletableFuture<Unit>>()

        logger.info("Received ${sqsEvent.records.size} records")

        val s3Records = sqsEvent.records.map { record ->
            val s3EventRecord = S3EventNotification.parseJson(record.body).records[0]
            val tableNameAndPartitionPath = parseObjectKey(s3EventRecord.s3.`object`.urlDecodedKey)
            Triple(record.messageId, s3EventRecord, tableNameAndPartitionPath)
        }

        var start = System.currentTimeMillis()
        for ((messageId, record, tableAndPartition) in s3Records) {
            val (tableName, partitionPath) = tableAndPartition
            if (tableName == "matano_alerts") {
                continue
            }

            val tableObj = tableObjs[tableName]
            if (tableObj == null) {
                logger.error("Table $tableName does not exist")
                failures.add(messageId)
                continue
            }
            tableObj.sqsMessageIds.add(messageId)
            val fut = processRecord(record, tableObj, partitionPath).handleAsync { _, e ->
                if (e != null && e.cause !is NoSuchKeyException) {
                    logger.error(e.message)
                    failures.add(messageId)
                }
            }
            processFutures.add(fut)
        }

        CompletableFuture.allOf(*processFutures.toTypedArray()).join()
        logger.info("Processed ${sqsEvent.records.size} records in ${System.currentTimeMillis() - start} ms")

        logger.info("Committing for tables: ${tableObjs.keys}")
        start = System.currentTimeMillis()
        runBlocking {
            for (tableObj in tableObjs.values) {
                launch(Dispatchers.IO) {
                    try {
                        if (tableObj.lazyAppendFiles.isInitialized()) {
                            tableObj.getAppendFiles().commit()
                        }
                        if (tableObj.lazyOverwriteFiles.isInitialized()) {
                            tableObj.getOverwrite().commit()
                        }
                    } catch (e: Exception) {
                        logger.error(e.message)
                        failures.addAll(tableObj.sqsMessageIds)
                    }
                }
            }
        }

        logger.info("Committed tables in ${System.currentTimeMillis() - start} ms")

        if (failures.isNotEmpty()) {
            logger.error("Encountered ${failures.size} Failures, cleaning up records...")
            val cleanupFutures = failures.map { sequencer -> deleteDuplicateMarker(sequencer) }
            CompletableFuture.allOf(*cleanupFutures.toTypedArray()).join()
        }

        return SQSBatchResponse(failures.map { BatchItemFailure(it) })
    }

    fun readParquetMetrics(s3Bucket: String, s3Key: String, table: Table): CompletableFuture<Metrics> {
        val start = System.currentTimeMillis()
        val ret = s3Client.getObject({ r ->
            r.bucket(s3Bucket).key(s3Key).range("bytes=-8")
        }, AsyncResponseTransformer.toBytes()).thenComposeAsync { r ->
            val footerLength = BytesUtils.readIntLittleEndian(r.asInputStream())
            s3Client.getObject({ r ->
                r.bucket(s3Bucket).key(s3Key).range("bytes=-${footerLength + 8}")
            }, AsyncResponseTransformer.toBytes())
                .thenApplyAsync { r ->
                    val metadata = parquetMetadataConverter.readParquetMetadata(r.asInputStream(), NO_FILTER, null, false, footerLength)
                    val ret = ParquetUtil.footerMetrics(metadata, Stream.empty(), MetricsConfig.forTable(table))
                    logger.debug("Read parquet footer for s3://$s3Bucket/$s3Key in: ${System.currentTimeMillis() - start} ms")
                    ret
                }
        }
        return ret
    }

    fun processRecord(record: S3EventNotificationRecord, tableObj: TableObj, partitionPath: String): CompletableFuture<Unit> {
        val s3Bucket = record.s3.bucket.name
        val s3Object = record.s3.`object`
        val s3ObjectKey = s3Object.urlDecodedKey
        val s3ObjectSize = s3Object.sizeAsLong
        val s3Path = "s3://$s3Bucket/$s3ObjectKey"

        logger.info("Processing record: $s3ObjectKey ($s3ObjectSize bytes) for table: ${tableObj.table.name()}")

        return checkDuplicate(s3Object.sequencer).thenComposeAsync { isDuplicate ->
            if (isDuplicate) {
                logger.info("Found duplicate SQS message for key: $s3ObjectKey. Skipping...")
                return@thenComposeAsync CompletableFuture.completedFuture(Unit)
            }

            val icebergTable = tableObj.table
            val isEnrichmentTable = icebergTable.name().split(".").last().startsWith("enrich_")

            readParquetMetrics(s3Bucket, s3ObjectKey, icebergTable).thenComposeAsync { metrics ->
                val partition = icebergTable.spec()
                val dataFile = DataFiles.builder(partition)
                    .apply {
                        if (partition.isPartitioned) { this.withPartitionPath(partitionPath) }
                    }
                    .withPath(s3Path)
                    .withFileSizeInBytes(s3ObjectSize)
                    .withFormat("PARQUET")
                    .withMetrics(metrics)
                    .build()

                if (isEnrichmentTable) {
                    enrichmentMetadataWriter.doMetadataWrite(icebergCatalog, icebergTable, dataFile, { tableObj.getAppendFiles() }, { tableObj.getOverwrite() })
                } else {
                    tableObj.getAppendFiles().appendFile(dataFile)
                    CompletableFuture.completedFuture(Unit)
                }
            }
        }
    }

    fun checkDuplicate(sequencer: String): CompletableFuture<Boolean> {
        val expireTime = ((System.currentTimeMillis() / 1000L) + DDB_ITEM_EXPIRE_SECONDS).toString()
        val attrs = mapOf(
            "sequencer" to AttributeValue.builder().s(sequencer).build(),
            "ttl" to AttributeValue.builder().n(expireTime).build(),
        )
        val req = PutItemRequest.builder()
            .tableName(DUPLICATES_DDB_TABLE_NAME)
            .item(attrs)
            .conditionExpression("attribute_not_exists(sequencer)")
            .build()

        return ddb.putItem(req).handle { _, e ->
            if (e != null) {
                if (e.cause is ConditionalCheckFailedException) {
                    true
                } else {
                    throw e
                }
            } else {
                false
            }
        }
    }

    private fun deleteDuplicateMarker(sequencer: String): CompletableFuture<Unit> {
        val req = DeleteItemRequest.builder()
            .tableName(DUPLICATES_DDB_TABLE_NAME)
            .key(mapOf("sequencer" to AttributeValue.builder().s(sequencer).build()))
            .build()
        return ddb.deleteItem(req).thenApplyAsync { }
    }

    companion object {
        const val MATANO_NAMESPACE = "matano"
        const val TIMESTAMP_COLUMN_NAME = "ts"
        private const val DDB_ITEM_EXPIRE_SECONDS = 1 * 24 * 60 * 60
        private val DUPLICATES_DDB_TABLE_NAME = System.getenv("DUPLICATES_DDB_TABLE_NAME")
        private val WAREHOUSE_PATH = "s3://${System.getenv("MATANO_ICEBERG_BUCKET")}/lake"
        val icebergProperties = mapOf(
            "catalog-name" to "iceberg",
            "catalog-impl" to "org.apache.iceberg.aws.glue.GlueCatalog",
            "warehouse" to WAREHOUSE_PATH,
            "io-impl" to "org.apache.iceberg.aws.s3.S3FileIO",
            "write.metadata.delete-after-commit.enabled" to "true",
            "glue.skip-archive" to "true",
        )

        val parquetMetadataConverter = ParquetMetadataConverter()

        val ddb = DynamoDbAsyncClient
            .builder()
            .httpClientBuilder(
                NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(500)
                    .maxPendingConnectionAcquires(50000)
                    .connectionAcquisitionTimeout(Duration.ofSeconds(60))
                    .eventLoopGroupBuilder(
                        SdkEventLoopGroup.builder().numberOfThreads(10),
                    )
                    .tcpKeepAlive(true),
            )
            .defaultsMode(DefaultsMode.IN_REGION)
            .build()

        val s3Client = S3AsyncClient
            .builder()
            .httpClientBuilder(
                NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(500)
                    .maxPendingConnectionAcquires(50000)
                    .connectionAcquisitionTimeout(Duration.ofSeconds(60))
                    .eventLoopGroupBuilder(
                        SdkEventLoopGroup.builder().numberOfThreads(10),
                    )
                    .tcpKeepAlive(true),
            )
            .defaultsMode(DefaultsMode.IN_REGION)
            .build()

        fun createIcebergCatalog(): Catalog {
            return GlueCatalog().apply { initialize("glue_catalog", icebergProperties) }
        }
    }
}
