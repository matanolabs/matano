package com.matano.iceberg
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.s3.event.S3EventNotification
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.iceberg.*
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.ParquetUtil
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

class LazyConcurrentMap<K, V>(
    private val compute: (K) -> V,
    private val map: ConcurrentHashMap<K, V> = ConcurrentHashMap()
) : Map<K, V> by map {
    override fun get(key: K): V? = map.getOrPut(key) { compute(key) }
}

class IcebergMetadataHandler : RequestHandler<SQSEvent, Void?> {
    val writer = IcebergMetadataWriter()
    override fun handleRequest(event: SQSEvent, context: Context): Void? {
        writer.handle(event)
        return null
    }
}

fun main(args: Array<String>) {
}

class IcebergMetadataWriter {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val icebergCatalog = createIcebergCatalog()
    inner class TableObj(tableName: String) {
        val table: Table = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), tableName))
        val appendFiles: AppendFiles = table.newAppend()
    }

    private fun createIcebergCatalog(): Catalog {
        val glueCatalog = GlueCatalog()
            .apply { initialize("glue_catalog", icebergProperties) }
        return CachingCatalog.wrap(glueCatalog)
    }

    private fun parseObjectKey(key: String): Pair<String, String> {
        // lake/TABLE_NAME/data/ts_day=2022-07-05/<file>.parquet
        val parts = key.split("/")
        val tableName = parts[1]
        val partitionPath = parts[3]
        logger.info("Using table: $tableName")
        return Pair(tableName, partitionPath)
    }

    fun handle(sqsEvent: SQSEvent) {
        val tableObjs = LazyConcurrentMap<String, TableObj>({ name -> TableObj(name) })
        runBlocking {
            for (record in sqsEvent.records) {
                launch(Dispatchers.IO) { processRecord(record, tableObjs) }
            }
        }
        println("Committing...")
        runBlocking {
            for (tableObj in tableObjs.values) {
                launch(Dispatchers.IO) { tableObj.appendFiles.commit() }
            }
        }
        println("DONE!")
    }

    fun readParquetMetrics(s3Path: String, table: Table): Metrics {
        val inputFile = table.io().newInputFile(s3Path)
        return ParquetUtil.fileMetrics(inputFile, MetricsConfig.forTable(table))
    }

    fun processRecord(sqsMessage: SQSMessage, tableObjs: Map<String, TableObj>) {
        val record = S3EventNotification.parseJson(sqsMessage.body).records[0]
        val s3Bucket = record.s3.bucket.name
        val s3Object = record.s3.`object`
        val s3ObjectKey = s3Object.urlDecodedKey
        val s3ObjectSize = s3Object.sizeAsLong
        val s3Path = "s3://$s3Bucket/$s3ObjectKey"
        println(s3Path)

        val (tableName, partitionPath) = parseObjectKey(s3ObjectKey)

        if (tableName == "matano_alerts") {
            return
        }

        if (checkDuplicate(s3Object.sequencer)) {
            logger.info("Found duplicate SQS message for key: $s3ObjectKey. Skipping...")
            return
        }

        val tableObj = tableObjs[tableName]
        val icebergTable = tableObj!!.table

        val metrics = readParquetMetrics(s3Path, icebergTable)
        val partition = PartitionSpec.builderFor(icebergTable.schema()).day(TIMESTAMP_COLUMN_NAME).build()
        val dataFile = DataFiles.builder(partition)
            .withPartitionPath(partitionPath)
            .withPath(s3Path)
            .withFileSizeInBytes(s3ObjectSize)
            .withFormat("PARQUET")
            .withMetrics(metrics)
            .build()
        tableObj.appendFiles.appendFile(dataFile)
    }

    fun checkDuplicate(sequencer: String): Boolean {
        val expireTime = ((System.currentTimeMillis() / 1000L) + DDB_ITEM_EXPIRE_SECONDS).toString()
        val attrs = mapOf(
            "sequencer" to AttributeValue(sequencer),
            "ttl" to AttributeValue().apply { this.setN(expireTime) }
        )
        val req = PutItemRequest(DUPLICATES_DDB_TABLE_NAME, attrs)
            .apply { this.conditionExpression = "attribute_not_exists(sequencer)" }

        try { ddb.putItem(req) } catch (e: ConditionalCheckFailedException) {
            return true
        }
        return false
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
            "fs.s3a.path.style.access" to "true"
        )
        private val ddb = AmazonDynamoDBClientBuilder.defaultClient()
    }
}
