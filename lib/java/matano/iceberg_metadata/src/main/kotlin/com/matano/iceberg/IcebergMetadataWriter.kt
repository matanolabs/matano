package com.matano.iceberg
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.s3.event.S3EventNotification
import org.apache.iceberg.*
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.ParquetUtil
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class LazyConcurrentMap<K, V>(
        private val compute: (K) -> V,
        private val map: ConcurrentHashMap<K,V> = ConcurrentHashMap()
): Map<K,V> by map {
    override fun get(key: K): V? = map.getOrPut(key) { compute(key) }
}

class IcebergMetadataHandler : RequestHandler<SQSEvent, Void?> {
    val writer = IcebergMetadataWriter()
    override fun handleRequest(event: SQSEvent, context: Context): Void? {
        println("#####################")
        writer.handle(event)
        println("#####################")
        return null
    }
}

fun main(args: Array<String>) {
}

class IcebergMetadataWriter {
    lateinit var fileIO: S3FileIO
    val icebergCatalog = createIcebergCatalog()

    // val executor = Executors.newFixedThreadPool(8)

    inner class TableObj(tableName: String) {
        val table: Table = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), tableName))
        val appendFiles: AppendFiles = table.newAppend()
    }

    private fun createIcebergCatalog(): Catalog {
        return GlueCatalog()
                .apply { initialize("glue_catalog", icebergProperties) }
                .apply {
                    val fileIOField = GlueCatalog::class.java.getDeclaredField("fileIO")
                    fileIOField.setAccessible(true)
                    fileIO = fileIOField.get(this) as S3FileIO
                }
    }

    private fun getTableNameFromObjectKey(key: String): String {
        // lake/TABLE_NAME/data/partition_day=2022-07-05/<file>.parquet
        // TODO: is assumption right?

        val tableName = key.split("/")[1]
        println("USING TABLE: $tableName")
        return tableName
    }

    fun handle(sqsEvent: SQSEvent) {
        val tableObjs = LazyConcurrentMap<String, TableObj>({ name -> TableObj(name) })
        for (record in sqsEvent.records) {
            processRecord(record, tableObjs)
        }
//        val processFutures = sqsEvent.records.map {record ->
//            CompletableFuture.runAsync({ processRecord(record) }, executor)
//        }
//        CompletableFuture.allOf(*processFutures.toTypedArray()).join()

        println("Committing...")
        for (tableObj in tableObjs.values) {
            tableObj.appendFiles.commit()
        }
//        val futures = tableObjs.map.values.map { tableObj ->
//            CompletableFuture.runAsync({ tableObj.appendFiles.commit() }, executor)
//        }
//        CompletableFuture.allOf(*futures.toTypedArray()).join()
        println("DONE!")
    }

    fun readParquetMetrics(s3Path: String, table: Table): Metrics {
        val inputFile = fileIO.newInputFile(s3Path)
        return ParquetUtil.fileMetrics(inputFile, MetricsConfig.forTable(table))
    }

    fun processRecord(sqsMessage: SQSMessage, tableObjs: Map<String, TableObj>): Unit {
        val record = S3EventNotification.parseJson(sqsMessage.body).records[0]
        val s3Bucket = record.s3.bucket.name
        val s3Object = record.s3.`object`
        val s3ObjectKey = s3Object.key
        val s3ObjectSize = s3Object.sizeAsLong
        val s3Path = "s3://$s3Bucket/$s3ObjectKey"
        println(s3Path)

        if (checkDuplicate(s3Object.sequencer)) {
            println("Found duplicate SQS message for key: ${s3ObjectKey}. Skipping...")
            return
        }

        val tableName = getTableNameFromObjectKey(s3ObjectKey)
        val tableObj = tableObjs[tableName]
        val icebergTable = tableObj!!.table

        val metrics = readParquetMetrics(s3Path, icebergTable)
        val partition = PartitionSpec.builderFor(icebergTable.schema()).day(TIMESTAMP_COLUMN_NAME).build()
        val dataFile = DataFiles.builder(partition)
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

        try { ddb.putItem(req) }
        catch (e: ConditionalCheckFailedException) {
            return true
        }
        return false
    }

    companion object {
        private const val MATANO_NAMESPACE = "matano"
        private const val TIMESTAMP_COLUMN_NAME = "ts"
        private const val DDB_ITEM_EXPIRE_SECONDS = 1 * 24 * 60 * 60
        private val DUPLICATES_DDB_TABLE_NAME = System.getenv("DUPLICATES_DDB_TABLE_NAME")
        private val WAREHOUSE_PATH = "s3://${System.getenv("MATANO_ICEBERG_BUCKET")}/lake"
        val icebergProperties = mapOf(
                "catalog-name" to "iceberg",
                "catalog-impl" to "org.apache.iceberg.aws.glue.GlueCatalog",
                "warehouse" to WAREHOUSE_PATH,
                "io-impl" to "org.apache.iceberg.aws.s3.S3FileIO",
                "fs.s3a.endpoint.region" to "eu-central-1",
                "fs.s3a.path.style.access" to "true"
        )
        private val ddb = AmazonDynamoDBClientBuilder.defaultClient()
    }
}
