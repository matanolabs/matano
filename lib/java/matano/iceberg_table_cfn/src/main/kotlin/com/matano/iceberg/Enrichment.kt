package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.s3.event.S3EventNotification
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.avro.generic.GenericData
import org.apache.iceberg.*
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.DelegatingPositionOutputStream
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import tech.allegro.schema.json2avro.converter.JsonAvroConverter
import java.io.*
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.stream.Stream
import kotlin.collections.List

class InMemoryOutputFile : OutputFile {
    private val baos = ByteArrayOutputStream()

    override fun create(blockSizeHint: Long): PositionOutputStream { // Mode.CREATE calls this method
        return InMemoryPositionOutputStream(baos)
    }

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
        return create(blockSizeHint)
    }
    override fun supportsBlockSize(): Boolean = false
    override fun defaultBlockSize(): Long = 0

    fun toArray(): ByteArray = baos.toByteArray()

    private class InMemoryPositionOutputStream(outputStream: OutputStream?) : DelegatingPositionOutputStream(outputStream) {
        override fun getPos(): Long {
            return (stream as ByteArrayOutputStream).size().toLong()
        }
    }
}

class InMemoryIcebergOutputFile : org.apache.iceberg.io.OutputFile {
    private val baos = ByteArrayOutputStream()
    override fun create(): org.apache.iceberg.io.PositionOutputStream {
        return InMemoryPositionOutputStream(baos)
    }

    override fun createOrOverwrite(): org.apache.iceberg.io.PositionOutputStream {
        return create()
    }

    override fun location(): String? = null

    override fun toInputFile(): InputFile? {
        return null
    }

    fun toArray(): ByteArray = baos.toByteArray()

    private class InMemoryPositionOutputStream(val outputStream: OutputStream) : org.apache.iceberg.io.PositionOutputStream() {
        override fun write(b: Int) {
            outputStream.write(b)
        }

        override fun getPos(): Long {
            return (outputStream as ByteArrayOutputStream).size().toLong()
        }
    }
}

data class EnrichmentConfig(
    val name: String,
    val enrichment_type: String,
    val write_mode: String = "overwrite",
    val schema: JsonNode
)

fun main() {
}

fun loadConfiguration(): Map<String, EnrichmentConfig> {
    val path = "/opt/config/enrichment"
    val mapper = YAMLMapper().registerModule(kotlinModule())
    val configs = File(path).walk().maxDepth(2).filter { it.isFile && it.endsWith("enrichment_table.yml") }.map { f ->
        val conf = mapper.readValue<EnrichmentConfig>(f.inputStream())
        Pair(conf.name, conf)
    }.toMap()
    return configs
}

data class EnrichmentSyncRequest(
    val time: String,
    val table_name: String
)

class EnrichmentSyncerHandler : RequestHandler<SQSEvent, Void?> {
    val syncer = EnrichmentIcebergSyncer()

    override fun handleRequest(event: SQSEvent, context: Context?): Void? {
        syncer.handle(event, loadConfiguration())
        return null
    }
}

/** Periodically sync enrichment Iceberg tables for direct use */
class EnrichmentIcebergSyncer {
    val s3AsyncClient = S3AsyncClient.create()
    val icebergCatalog = MatanoIcebergTableCustomResource.createIcebergCatalog()
    val enrichmentTablesBucket = System.getenv("ENRICHMENT_TABLES_BUCKET")
    val mapper = jacksonObjectMapper()
    val planExecutorService = Executors.newFixedThreadPool(20)

    fun handle(event: SQSEvent, configs: Map<String, EnrichmentConfig>) {
        val records = event.records.map { mapper.readValue<EnrichmentSyncRequest>(it.body) }
        runBlocking {
            for (record in records) {
                launch(Dispatchers.IO) {
                    syncTable(record.table_name, record.time, configs[record.table_name]!!)
                }
            }
        }
    }

    // The ParquetWriter doesn't write a valid Parquet file when we don't write any rows (empty). This hackily fixes.
    val hackParquetWriterEnsureMethod: java.lang.reflect.Method by lazy {
        Class.forName("org.apache.iceberg.parquet.ParquetWriter")
            .getDeclaredMethod("ensureWriterInitialized")
            .apply { isAccessible = true }
    }

    fun syncTable(tableName: String, time: String, conf: EnrichmentConfig) {
        val icebergTable = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(MatanoIcebergTableCustomResource.MATANO_NAMESPACE), "enrich_$tableName"))

        val timeDt = OffsetDateTime.parse(time)
        val fiveMinAgoMillis = timeDt.minusMinutes(5).toEpochSecond() * 1000
        // can get stuck...
        if (icebergTable.currentSnapshot().timestampMillis() < fiveMinAgoMillis) {
            println("Skipping table: $tableName as no updates found.")
            return
        }

        val outFile = InMemoryIcebergOutputFile()
        val appenderFactory = org.apache.iceberg.data.GenericAppenderFactory(icebergTable.schema())
        val writer = appenderFactory.newAppender(outFile, FileFormat.PARQUET)
        writer.use { w ->
            IcebergGenerics.read(icebergTable).build().iterator().use { recs ->
                if (!recs.hasNext()) {
                    hackParquetWriterEnsureMethod.invoke(writer)
                } else {
                    recs.forEach { w.add(it) }
                }
            }
        }

        val icebergS3Path = icebergTable.locationProvider().newDataLocation("${UUID.randomUUID()}.zstd.parquet")
        val (s3Bucket, s3Key) = icebergS3Path.removePrefix("s3://").split('/', limit = 2)
        val enrichmentTablesS3Key = "tables/${icebergTable.name()}.zstd.parquet"

        val dataBody = AsyncRequestBody.fromBytes(outFile.toArray())
        val fut1 = s3AsyncClient.putObject({ it.bucket(s3Bucket).key(s3Key) }, dataBody)
        val fut2 = s3AsyncClient.putObject({ it.bucket(enrichmentTablesBucket).key(enrichmentTablesS3Key) }, dataBody)

        val futs = arrayOf(fut1, fut2)
        CompletableFuture.allOf(*futs).get()

        val dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(icebergS3Path)
            .withFileSizeInBytes(writer.length())
            .withFormat("PARQUET")
            .withMetrics(writer.metrics())
            .build()

        val currentFiles = icebergTable.newScan().planWith(planExecutorService).planFiles().map { it.file() }
        icebergTable.newOverwrite().apply {
            currentFiles.forEach { deleteFile(it) }
        }.addFile(dataFile).commit()
    }
}

class EnrichmentIngestorHandler : RequestHandler<SQSEvent, Void?> {
    val configs = loadConfiguration()
    val ingestor = EnrichmentIngestor(configs)
    override fun handleRequest(event: SQSEvent, context: Context): Void? {
        ingestor.handle(event)
        return null
    }
}

fun ByteArrayOutputStream.toInputStream(): InputStream {
    val inputStream = PipedInputStream()
    PipedOutputStream(inputStream).use { this.writeTo(it) }
    return inputStream
}

data class EnrichmentIngestorRecord(
    val bucket: String,
    val key: String
)

/** Ingest raw enrichment data into Iceberg tables */
class EnrichmentIngestor(
    val configs: Map<String, EnrichmentConfig>
) {
    private val logger = LoggerFactory.getLogger(this::class.java)
    val icebergCatalog = MatanoIcebergTableCustomResource.createIcebergCatalog()
    val jsonAvroConverter = JsonAvroConverter.builder().build()
    val s3AsyncClient = S3AsyncClient.create()
//    val csvMapper = CsvMapper()

    fun handle(event: SQSEvent) {
        val tableRecords = event.records
            .map { S3EventNotification.parseJson(it.body).records[0] }
            .groupBy { it.s3.`object`.urlDecodedKey.split("/")[0] }

        logger.info("Processing ${tableRecords.size} records")
        runBlocking {
            for ((tableName, s3Events) in tableRecords) {
                val records = s3Events.map { EnrichmentIngestorRecord(it.s3.bucket.name, it.s3.`object`.urlDecodedKey) }
                launch(Dispatchers.IO) { doIngest(tableName, records) }
            }
        }
        logger.info("Done successfully")
    }

    fun doIngest(tableName: String, records: List<EnrichmentIngestorRecord>) {
        val baos = ByteArrayOutputStream()
        val futs = records.map { rec ->
            s3AsyncClient.getObject({ it.bucket(rec.bucket).key(rec.key) }, AsyncResponseTransformer.toBytes())
                .thenApply { baos.write(it.asByteArray()) }
        }
        CompletableFuture.allOf(*futs.toTypedArray()).get()
        ingestData(tableName, baos.toInputStream())
    }

    fun ingestData(tableName: String, data: InputStream) {
        val conf = configs[tableName] ?: throw RuntimeException("Invalid table name")
        val icebergTable = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(MatanoIcebergTableCustomResource.MATANO_NAMESPACE), "enrich_$tableName"))
        val avroSchema = AvroSchemaUtil.convert(icebergTable.schema(), tableName)
        val outFile = InMemoryOutputFile()
        val writer = AvroParquetWriter.builder<GenericData.Record>(outFile)
            .withSchema(avroSchema)
            .withCompressionCodec(CompressionCodecName.ZSTD)
            .build()

        writer.use { w ->
            Scanner(data).useDelimiter("\n").forEach { line ->
                w.write(jsonAvroConverter.convertToGenericDataRecord(line.toByteArray(), avroSchema))
            }
        }

        val meta = writer.footer.fileMetaData
        val metrics = ParquetUtil.footerMetrics(ParquetMetadata(meta, listOf()), Stream.empty(), MetricsConfig.forTable(icebergTable))

        val s3Path = icebergTable.locationProvider().newDataLocation("${UUID.randomUUID()}.zstd.parquet")
        val (s3Bucket, s3Key) = s3Path.removePrefix("s3://").split('/', limit = 2)

        val outputBytes = outFile.toArray()
        s3AsyncClient
            .putObject({ it.bucket(s3Bucket).key(s3Key) }, AsyncRequestBody.fromBytes(outputBytes))
            .get()

        val dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(s3Path)
            .withFileSizeInBytes(outputBytes.size.toLong())
            .withFormat("PARQUET")
            .withMetrics(metrics)
            .build()

        if (conf.write_mode == "append") {
            icebergTable
                .newAppend()
                .appendFile(dataFile)
                .commit()
        } else if (conf.write_mode == "overwrite") {
            icebergTable.newOverwrite().apply {
                icebergTable.newScan().planFiles().map { it.file() }.forEach { deleteFile(it) }
            }.addFile(dataFile).commit()
        }
    }
}

data class EnrichmentStaticTableCRProps(
    val staticTablesBucket: String,
    val staticTablesKey: String,
    val tableName: String
)

class EnrichmentStaticTableCR : CFNCustomResource {
    val configs = loadConfiguration()
    val enrichmentIngestor = EnrichmentIngestor(configs)
    val s3AsyncClient = enrichmentIngestor.s3AsyncClient
    val mapper = CFNCustomResource.mapper

    fun createOrUpdate(event: CloudFormationCustomResourceEvent): String {
        val props = mapper.convertValue<EnrichmentStaticTableCRProps>(event.resourceProperties)
        val data = s3AsyncClient.getObject(
            { it.bucket(props.staticTablesBucket).key(props.staticTablesKey) },
            AsyncResponseTransformer.toBytes()
        ).get().asInputStream()

        enrichmentIngestor.ingestData(props.tableName, data)
        return props.tableName
    }

    override fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        val tableName = createOrUpdate(event)
        return CfnResponse(PhysicalResourceId = tableName)
    }

    override fun update(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        createOrUpdate(event)
        return null
    }
}
