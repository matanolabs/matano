package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.iceberg.*
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.*
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.OutputStream
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.stream.Stream

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

// ParquetIO is private...
class ParquetInputStreamAdapter constructor(private val delegate: org.apache.iceberg.io.SeekableInputStream) : DelegatingSeekableInputStream(delegate) {
    override fun getPos() = delegate.pos
    override fun seek(newPos: Long) = delegate.seek(newPos)
}
class ParquetIcebergInputFile(private val inputFile: org.apache.iceberg.io.InputFile) : org.apache.parquet.io.InputFile {
    override fun getLength(): Long = inputFile.length
    override fun newStream(): SeekableInputStream = ParquetInputStreamAdapter(inputFile.newStream())
}
class ParquetOutputStreamAdapter(private val delegate: org.apache.iceberg.io.PositionOutputStream) : DelegatingPositionOutputStream(delegate) {
    override fun getPos() = delegate.pos
}
class ParquetIcebergOutputFile(private val file: org.apache.iceberg.io.OutputFile) : OutputFile {
    override fun create(ignored: Long): PositionOutputStream = ParquetOutputStreamAdapter(file.create())
    override fun createOrOverwrite(ignored: Long): PositionOutputStream = ParquetOutputStreamAdapter(file.createOrOverwrite())

    override fun supportsBlockSize() = false
    override fun defaultBlockSize(): Long = 0
}

data class EnrichmentConfig(
    val name: String,
    val enrichment_type: String,
    val write_mode: String = "overwrite",
    val schema: JsonNode
)

fun loadEnrichmentConfiguration(): Map<String, EnrichmentConfig> {
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
        syncer.handle(event, loadEnrichmentConfiguration())
        return null
    }
}

/** Periodically sync enrichment Iceberg tables for direct use */
class EnrichmentIcebergSyncer {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val s3AsyncClient = S3AsyncClient.create()
    val icebergCatalog = IcebergMetadataWriter.createIcebergCatalog()
    val enrichmentTablesBucket = System.getenv("ENRICHMENT_TABLES_BUCKET") ?: throw RuntimeException("Need enrichment bucket.")
    val mapper = jacksonObjectMapper()
    val planExecutorService = Executors.newFixedThreadPool(20)
    val athenaQueryRunner = AthenaQueryRunner()

    fun handle(event: SQSEvent, configs: Map<String, EnrichmentConfig>) {
        val records = event.records.map { mapper.readValue<EnrichmentSyncRequest>(it.body) }
        runBlocking {
            for (record in records) {
                launch(Dispatchers.IO) {
                    val conf = configs[record.table_name] ?: throw RuntimeException("Invalid table: ${record.table_name}, No config found.")
                    syncTable(record.table_name, record.time, conf)
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

    suspend fun syncTable(tableName: String, time: String, conf: EnrichmentConfig?) {
        val enrichTableName = "enrich_$tableName"
        val icebergTable = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(IcebergMetadataWriter.MATANO_NAMESPACE), enrichTableName))

        val currentSnapshot = icebergTable.currentSnapshot()
        if (currentSnapshot == null) {
            logger.info("Empty table, returning.")
            return
        }

        val timeDt = OffsetDateTime.parse(time)
        val fifteenMinAgoMillis = timeDt.minusMinutes(1).toEpochSecond() * 1000
        // can get stuck...
        if (currentSnapshot.timestampMillis() < fifteenMinAgoMillis) {
            logger.info("Skipping table: $tableName as no updates found.")
            return
        }

        // Run an Athena query to select data (resolve deletes). Iceberg Generics has bug? returns nulls.
        val tempSyncBucket = enrichmentTablesBucket
        val uniquePath = UUID.randomUUID().toString()
        val keyPrefix = "temp-enrich-sync/$uniquePath/"
        val qs = """
            UNLOAD (SELECT * FROM $enrichTableName) 
            TO 's3://$tempSyncBucket/$keyPrefix'
            WITH (format = 'PARQUET', compression='snappy')
        """.trimIndent()
        athenaQueryRunner.runAthenaQuery(qs)
        val filePaths = s3AsyncClient.listObjects { r -> r.bucket(tempSyncBucket).prefix(keyPrefix) }.await().contents().map {
            "s3://$tempSyncBucket/${it.key()}"
        }
        val inputFiles = filePaths.map { icebergTable.io().newInputFile(it) }
        // Athena can write multiple files, concat into one file
        val outFile = InMemoryIcebergOutputFile()
        val footer = concatIcebergParquetFiles(inputFiles, outFile, icebergTable.schema()) // parallelize?

        val icebergS3Path = icebergTable.locationProvider().newDataLocation("${UUID.randomUUID()}.parquet")
        val (s3Bucket, s3Key) = icebergS3Path.removePrefix("s3://").split('/', limit = 2)
        val enrichmentTablesS3Key = "tables/$tableName.parquet"

        val dataBytes = outFile.toArray()
        val dataLength = dataBytes.size
        val dataBody = AsyncRequestBody.fromBytes(dataBytes)

        // Write to Iceberg and enrichment table.
        val fut1 = s3AsyncClient.putObject({ it.bucket(s3Bucket).key(s3Key) }, dataBody)
        val fut2 = s3AsyncClient.putObject({ it.bucket(enrichmentTablesBucket).key(enrichmentTablesS3Key) }, dataBody)

        CompletableFuture.allOf(fut1, fut2).await()

        val metrics = ParquetUtil.footerMetrics(footer, Stream.empty(), MetricsConfig.forTable(icebergTable))
        val dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(icebergS3Path)
            .withFileSizeInBytes(dataLength.toLong())
            .withFormat("PARQUET")
            .withMetrics(metrics)
            .build()

        val currentFiles = icebergTable.newScan().planWith(planExecutorService).planFiles().map { it.file() }
        icebergTable.newRewrite()
            .scanManifestsWith(planExecutorService)
            .validateFromSnapshot(currentSnapshot.snapshotId())
            .rewriteFiles(currentFiles.toSet(), setOf(dataFile))
            .commit()
        logger.info("Completed syncing table: $enrichTableName")
    }

    private fun concatIcebergParquetFiles(inputFiles: Iterable<InputFile>, outputFile: org.apache.iceberg.io.OutputFile, schema: Schema): ParquetMetadata {
        val rowGroupSize: Long = 128 * 1024 * 1024
        val writer = ParquetFileWriter(
            ParquetIcebergOutputFile(outputFile),
            ParquetSchemaUtil.convert(schema, "table"),
            ParquetFileWriter.Mode.CREATE,
            rowGroupSize,
            0
        )
        writer.start()
        for (inputFile in inputFiles) {
            writer.appendFile(ParquetIcebergInputFile(inputFile))
        }
        writer.end(mapOf())
        return writer.footer
    }
}

/** Write Iceberg metadata for enrichment tables */
class EnrichmentMetadataWriter(
    val configs: Map<String, EnrichmentConfig>
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    fun doMetadataWrite(
        icebergTable: Table,
        dataFile: DataFile,
        appendFiles: AppendFiles,
        overwriteFiles: () -> OverwriteFiles
    ) {
        val icebergTableName = icebergTable.name().split(".").last() // remove catalog, db
        val enrichTableName = icebergTableName.removePrefix("enrich_")
        val conf = configs[enrichTableName] ?: throw RuntimeException("Invalid table name: $enrichTableName")
        if (conf.enrichment_type == "static" || conf.write_mode == "overwrite") {
            logger.info("Doing overwrite for enrichment table: $icebergTableName")
            overwriteFiles().apply {
                icebergTable.newScan().planFiles().map { it.file() }.forEach { deleteFile(it) }
            }.addFile(dataFile)
        } else if (conf.write_mode == "append") {
            logger.info("Doing append for enrichment table: $icebergTableName")
            appendFiles.appendFile(dataFile)
        }
    }
}
