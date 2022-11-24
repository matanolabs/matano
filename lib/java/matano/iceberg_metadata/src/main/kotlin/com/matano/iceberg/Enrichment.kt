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
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.iceberg.*
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.io.InputFile
import org.apache.parquet.io.DelegatingPositionOutputStream
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.io.*
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

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
    val s3AsyncClient = S3AsyncClient.create()
    val icebergCatalog = IcebergMetadataWriter.createIcebergCatalog()
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
        val enrichTableName = "enrich_$tableName"
        val icebergTable = icebergCatalog.loadTable(TableIdentifier.of(Namespace.of(IcebergMetadataWriter.MATANO_NAMESPACE), enrichTableName))

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
        val enrichmentTablesS3Key = "tables/$enrichTableName.zstd.parquet"

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

        // Unfortunately have to double scan (IceberGenerics.read does one internally but doesn't expose).
        val currentFiles = icebergTable.newScan().planWith(planExecutorService).planFiles().map { it.file() }
        val currentSnapshotId = icebergTable.currentSnapshot().snapshotId()
        icebergTable.newRewrite()
            .scanManifestsWith(planExecutorService)
            .validateFromSnapshot(currentSnapshotId)
            .rewriteFiles(currentFiles.toSet(), setOf(dataFile))
            .commit()
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
