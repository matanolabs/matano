package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.luben.zstd.ZstdOutputStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.AppendFiles
import org.apache.iceberg.DataFile
import org.apache.iceberg.DataFiles
import org.apache.iceberg.MetricsConfig
import org.apache.iceberg.OverwriteFiles
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.io.ByteArrayOutputStream
import java.io.File
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.stream.Stream
import kotlin.coroutines.coroutineContext

data class EnrichmentConfig(
    val name: String,
    val enrichment_type: String,
    val write_mode: String = "overwrite",
    val schema: JsonNode
)

fun loadEnrichmentConfiguration(): Map<String, EnrichmentConfig> {
    val path = "/opt/config/enrichment"
    val mapper = YAMLMapper().registerModule(kotlinModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val configs = File(path).walk().maxDepth(2).filter { it.isFile && it.endsWith("enrichment.yml") }.map { f ->
        val conf = mapper.readValue<EnrichmentConfig>(f.inputStream())
        Pair(conf.name, conf)
    }.toMap()
    println("Loaded enrichment configurations for enrichment tables: ${configs.keys.toList()}")
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
    val icebergCatalog: Catalog by lazy {
        IcebergMetadataWriter.createIcebergCatalog()
    }
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

    suspend fun syncTable(tableName: String, time: String, conf: EnrichmentConfig) {
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
        // Converting Parquet to Avro in memory had some oddities so just run concurrent Athena unloads.
        // TODO: what happens if iceberg commit fails? eh...
        val dataFile = withContext(coroutineContext) {
            launch { doAvroWrite(tableName, enrichTableName, conf) }
            async { doParquetWrite(tableName, enrichTableName, icebergTable) }
        }.await()

        val currentFiles = icebergTable.newScan().planWith(planExecutorService).planFiles().map { it.file() }
        icebergTable.newRewrite()
            .scanManifestsWith(planExecutorService)
            .validateFromSnapshot(currentSnapshot.snapshotId())
            .rewriteFiles(currentFiles.toSet(), setOf(dataFile))
            .commit()
        logger.info("Completed syncing table: $enrichTableName")
    }

    suspend fun doAvroWrite(tableName: String, enrichTableName: String, conf: EnrichmentConfig) {
        val tempSyncBucket = enrichmentTablesBucket
        val uniquePath = UUID.randomUUID().toString()
        val keyPrefix = "temp-enrich-sync/avro/$uniquePath/"

        // Avro unload doesn't support compression
        val qs = """
            UNLOAD (SELECT * FROM $enrichTableName)
            TO 's3://$tempSyncBucket/$keyPrefix'
            WITH (format = 'AVRO')
        """.trimIndent()
        athenaQueryRunner.runAthenaQuery(qs)

        // Make downloads concurrent
        val futs = s3AsyncClient.listObjectsV2 { r -> r.bucket(tempSyncBucket).prefix(keyPrefix) }.await().contents().map { res ->
            s3AsyncClient
                .getObject({ it.bucket(tempSyncBucket).key(res.key()) }, AsyncResponseTransformer.toBytes())
                .thenApply { it.asInputStream() }
        }

        CompletableFuture.allOf(*futs.toTypedArray()).await()
        val data = futs.map { it.get() }

        val writer = DataFileWriter(GenericDatumWriter<GenericRecord>())
            .setCodec(CodecFactory.zstandardCodec(-3))
            // We aim for ~10ish records in each block as needs to be scanned on lookup.
            .setSyncInterval(2000)
        val avroBaos = ByteArrayOutputStream()
        var started = false

        // Combine all the avro files into one
        for (chunk in data) {
            val reader = DataFileStream(chunk, GenericDatumReader<GenericRecord>())
            // Use schema from file for writer. Need to do on first.
            if (!started) {
                writer.create(reader.schema, avroBaos, "matanoisawesome1".encodeToByteArray())
                started = true
            }
            reader.use { rdr ->
                rdr.forEach { writer.append(it) }
            }
        }
        writer.close()
        val avroBytes = avroBaos.toByteArray()

        val primaryKey: String? = conf.schema.get("primary_key").textValue()

        // Generate index for avro file
        val indexUploadFut = if (primaryKey != null) {
            logger.info("Generating index for table: $tableName")

            val index = generateAvroIndex(avroBytes, primaryKey)

            val indexBytes = ByteArrayOutputStream().let { baos ->
                ZstdOutputStream(baos).use { zstd -> mapper.writeValue(zstd, index) }
                baos.toByteArray()
            }
            val enrichmentTableIndexS3Key = "tables/${tableName}_index.json.zst"
            s3AsyncClient.putObject({ it.bucket(enrichmentTablesBucket).key(enrichmentTableIndexS3Key) }, AsyncRequestBody.fromBytes(indexBytes))
        } else {
            CompletableFuture.completedFuture(null)
        }

        val enrichmentTableS3Key = "tables/$tableName.avro"
        val avroUploadFut = s3AsyncClient.putObject({ it.bucket(enrichmentTablesBucket).key(enrichmentTableS3Key) }, AsyncRequestBody.fromBytes(avroBytes.clone()))

        CompletableFuture.allOf(avroUploadFut, indexUploadFut).await()
    }

    suspend fun doParquetWrite(tableName: String, enrichTableName: String, icebergTable: Table): DataFile? {
        // Run an Athena query to select data (resolve deletes). Iceberg Generics has bug? returns nulls.
        val tempSyncBucket = enrichmentTablesBucket
        val uniquePath = UUID.randomUUID().toString()
        val keyPrefix = "temp-enrich-sync/parquet/$uniquePath/"
        val qs = """
            UNLOAD (SELECT * FROM $enrichTableName)
            TO 's3://$tempSyncBucket/$keyPrefix'
            WITH (format = 'PARQUET', compression='snappy')
        """.trimIndent()
        athenaQueryRunner.runAthenaQuery(qs)

        val futs = s3AsyncClient.listObjectsV2 { r -> r.bucket(tempSyncBucket).prefix(keyPrefix) }.await().contents().map { res ->
            s3AsyncClient
                .getObject({ it.bucket(tempSyncBucket).key(res.key()) }, AsyncResponseTransformer.toBytes())
                .thenApply { InMemoryInputFile(it.asByteArray()) }
        }
        CompletableFuture.allOf(*futs.toTypedArray()).await()
        val inputFiles = futs.map { it.get() }

        val outFile = InMemoryIcebergOutputFile()
        val footer = concatIcebergParquetFiles(inputFiles, outFile)

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
        return dataFile
    }

    private fun concatIcebergParquetFiles(inputFiles: Iterable<InputFile>, outputFile: org.apache.iceberg.io.OutputFile): ParquetMetadata {
        // Get the schema from the file instead of using the table schema to avoid incompatibilities.
        val schema = ParquetFileReader.open(inputFiles.first()).use { rdr -> rdr.footer.fileMetaData.schema }
        val rowGroupSize: Long = 128 * 1024 * 1024
        val writer = ParquetFileWriter(
            ParquetIcebergOutputFile(outputFile),
            schema,
            ParquetFileWriter.Mode.CREATE,
            rowGroupSize,
            0
        )
        writer.start()
        for (inputFile in inputFiles) {
            writer.appendFile(inputFile)
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
