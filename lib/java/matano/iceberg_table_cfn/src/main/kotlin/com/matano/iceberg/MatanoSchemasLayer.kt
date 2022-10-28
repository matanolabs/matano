package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.lingala.zip4j.ZipFile
import org.apache.iceberg.SchemaParser
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import java.util.*
import kotlin.io.path.createTempDirectory
import kotlin.io.path.writeText

data class SchemasLayerCRProps(val tables: List<String>)

class MatanoSchemasLayerCustomResource {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val icebergCatalog = MatanoIcebergTableCustomResource.createIcebergCatalog()
    val mapper = jacksonObjectMapper()

    val s3: S3Client = S3Client.create()

    fun handleRequest(event: CloudFormationCustomResourceEvent, context: Context): Map<*, *>? {
        val res = when (event.requestType) {
            "Create" -> create(event, context)
            "Update" -> update(event, context)
            "Delete" -> delete(event, context)
            else -> throw RuntimeException("Unexpected request type: ${event.requestType}.")
        }
        return if (res == null) null else mapper.convertValue(res, Map::class.java)
    }

    private fun doWork(logSources: List<String>, outputPath: String) {
        val tempDir = createTempDirectory().resolve("schemas")
        val outFileName = "$outputPath.zip" // UUID.randomUUID().toString() + "zip"
        val outZipFile = ZipFile(tempDir.resolve(outFileName).toFile())

        for (logSource in logSources) {
            val tableId = TableIdentifier.of(Namespace.of(MatanoIcebergTableCustomResource.MATANO_NAMESPACE), logSource)
            val table = icebergCatalog.loadTable(tableId)
            val icebergSchema = table.schema()
            val avroSchema = AvroSchemaUtil.convert(icebergSchema.asStruct(), logSource)
            val parquetSchema = ParquetSchemaUtil.convert(icebergSchema, logSource)

            val logSourceSubDir = tempDir.resolve(logSource)
            logSourceSubDir.toFile().mkdirs()
            logSourceSubDir.resolve("iceberg_schema.json").writeText(SchemaParser.toJson(icebergSchema))
            logSourceSubDir.resolve("avro_schema.avsc").writeText(avroSchema.toString())
            writeParquetSchema(logSourceSubDir.resolve("metadata.parquet").toAbsolutePath().toString(), parquetSchema)
        }
        outZipFile.addFolder(tempDir.toFile())

        s3.putObject({ b -> b.bucket(System.getenv("ASSETS_BUCKET_NAME")).key("$outputPath.zip") }, outZipFile.file.toPath())
    }

    fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")
        val logSources = event.resourceProperties["logSources"] as List<String>
        val schemaOutputPath = event.resourceProperties["schemaOutputPath"] as String

        doWork(logSources, schemaOutputPath)

        val physicalId = UUID.randomUUID().toString()
        logger.info("Returning with physical resource ID: $physicalId")
        return CfnResponse(PhysicalResourceId = physicalId)
    }

    fun update(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val logSources = event.resourceProperties["logSources"] as List<String>
        val schemaOutputPath = event.resourceProperties["schemaOutputPath"] as String

        doWork(logSources, schemaOutputPath)
        return null
    }

    fun delete(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")
        return null
    }
}
