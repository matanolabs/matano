package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.lingala.zip4j.ZipFile
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.SchemaParser
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.types.TypeUtil.NextID
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createTempDirectory
import kotlin.io.path.writeText


fun main() {
}

@JsonNaming(PropertyNamingStrategies.UpperCamelCaseStrategy::class)
data class CfnResponse(
        val PhysicalResourceId: String? = null,
        val Data: Map<String, String>? = null,
        val NoEcho: Boolean = false,
)

// Cloudformation stringifies all values in properties!
private fun processCfnNode(path: String, node: JsonNode, parent: ObjectNode) {
    if (node.isObject) {
        val fields = node.fields()
        fields.forEachRemaining { (k,v) -> processCfnNode(k,v, node as ObjectNode) }
    } else if (node.isArray) {
        val elems = node.elements()
        while (elems.hasNext()) {
            processCfnNode(path, elems.next(), parent)
        }
    } else { // value node
        if (node.isTextual and node.asText().equals("true") || node.asText().equals("false")) {
            parent.replace(path, BooleanNode.valueOf(node.asText().toBoolean()))
        } else if (node.isTextual) {
            val maybeNum = node.asText().toIntOrNull()
            if (maybeNum != null) {
                parent.replace(path, IntNode.valueOf(maybeNum))
            }
        }
    }
}

fun convertCfnSchema(node: JsonNode) {
    processCfnNode("", node, node as ObjectNode)
}

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
        val tempDir = createTempDirectory()
        val outFileName = "$outputPath.zip"// UUID.randomUUID().toString() + "zip"
        val outZipFile = ZipFile(tempDir.resolve(outFileName).toFile())

        for (logSource in logSources) {
            val tableId = TableIdentifier.of(Namespace.of(MatanoIcebergTableCustomResource.MATANO_NAMESPACE), logSource)
            val table = icebergCatalog.loadTable(tableId)
            val icebergSchema = table.schema()
            val avroSchema = AvroSchemaUtil.convert(icebergSchema.asStruct(), logSource)

            val logSourceSubDir = tempDir.resolve(logSource)
            logSourceSubDir.toFile().mkdirs()
            logSourceSubDir.resolve("iceberg_schema.json").writeText(SchemaParser.toJson(icebergSchema))
            logSourceSubDir.resolve("avro_schema.avsc").writeText(avroSchema.toString())
            outZipFile.addFolder(logSourceSubDir.toFile())
        }

        s3.putObject({ b -> b.bucket(System.getenv("ASSETS_BUCKET_NAME")).key("$outputPath.zip")}, outZipFile.file.toPath())
    }

    fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")
        val logSources = event.resourceProperties["logSources"] as List<String>
        val schemaOutputPath = event.resourceProperties["schemaOutputPath"] as String

        doWork(logSources, schemaOutputPath)

        val physicalId = UUID.randomUUID().toString()
        logger.info("Returning with physical resource ID: $physicalId")
        return CfnResponse(PhysicalResourceId = physicalId,)
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

class MatanoIcebergTableCustomResource {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val icebergCatalog = createIcebergCatalog()
    val mapper = jacksonObjectMapper()

    fun handleRequest(event: CloudFormationCustomResourceEvent, context: Context): Map<*, *>? {
        val res = when (event.requestType) {
            "Create" -> create(event, context)
            "Update" -> update(event, context)
            "Delete" -> delete(event, context)
            else -> throw RuntimeException("Unexpected request type: ${event.requestType}.")
        }
        return if (res == null) null else mapper.convertValue(res, Map::class.java)
    }

    private fun readCfnSchema(rawValue: Any): JsonNode {
        return mapper.valueToTree<JsonNode>(rawValue).apply { convertCfnSchema(this) }
    }

    fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val logSourceName = event.resourceProperties["logSourceName"] as String
        val schemaRaw = event.resourceProperties["schema"] ?: throw RuntimeException("`schema` cannot be null.")
        val inputSchema = readCfnSchema(schemaRaw)
        val icebergSchema = RelaxedIcebergSchemaParser.fromJson(inputSchema)

        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), logSourceName)
        val partition = PartitionSpec.builderFor(icebergSchema).day(TIMESTAMP_COLUMN_NAME).build()
        val table = icebergCatalog.createTable(tableId, icebergSchema, partition, mapOf(
                "format-version" to "2",
        ))
        logger.info("Successfully created table.")
        return CfnResponse(PhysicalResourceId = logSourceName)
    }

    fun update(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val logSourceName = event.resourceProperties["logSourceName"] as String
        val schemaRaw = event.resourceProperties["schema"] ?: throw RuntimeException("`schema` cannot be null.")
        val inputSchema = readCfnSchema(schemaRaw)
        val requestIcebergSchema = RelaxedIcebergSchemaParser.fromJson(inputSchema)

        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), logSourceName)
        val table = icebergCatalog.loadTable(tableId)
        val existingSchema = table.schema()

        val highestExistingId = TypeUtil.indexById(existingSchema.asStruct()).keys.max()
        val newIdCounter = AtomicInteger(highestExistingId + 1)

        val resolvedNewSchema = TypeUtil.assignFreshIds(requestIcebergSchema, existingSchema) { newIdCounter.incrementAndGet() }
        logger.info("Using resolved schema:")
        logger.info(SchemaParser.toJson(resolvedNewSchema))

        val updateSchemaReq = table.updateSchema()
                .unionByNameWith(resolvedNewSchema)
                .setIdentifierFields(resolvedNewSchema.identifierFieldNames())
        val updateSchema = updateSchemaReq.apply()
        if (!existingSchema.sameSchema(updateSchema)) {
            logger.info("Extending schema of ${table.name()}")
            updateSchemaReq.commit()
        }
        return null
    }

    fun delete(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val logSourceName = event.resourceProperties["logSourceName"] as String
        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), logSourceName)
        val dropped = try {
            icebergCatalog.dropTable(tableId, false)
        } catch (e: software.amazon.awssdk.services.s3.model.NoSuchKeyException) {
            logger.info("S3 key not found while deleting table, skipping...")
        }
        return null
    }

    companion object {
        fun createIcebergCatalog(): GlueCatalog {
            return GlueCatalog().apply { initialize("glue_catalog", icebergProperties) }
        }

        const val MATANO_NAMESPACE = "matano"
        private const val TIMESTAMP_COLUMN_NAME = "ts"
        val icebergProperties = mapOf(
                "catalog-name" to "iceberg",
                "catalog-impl" to "org.apache.iceberg.aws.glue.GlueCatalog",
                "warehouse" to "s3://${System.getenv("MATANO_ICEBERG_BUCKET")}/lake",
                "io-impl" to "org.apache.iceberg.aws.s3.S3FileIO",
                "fs.s3a.path.style.access" to "true"
        )
    }
}
