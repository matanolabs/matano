package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TreeTraversingParser
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.SchemaParser
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import java.io.IOException


fun main() {
    val stuff = MatanoIcebergTableCustomResource::class.java.classLoader.getResource("ecs_iceberg_schema.json")!!.readText()

    println(stuff.subSequence(0, 400))
//    val pp = Paths.get("/home/samrose/workplace/matano/test123.json")
//    val ss11 = Files.readString(pp)
//    val iceSch = SchemaParser.fromJson(ss11)
//    val mitcr = MatanoIcebergTableCustomResource()
//    mitcr.icebergCatalog.createTable(
//            TableIdentifier.of(Namespace.of("matano"), "oilosity"),
//            iceSch,
//            PartitionSpec.unpartitioned()
//    )
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


class MatanoIcebergTableCustomResource {
    val icebergCatalog = createIcebergCatalog()
    val mapper = jacksonObjectMapper()

    fun createIcebergCatalog(): GlueCatalog {
        return GlueCatalog().apply { initialize("glue_catalog", icebergProperties) }
    }

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
        println("1")
        val logSourceName = event.resourceProperties["logSourceName"] as String
        val schemaRaw = event.resourceProperties["schema"] ?: throw RuntimeException("`schema` cannot be null.")
        val inputSchema = readCfnSchema(schemaRaw)
        val icebergSchema = SchemaParser.fromJson(inputSchema)

        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), logSourceName)
        val partition = PartitionSpec.builderFor(icebergSchema).day(TIMESTAMP_COLUMN_NAME).build()
        val table = icebergCatalog.createTable(tableId, icebergSchema, partition, mapOf(
                "format-version" to "2",
        ))
        println("###### Successfully created table.")
        return CfnResponse(PhysicalResourceId = logSourceName)
    }

    fun update(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        println("updating")
        val logSourceName = event.resourceProperties["logSourceName"] as String
        val schemaRaw = event.resourceProperties["schema"] ?: throw RuntimeException("`schema` cannot be null.")
        val inputSchema = readCfnSchema(schemaRaw)
        val requestIcebergSchema = SchemaParser.fromJson(inputSchema)

        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), logSourceName)
        val table = icebergCatalog.loadTable(tableId)
        val updateSchemaReq = table.updateSchema()
                .unionByNameWith(requestIcebergSchema)
                .setIdentifierFields(requestIcebergSchema.identifierFieldNames())
        val updateSchema = updateSchemaReq.apply()
        if (!table.schema().sameSchema(updateSchema)) {
            println("Extending schema of ${table.name()}")
            updateSchemaReq.commit()
        }
        return null
    }

    fun delete(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        val logSourceName = event.resourceProperties["logSourceName"] as String
        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), logSourceName)
        val dropped = try {
            icebergCatalog.dropTable(tableId, false)
        } catch (e: software.amazon.awssdk.services.s3.model.NoSuchKeyException) {
            println("S3 key not found while deleting table, skipping...")
        }
        return null
    }

    companion object {
        private const val MATANO_NAMESPACE = "matano"
        private const val TIMESTAMP_COLUMN_NAME = "ts"
        val icebergProperties = mapOf(
                "catalog-name" to "iceberg",
                "catalog-impl" to "org.apache.iceberg.aws.glue.GlueCatalog",
                "warehouse" to "s3://${System.getenv("MATANO_ICEBERG_BUCKET")}/lake",
                "io-impl" to "org.apache.iceberg.aws.s3.S3FileIO",
                "fs.s3a.endpoint.region" to "eu-central-1",
                "fs.s3a.path.style.access" to "true"
        )
    }
}
