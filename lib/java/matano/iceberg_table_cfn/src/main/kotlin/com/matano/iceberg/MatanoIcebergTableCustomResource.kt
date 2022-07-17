package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.SchemaParser
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier

// Cloudformation stringifies all values in properties!
data class IcebergField(
        val name: String,
        val type: String,
        @JsonProperty(access=JsonProperty.Access.READ_ONLY) var id: Int,
        @JsonProperty(access=JsonProperty.Access.READ_ONLY) val required: Boolean = false
)
data class IcebergSchema(
        @JsonProperty(access=JsonProperty.Access.READ_ONLY) val type: String = "struct",
        val fields: List<IcebergField>
) {
    fun parsed(): IcebergSchema {
        return IcebergSchema(fields = fields.mapIndexed { idx, f -> f.copy(id=idx) })
    }
}

@JsonNaming(PropertyNamingStrategies.UpperCamelCaseStrategy::class)
data class CfnResponse(
        val PhysicalResourceId: String? = null,
        val Data: Map<String, String>? = null,
        val NoEcho: Boolean = false,
)

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

    fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        val logSourceName = event.resourceProperties["logSourceName"] as String
        val schemaRaw = mapper.convertValue(event.resourceProperties["schema"], IcebergSchema::class.java)
        println(mapper.writeValueAsString(schemaRaw))
        val parsedSchema = schemaRaw.parsed()
        println(mapper.writeValueAsString(parsedSchema))
        val icebergSchema = SchemaParser.fromJson(mapper.writeValueAsString(parsedSchema))
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
        val schemaRaw = mapper.convertValue(event.resourceProperties["schema"], IcebergSchema::class.java)
        println(mapper.writeValueAsString(schemaRaw))
        val parsedSchema = schemaRaw.parsed()
        println(mapper.writeValueAsString(parsedSchema))
        val requestIcebergSchema = SchemaParser.fromJson(mapper.writeValueAsString(parsedSchema))
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
        private const val TIMESTAMP_COLUMN_NAME = "@timestamp"
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
