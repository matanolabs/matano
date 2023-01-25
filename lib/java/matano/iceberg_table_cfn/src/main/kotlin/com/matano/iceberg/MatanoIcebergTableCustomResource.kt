package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonNaming
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.SchemaParser
import org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.expressions.UnboundTerm
import org.apache.iceberg.mapping.MappingUtil
import org.apache.iceberg.mapping.NameMappingParser
import org.apache.iceberg.types.TypeUtil
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

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
        fields.forEachRemaining { (k, v) -> processCfnNode(k, v, node as ObjectNode) }
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

data class MatanoPartitionSpec(
    val column: String,
    val transform: String = "identity",
)

class RelaxedIcebergSchemaDeserializer : StdNodeBasedDeserializer<Schema>(Schema::class.java) {
    override fun convert(root: JsonNode, ctxt: DeserializationContext): Schema {
        convertCfnSchema(root)
        return RelaxedIcebergSchemaParser.fromJson(root)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class MatanoTableRequest(
    val tableName: String,
    @JsonDeserialize(using = RelaxedIcebergSchemaDeserializer::class)
    val schema: Schema,
    val partitions: List<MatanoPartitionSpec> = listOf(),
    val tableProperties: MutableMap<String, String> = mutableMapOf(),
    val glueDatabaseName: String? = null,
)

sealed interface MatanoIcebergTransform {
    companion object {
        object Identity : MatanoIcebergTransform
        object Hour : MatanoIcebergTransform
        class Bucket(val width: Int) : MatanoIcebergTransform
    }
}

interface CFNCustomResource {
    fun handleRequest(event: CloudFormationCustomResourceEvent, context: Context): Map<*, *>? {
        val res = when (event.requestType) {
            "Create" -> create(event, context)
            "Update" -> update(event, context)
            "Delete" -> delete(event, context)
            else -> throw RuntimeException("Unexpected request type: ${event.requestType}.")
        }
        return if (res == null) null else mapper.convertValue(res, Map::class.java)
    }
    fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse?
    fun update(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse?
    fun delete(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        return null
    }

    companion object {
        val mapper = jacksonObjectMapper()
    }
}

class MatanoIcebergTableCustomResource : CFNCustomResource {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val icebergCatalog: Catalog by lazy { createIcebergCatalog() }
    val mapper = CFNCustomResource.mapper

    private val ICEBERG_HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)\\]")

    fun parseTransform(s: String): MatanoIcebergTransform {
        return when {
            s == "identity" -> MatanoIcebergTransform.Companion.Identity
            s == "hour" -> MatanoIcebergTransform.Companion.Hour
            s.startsWith("bucket") -> {
                val widthMatcher = ICEBERG_HAS_WIDTH.matcher(s)
                if (widthMatcher.matches()) {
                    val parsedWidth = widthMatcher.group(2).toInt()
                    MatanoIcebergTransform.Companion.Bucket(parsedWidth)
                } else {
                    throw RuntimeException("Invalid bucket transform: $s")
                }
            }
            else -> throw RuntimeException("Unsupported partition transform: $s")
        }
    }

    fun createIcebergTerm(partition: MatanoPartitionSpec): UnboundTerm<Any> {
        return when (val parsedTransform = parseTransform(partition.transform)) {
            is MatanoIcebergTransform.Companion.Identity -> Expressions.ref(partition.column)
            is MatanoIcebergTransform.Companion.Hour -> Expressions.hour(partition.column)
            is MatanoIcebergTransform.Companion.Bucket -> Expressions.bucket(partition.column, parsedTransform.width)
        }
    }

    fun createIcebergPartitionSpec(partitions: List<MatanoPartitionSpec>, icebergSchema: Schema): PartitionSpec {
        val builder = PartitionSpec.builderFor(icebergSchema)
        // move ts to front
        val newPartitions = partitions.sortedBy { p -> p.column != "ts" }
        for (partition in newPartitions) {
            when (val parsedTransform = parseTransform(partition.transform)) {
                is MatanoIcebergTransform.Companion.Identity -> builder.identity(partition.column)
                is MatanoIcebergTransform.Companion.Hour -> builder.hour(partition.column)
                is MatanoIcebergTransform.Companion.Bucket -> builder.bucket(partition.column, parsedTransform.width)
            }
        }
        return builder.build()
    }

    override fun create(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val requestProps = mapper.convertValue<MatanoTableRequest>(event.resourceProperties)
        val schema = TypeUtil.assignIncreasingFreshIds(requestProps.schema)
        val tableProperties = requestProps.tableProperties
        val mappingJson = NameMappingParser.toJson(MappingUtil.create(schema))
        tableProperties[DEFAULT_NAME_MAPPING] = mappingJson

        // Now technically this should be a part of the logical ID since update requires replacement
        // but we only use it internally for merge-mode enrichment tables so just will ignore prop change on update.
        val namespace = requestProps.glueDatabaseName ?: MATANO_NAMESPACE
        val tableId = TableIdentifier.of(Namespace.of(namespace), requestProps.tableName)
        val partition = if (requestProps.partitions.isEmpty()) PartitionSpec.unpartitioned() else createIcebergPartitionSpec(requestProps.partitions, schema)
        logger.info("Using partition: $partition")
        val table = icebergCatalog.createTable(
            tableId,
            schema,
            partition,
            tableProperties,
        )
        logger.info("Successfully created table.")
        return CfnResponse(PhysicalResourceId = requestProps.tableName)
    }

    override fun update(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val newProps = mapper.convertValue<MatanoTableRequest>(event.resourceProperties)
        val oldProps = mapper.convertValue<MatanoTableRequest>(event.oldResourceProperties)

        val namespace = newProps.glueDatabaseName ?: MATANO_NAMESPACE
        val tableId = TableIdentifier.of(Namespace.of(namespace), newProps.tableName)
        val table = icebergCatalog.loadTable(tableId)
        val existingSchema = table.schema()

        val shouldUpdateSchema = oldProps.schema != newProps.schema

        val oldInputPartitions = oldProps.partitions
        val newInputPartitions = newProps.partitions
        val shouldUpdatePartitions = oldInputPartitions != newInputPartitions
        val tx = table.newTransaction()

        // TODO: is this actually an issue? Rexamine if/when we add user partitions.
//        if (shouldUpdateSchema && shouldUpdatePartitions) {
//            throw RuntimeException("Cannot update schema and partitions in the same operation.")
//        }

        if (shouldUpdateSchema) {
            logger.info("Updating schema of ${table.name()}")

            val highestExistingId = TypeUtil.indexById(existingSchema.asStruct()).keys.max()
            val newIdCounter = AtomicInteger(highestExistingId + 1)
            val resolvedNewSchema = TypeUtil.assignFreshIds(newProps.schema, existingSchema) { newIdCounter.incrementAndGet() }

            logger.info("Using resolved schema:")
            logger.info(SchemaParser.toJson(resolvedNewSchema))

            val updateSchemaReq = tx.updateSchema()
                .unionByNameWith(resolvedNewSchema)
                .setIdentifierFields(resolvedNewSchema.identifierFieldNames())
            val updateSchema = updateSchemaReq.apply()
            updateSchemaReq.commit()

            val mappingJson = NameMappingParser.toJson(MappingUtil.create(updateSchema))
            newProps.tableProperties[DEFAULT_NAME_MAPPING] = mappingJson
        }

        if (shouldUpdatePartitions) {
            logger.info("Updating partitions of ${table.name()}")
            val update = tx.updateSpec()

            val oldFields = oldInputPartitions.toSet()
            val newFields = newInputPartitions.toSet()

            val additions = newFields - oldFields
            val removals = oldFields - newFields

            for (addition in additions) {
                val term = createIcebergTerm(addition)
                update.addField(addition.column, term)
            }
            for (removal in removals) {
                update.removeField(removal.column)
            }
            update.commit()
        }

        val oldProperties = table.properties().toMap().filterKeys { it != DEFAULT_NAME_MAPPING }
        val newProperties = newProps.tableProperties.filterKeys { it != "format-version" }

        if (newProperties != oldProperties) {
            logger.info("Updating table Properties")
            val update = tx.updateProperties()
            val modifications = newProperties.filter { (k, v) -> !oldProperties.containsKey(k) || oldProperties[k] != v }
            val removals = oldProperties.filterKeys { k -> !newProperties.containsKey(k) }

            logger.info("Modifications: $modifications")
            logger.info("Removals: $removals")

            modifications.forEach { (k, v) -> update.set(k, v) }
            removals.keys.forEach { update.remove(it) }
            update.commit()
        }

        tx.commitTransaction()

        return null
    }

    override fun delete(event: CloudFormationCustomResourceEvent, context: Context): CfnResponse? {
        logger.info("Received event: ${mapper.writeValueAsString(event)}")

        val tableName = event.resourceProperties["tableName"] as String
        val tableId = TableIdentifier.of(Namespace.of(MATANO_NAMESPACE), tableName)
        val dropped = try {
            icebergCatalog.dropTable(tableId, false)
        } catch (e: software.amazon.awssdk.services.s3.model.NoSuchKeyException) {
            logger.info("S3 key not found while deleting table, skipping...")
        } catch (e: software.amazon.awssdk.services.glue.model.EntityNotFoundException) {
            logger.info("Glue table not found while deleting table, skipping...")
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
            "glue.skip-archive" to "true",
        )
    }
}
