package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.iceberg.*
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.parquet.ParquetUtil
import java.io.InputStream
import java.io.OutputStream
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

data class AlertsIcebergHelperRequest(
    val operation: String,
    val data: Any?
)

data class IcebergCommitRequestItem(
    val old_key: String?,
    val new_key: String,
    val ts_hour: String,
    val file_size_bytes: Int
)

class AlertsIcebergHelper : RequestStreamHandler {
    private val icebergCatalog = createIcebergCatalog()
    private val mapper = jacksonObjectMapper()

    private fun createIcebergCatalog(): Catalog {
        val glueCatalog = GlueCatalog()
            .apply { initialize("glue_catalog", IcebergMetadataWriter.icebergProperties) }
        return CachingCatalog.wrap(glueCatalog)
    }

    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val request = mapper.readValue(input, AlertsIcebergHelperRequest::class.java)
        val ret = when (request.operation) {
            "read_files" -> readFiles()
            "do_commit" -> doCommit(request)
            else -> throw RuntimeException("Invalid operation: ${request.operation}")
        }
        output.use { it?.write(ret.toByteArray()) }
    }

    fun readParquetMetrics(s3Path: String, table: Table): Metrics {
        val inputFile = table.io().newInputFile(s3Path)
        return ParquetUtil.fileMetrics(inputFile, MetricsConfig.forTable(table))
    }

    fun doCommit(req: AlertsIcebergHelperRequest): String {
        val payload: List<IcebergCommitRequestItem> = mapper.convertValue(req.data!!)
        println("Doing commit for: $payload")
        val table = icebergCatalog.loadTable(
            TableIdentifier.of(Namespace.of(IcebergMetadataWriter.MATANO_NAMESPACE), MATANO_ALERTS_TABLE_NAME)
        )
        val transaction = table.newTransaction()
        val partition = table.spec()

        val lakeStorageBucket = System.getenv("MATANO_ICEBERG_BUCKET")
        val lakePath = { key: String -> "s3://$lakeStorageBucket/$key" }

        for (item in payload) {
            val partitionTsHourInt = dtToHours(item.ts_hour)
            val newPath = lakePath(item.new_key)
            val newDataFile = DataFiles.builder(partition)
                .withPartitionPath("ts_hour=$partitionTsHourInt/partition_hour=$partitionTsHourInt")
                // .withPath(newPath)
                // TODO: this call and calls for file sizes could be avoided by passing into lambda
                .withMetrics(readParquetMetrics(newPath, table))
                .withInputFile(table.io().newInputFile(newPath))
                .withFormat("PARQUET")
                .build()
            if (item.old_key != null) {
                val oldPath = lakePath(item.old_key)
                val oldDataFile = DataFiles.builder(partition)
                    .withPartitionPath("ts_hour=$partitionTsHourInt/partition_hour=$partitionTsHourInt")
                    .withInputFile(table.io().newInputFile(oldPath))
                    .withMetrics(readParquetMetrics(oldPath, table)) // TODO: avoid, need to return to Rust and pass back from readFiles
//                        .withPath(lakePath(item.old_path))
                    .withFormat("PARQUET")
                    .build()
                transaction
                    .newOverwrite()
                    .addFile(newDataFile)
                    .deleteFile(oldDataFile)
                    .commit()
            } else {
                transaction
                    .newAppend()
                    .appendFile(newDataFile)
                    .commit()
            }
        }
        transaction.commitTransaction()
        return ""
    }

    fun readFiles(): String {
        val table = icebergCatalog.loadTable(
            TableIdentifier.of(Namespace.of(IcebergMetadataWriter.MATANO_NAMESPACE), MATANO_ALERTS_TABLE_NAME)
        )
        val now = LocalDateTime.now().atOffset(ZoneOffset.UTC)
        val nowHours = now.truncatedTo(ChronoUnit.HOURS)
        val twentyFourHoursAgoEpochHours = (nowHours.minusHours(24).toEpochSecond() / 3600).toInt()
        val nowEpochHours = (nowHours.toEpochSecond() / 3600).toInt()

        val files = table
            .newScan()
            .filter(
                Expressions.and(
                    Expressions.greaterThanOrEqual(Expressions.hour("ts"), twentyFourHoursAgoEpochHours),
                    Expressions.lessThanOrEqual(Expressions.hour("ts"), nowEpochHours)
                )
            )
            .planFiles()
            .map {
                it.file().path()
            }
            .toList()

        return mapper.writeValueAsString(files)
    }

    companion object {
        const val MATANO_ALERTS_TABLE_NAME = "matano_alerts"
    }
}
