package com.matano.iceberg

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.fasterxml.jackson.module.kotlin.*
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.OutputStream
import java.time.OffsetDateTime
import java.util.concurrent.Executors

data class ExpireSnapshotsRequest(val time: String, val table_name: String)

class ExpireSnapshotsHandler : RequestStreamHandler {
    val expireSnapshots = ExpireSnapshots()
    val mapper = jacksonObjectMapper()
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val event = mapper.readValue<ExpireSnapshotsRequest>(input!!)
        expireSnapshots.handle(event)
    }
}

class ExpireSnapshots {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val icebergCatalog = createIcebergCatalog()
    private fun createIcebergCatalog(): Catalog {
        System.setProperty("http-client.type", "apache")
        val glueCatalog = GlueCatalog().apply { initialize("glue_catalog", IcebergMetadataWriter.icebergProperties) }
        return CachingCatalog.wrap(glueCatalog)
    }
    val planExecutorService = Executors.newFixedThreadPool(100)
    val expireExecutorService = Executors.newFixedThreadPool(100)
    val namespace = Namespace.of(IcebergMetadataWriter.MATANO_NAMESPACE)

    fun handle(event: ExpireSnapshotsRequest) {
        logger.info("Expiring snapshots for: $event")
        val dt = OffsetDateTime.parse(event.time)
        val table = icebergCatalog.loadTable(TableIdentifier.of(namespace, event.table_name))

        val dtMark = dt.minusDays(1).toInstant().toEpochMilli()
        val start = System.currentTimeMillis()
        table
            .expireSnapshots()
            .expireOlderThan(dtMark)
            .planWith(planExecutorService)
            .executeDeleteWith(expireExecutorService)
            .commit()
        val end = System.currentTimeMillis()
        logger.info("Expired snapshots in ${(end - start) / 1000} seconds")
    }
}
