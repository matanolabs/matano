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
import java.util.concurrent.Executors

data class RewriteManifestsRequest(val table_name: String)

class RewriteManifestsHandler : RequestStreamHandler {
    val rewriteManifests = RewriteManifests()
    val mapper = jacksonObjectMapper()
    override fun handleRequest(input: InputStream, output: OutputStream, context: Context?) {
        val event = mapper.readValue<RewriteManifestsRequest>(input)
        rewriteManifests.handle(event)
    }
}

class RewriteManifests {
    private val logger = LoggerFactory.getLogger(this::class.java)

    val icebergCatalog: Catalog by lazy { createIcebergCatalog() }
    private fun createIcebergCatalog(): Catalog {
        val glueCatalog = GlueCatalog().apply { initialize("glue_catalog", IcebergMetadataWriter.icebergProperties) }
        return CachingCatalog.wrap(glueCatalog)
    }
    val planExecutorService = Executors.newFixedThreadPool(30)
    val namespace = Namespace.of(IcebergMetadataWriter.MATANO_NAMESPACE)

    fun handle(event: RewriteManifestsRequest) {
        logger.info("Expiring snapshots for: ${event.table_name}")
        val table = icebergCatalog.loadTable(TableIdentifier.of(namespace, event.table_name))
        if (table.currentSnapshot() == null) {
            logger.info("Empty table, returning.")
            return
        }
        val start = System.currentTimeMillis()

        table
            .rewriteManifests()
            .scanManifestsWith(planExecutorService)
            .rewriteIf { r -> r.length() < 10 * 1024 * 1024 } // 10MB
            .commit()

        val end = System.currentTimeMillis()
        logger.info("Rewrite manifests in ${(end - start) / 1000} seconds")
    }
}
