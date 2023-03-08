package com.matano.iceberg

import org.slf4j.LoggerFactory
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.athena.AthenaAsyncClient
import software.amazon.awssdk.services.athena.model.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class AthenaQueryRunner(val workGroup: String = "matano_system") {
    private val logger = LoggerFactory.getLogger(this::class.java)
    val SLEEP_AMOUNT_MS = 500L
    val delayedExecutor = CompletableFuture.delayedExecutor(SLEEP_AMOUNT_MS, TimeUnit.MILLISECONDS)

    fun runAthenaQuery(qs: String): CompletableFuture<Unit> {
        val queryExecutionContext = QueryExecutionContext.builder()
            .database("matano")
            .build()

        val startQueryExecutionRequest = StartQueryExecutionRequest.builder()
            .queryString(qs)
            .workGroup(workGroup)
            .queryExecutionContext(queryExecutionContext)
            .build()
        return athenaClient.startQueryExecution(startQueryExecutionRequest).thenComposeAsync { resp ->
            waitForQueryToComplete(resp.queryExecutionId())
        }
    }

    private fun waitForQueryToComplete(queryExecutionId: String): CompletableFuture<Unit> {
        val req = GetQueryExecutionRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build()
        return athenaClient.getQueryExecution(req).thenComposeAsync { resp ->
            val status = resp.queryExecution().status()
            when (status.state()) {
                QueryExecutionState.FAILED -> {
                    throw RuntimeException(
                        "The Amazon Athena query failed to run with error message: ${status.stateChangeReason()} ",
                    )
                }
                QueryExecutionState.CANCELLED -> throw RuntimeException("The Amazon Athena query was cancelled.")
                QueryExecutionState.SUCCEEDED -> {
                    logger.info("Successfully completed Athena query.")
                    CompletableFuture.completedFuture(Unit)
                }
                else -> {
                    CompletableFuture.supplyAsync({}, delayedExecutor).thenComposeAsync { waitForQueryToComplete(queryExecutionId) }
                }
            }
        }.thenApplyAsync {}
    }
    companion object {
        private val athenaClient: AthenaAsyncClient = AthenaAsyncClient.builder()
            .httpClientBuilder(NettyNioAsyncHttpClient.builder())
            .build()
    }
}
