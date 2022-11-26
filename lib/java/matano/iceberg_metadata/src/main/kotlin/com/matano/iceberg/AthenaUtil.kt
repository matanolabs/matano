package com.matano.iceberg

import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import software.amazon.awssdk.services.athena.AthenaAsyncClient
import software.amazon.awssdk.services.athena.model.*

class AthenaQueryRunner {
    private val athenaClient: AthenaAsyncClient = AthenaAsyncClient.builder().build()
    val SLEEP_AMOUNT_MS = 500L

    suspend fun runAthenaQuery(qs: String) {
        val queryExecutionContext = QueryExecutionContext.builder()
            .database("matano")
            .build()

        val startQueryExecutionRequest = StartQueryExecutionRequest.builder()
            .queryString(qs)
            .workGroup("matano")
            .queryExecutionContext(queryExecutionContext)
            .build()
        val startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest).await()
        val queryExecutionId = startQueryExecutionResponse.queryExecutionId()
        waitForQueryToComplete(queryExecutionId)
    }

    private suspend fun waitForQueryToComplete(queryExecutionId: String) {
        val req = GetQueryExecutionRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build()
        var resp: GetQueryExecutionResponse
        var isRunning = true
        while (isRunning) {
            resp = athenaClient.getQueryExecution(req).await()
            val status = resp.queryExecution().status()
            when (status.state()) {
                QueryExecutionState.FAILED -> {
                    throw RuntimeException(
                        "The Amazon Athena query failed to run with error message: ${status.stateChangeReason()} "
                    )
                }
                QueryExecutionState.CANCELLED -> throw RuntimeException("The Amazon Athena query was cancelled.")
                QueryExecutionState.SUCCEEDED -> isRunning = false
                else -> delay(SLEEP_AMOUNT_MS)
            }
        }
    }
}
