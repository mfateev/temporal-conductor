package io.temporal.conductor.poc3.activities

import io.temporal.conductor.poc3.model.TaskExecutionResult
import org.slf4j.LoggerFactory

/**
 * Implementation of TaskExecutionActivities.
 *
 * For POC purposes, this provides mock implementations of task execution.
 * In production, this would integrate with:
 * - Conductor worker polling (for SIMPLE tasks)
 * - HTTP client (for HTTP tasks)
 * - Other task-specific implementations
 */
class TaskExecutionActivitiesImpl : TaskExecutionActivities {

    private val logger = LoggerFactory.getLogger(TaskExecutionActivitiesImpl::class.java)

    override fun executeTask(
        taskName: String,
        taskRefName: String,
        input: Map<String, Any>
    ): TaskExecutionResult {
        logger.info("Executing task: {} (ref: {})", taskName, taskRefName)
        logger.debug("Task input: {}", input)

        return when {
            taskName.contains("http", ignoreCase = true) -> executeHttpTask(input)
            taskName.contains("transform", ignoreCase = true) -> executeTransformTask(input)
            taskName.contains("fail", ignoreCase = true) -> executeFailingTask(input)
            taskName.contains("slow", ignoreCase = true) -> executeSlowTask(input)
            else -> executeSimpleTask(taskName, taskRefName, input)
        }
    }

    private fun executeSimpleTask(
        taskName: String,
        taskRefName: String,
        input: Map<String, Any>
    ): TaskExecutionResult {
        // Default simple task - echo input with metadata
        val output = mutableMapOf<String, Any>(
            "taskName" to taskName,
            "taskRefName" to taskRefName,
            "result" to "completed",
            "processedAt" to System.currentTimeMillis()
        )

        // Pass through input data
        input.forEach { (key, value) ->
            output["input_$key"] = value
        }

        logger.info("Simple task completed: {}", taskRefName)
        return TaskExecutionResult(output = output)
    }

    private fun executeHttpTask(input: Map<String, Any>): TaskExecutionResult {
        // Mock HTTP task execution
        val url = input["url"] as? String ?: input["http_request"]?.let {
            (it as? Map<*, *>)?.get("uri") as? String
        } ?: "http://example.com"

        val method = input["method"] as? String ?: "GET"

        logger.info("Mock HTTP {} request to: {}", method, url)

        return TaskExecutionResult(
            output = mapOf(
                "statusCode" to 200,
                "headers" to mapOf("Content-Type" to "application/json"),
                "body" to mapOf(
                    "message" to "Mock response from $url",
                    "method" to method,
                    "timestamp" to System.currentTimeMillis()
                )
            )
        )
    }

    private fun executeTransformTask(input: Map<String, Any>): TaskExecutionResult {
        // Mock transform task - wrap input in transformed structure
        val transformed = mapOf(
            "original" to input,
            "transformed" to true,
            "transformedAt" to System.currentTimeMillis()
        )

        logger.info("Transform task completed")
        return TaskExecutionResult(output = transformed)
    }

    private fun executeFailingTask(input: Map<String, Any>): TaskExecutionResult {
        // Intentionally failing task for testing error handling
        val shouldFail = input["shouldFail"] as? Boolean ?: true

        return if (shouldFail) {
            logger.warn("Task intentionally failing")
            TaskExecutionResult(
                output = emptyMap(),
                status = "FAILED",
                failureReason = "Intentional failure for testing"
            )
        } else {
            TaskExecutionResult(
                output = mapOf("result" to "recovered")
            )
        }
    }

    private fun executeSlowTask(input: Map<String, Any>): TaskExecutionResult {
        // Slow task for testing timeouts
        val delayMs = (input["delayMs"] as? Number)?.toLong() ?: 1000L

        logger.info("Slow task sleeping for {}ms", delayMs)
        Thread.sleep(delayMs)

        return TaskExecutionResult(
            output = mapOf(
                "sleptFor" to delayMs,
                "completedAt" to System.currentTimeMillis()
            )
        )
    }
}
