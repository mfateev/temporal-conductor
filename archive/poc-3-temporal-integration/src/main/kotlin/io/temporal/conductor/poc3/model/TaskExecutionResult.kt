package io.temporal.conductor.poc3.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Result from task execution activity.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class TaskExecutionResult @JsonCreator constructor(
    /**
     * Task output data.
     */
    @JsonProperty("output")
    val output: Map<String, Any> = emptyMap(),

    /**
     * Task completion status: COMPLETED, FAILED
     */
    @JsonProperty("status")
    val status: String = "COMPLETED",

    /**
     * Failure reason if status is FAILED.
     */
    @JsonProperty("failureReason")
    val failureReason: String? = null
)
