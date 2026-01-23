package io.temporal.conductor.poc3.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Output from ConductorWorkflow execution.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ConductorWorkflowOutput @JsonCreator constructor(
    /**
     * Final workflow status: COMPLETED, FAILED, TIMED_OUT, TERMINATED
     */
    @JsonProperty("status")
    val status: String = "",

    /**
     * Workflow output data from the last task or explicit output.
     */
    @JsonProperty("output")
    val output: Map<String, Any> = emptyMap(),

    /**
     * Reason for failure if status is FAILED or TIMED_OUT.
     */
    @JsonProperty("failureReason")
    val failureReason: String? = null,

    /**
     * Task outputs keyed by reference name.
     * Useful for testing to verify which tasks executed.
     */
    @JsonProperty("taskOutputs")
    val taskOutputs: Map<String, Map<String, Any>> = emptyMap()
)
