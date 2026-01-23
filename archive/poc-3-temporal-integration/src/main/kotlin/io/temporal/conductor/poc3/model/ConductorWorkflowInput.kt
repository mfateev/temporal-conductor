package io.temporal.conductor.poc3.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Input for ConductorWorkflow execution.
 *
 * Contains the workflow definition, input parameters, and task definitions
 * needed to execute a Conductor workflow in Temporal.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ConductorWorkflowInput @JsonCreator constructor(
    /**
     * Serialized WorkflowDef JSON.
     * Using String to avoid serialization issues with Conductor's complex types.
     */
    @JsonProperty("workflowDefJson")
    val workflowDefJson: String = "",

    /**
     * Workflow input parameters - passed to the first task.
     */
    @JsonProperty("workflowInput")
    val workflowInput: Map<String, Any> = emptyMap(),

    /**
     * Task definitions keyed by task name.
     * Each value is a serialized TaskDef JSON.
     */
    @JsonProperty("taskDefsJson")
    val taskDefsJson: Map<String, String> = emptyMap()
)
