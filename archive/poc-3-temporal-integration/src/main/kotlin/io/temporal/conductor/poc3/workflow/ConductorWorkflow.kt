package io.temporal.conductor.poc3.workflow

import io.temporal.conductor.poc3.model.ConductorWorkflowInput
import io.temporal.conductor.poc3.model.ConductorWorkflowOutput
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * Temporal workflow interface for executing Conductor workflow definitions.
 *
 * This workflow runs the Conductor DeciderService scheduling loop within
 * Temporal's durable execution framework.
 */
@WorkflowInterface
interface ConductorWorkflow {
    /**
     * Execute a Conductor workflow definition.
     *
     * @param input Workflow definition, input parameters, and task definitions
     * @return Workflow output including status and task outputs
     */
    @WorkflowMethod
    fun execute(input: ConductorWorkflowInput): ConductorWorkflowOutput

    /**
     * Signal to manually complete a task that's waiting for external input.
     *
     * @param taskRefName Reference name of the task to complete
     * @param output Output data to set on the task
     */
    @SignalMethod
    fun completeTask(taskRefName: String, output: Map<String, Any>)

    /**
     * Query current workflow status.
     *
     * @return Current workflow status information
     */
    @QueryMethod
    fun getStatus(): WorkflowStatusInfo
}

/**
 * Status information returned by the getStatus query.
 */
data class WorkflowStatusInfo(
    val workflowStatus: String = "",
    val completedTasks: List<String> = emptyList(),
    val pendingTasks: List<String> = emptyList(),
    val failedTasks: List<String> = emptyList()
)
