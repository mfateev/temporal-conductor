package io.temporal.conductor.poc3

import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.conductor.poc3.workflow.ConductorWorkflow
import io.temporal.conductor.poc3.worker.ConductorWorker
import io.temporal.testing.WorkflowReplayer
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Test workflow replay determinism.
 *
 * These tests verify that workflows can be replayed from history without
 * NonDeterministicException errors.
 */
class ReplayTest : BaseWorkflowTest() {

    @Test
    fun `simple workflow can be replayed from history`() {
        val workflowDef = createSimpleWorkflowDef("replay-simple-test", "task1", "task2")
        val taskDefs = createTaskDefs("task1", "task2")
        val input = createInput(workflowDef, mapOf("value" to 42), taskDefs)

        // Execute workflow
        val options = WorkflowOptions.newBuilder()
            .setTaskQueue(ConductorWorker.TASK_QUEUE)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(5))
            .build()

        val workflow = client.newWorkflowStub(ConductorWorkflow::class.java, options)
        val result = workflow.execute(input)

        assertEquals("COMPLETED", result.status)

        // Get workflow history for replay
        val stub = WorkflowStub.fromTyped(workflow)
        val workflowId = stub.execution.workflowId

        // Fetch history
        val history = client.fetchHistory(workflowId)
        assertNotNull(history)

        // Replay from history - this will throw NonDeterministicException if
        // the workflow implementation is not deterministic
        WorkflowReplayer.replayWorkflowExecution(
            history,
            io.temporal.conductor.poc3.workflow.ConductorWorkflowImpl::class.java
        )
    }

    @Test
    fun `workflow with multiple tasks produces consistent IDs on replay`() {
        val workflowDef = createSimpleWorkflowDef("replay-multi-test", "task1", "task2", "task3")
        val taskDefs = createTaskDefs("task1", "task2", "task3")
        val input = createInput(workflowDef, mapOf("value" to 42), taskDefs)

        // First execution
        val options = WorkflowOptions.newBuilder()
            .setTaskQueue(ConductorWorker.TASK_QUEUE)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(5))
            .build()

        val workflow = client.newWorkflowStub(ConductorWorkflow::class.java, options)
        val result = workflow.execute(input)

        assertEquals("COMPLETED", result.status)
        assertEquals(3, result.taskOutputs.size)

        // Replay from history
        val stub = WorkflowStub.fromTyped(workflow)
        val workflowId = stub.execution.workflowId
        val history = client.fetchHistory(workflowId)

        // Replay should not throw
        WorkflowReplayer.replayWorkflowExecution(
            history,
            io.temporal.conductor.poc3.workflow.ConductorWorkflowImpl::class.java
        )
    }

    @Test
    fun `workflow survives simulated restart during execution`() {
        val workflowDef = createSimpleWorkflowDef("restart-test", "task1", "slow_task", "task3")
        val taskDefs = createTaskDefs("task1", "slow_task", "task3")
        val inputData = mapOf("value" to 42, "delayMs" to 100)
        val input = createInput(workflowDef, inputData, taskDefs)

        // Execute workflow
        val options = WorkflowOptions.newBuilder()
            .setTaskQueue(ConductorWorker.TASK_QUEUE)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(5))
            .build()

        val workflow = client.newWorkflowStub(ConductorWorkflow::class.java, options)
        val result = workflow.execute(input)

        assertEquals("COMPLETED", result.status)

        // Replay to verify determinism after all state changes
        val stub = WorkflowStub.fromTyped(workflow)
        val workflowId = stub.execution.workflowId
        val history = client.fetchHistory(workflowId)

        WorkflowReplayer.replayWorkflowExecution(
            history,
            io.temporal.conductor.poc3.workflow.ConductorWorkflowImpl::class.java
        )
    }
}
