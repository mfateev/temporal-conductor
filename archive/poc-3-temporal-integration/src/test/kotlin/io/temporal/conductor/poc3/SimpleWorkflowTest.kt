package io.temporal.conductor.poc3

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Test simple sequential workflow execution.
 */
class SimpleWorkflowTest : BaseWorkflowTest() {

    @Test
    fun `single task workflow completes`() {
        val workflowDef = createSimpleWorkflowDef("single-task-test", "task1")
        val taskDefs = createTaskDefs("task1")
        val input = createInput(workflowDef, mapOf("value" to 42), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("t0"))
    }

    @Test
    fun `two sequential tasks complete in order`() {
        val workflowDef = createSimpleWorkflowDef("two-task-test", "task1", "task2")
        val taskDefs = createTaskDefs("task1", "task2")
        val input = createInput(workflowDef, mapOf("value" to 42), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("t0"))
        assertTrue(result.taskOutputs.containsKey("t1"))
    }

    @Test
    fun `three sequential tasks complete in order`() {
        val workflowDef = createSimpleWorkflowDef("three-task-test", "task1", "task2", "task3")
        val taskDefs = createTaskDefs("task1", "task2", "task3")
        val input = createInput(workflowDef, mapOf("value" to 42), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertEquals(3, result.taskOutputs.size)
    }

    @Test
    fun `workflow with no tasks completes`() {
        val workflowDef = createSimpleWorkflowDef("empty-test")
        val input = createInput(workflowDef)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
    }

    @Test
    fun `workflow input is passed to first task`() {
        val workflowDef = createSimpleWorkflowDef("input-test", "task1")
        val taskDefs = createTaskDefs("task1")
        val input = createInput(
            workflowDef,
            mapOf("key1" to "value1", "key2" to 123),
            taskDefs
        )

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        // Task output should contain reference to input
        val taskOutput = result.taskOutputs["t0"]!!
        assertTrue(taskOutput.containsKey("input_key1") || taskOutput.containsKey("taskName"))
    }
}
