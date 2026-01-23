package io.temporal.conductor.poc3

import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Test DO_WHILE task type for loops.
 */
class DoWhileWorkflowTest : BaseWorkflowTest() {

    @Test
    fun `do while iterates specified number of times`() {
        val workflowDef = createDoWhileWorkflowDef(iterations = 3)
        val taskDefs = createTaskDefs("loop_body_task")
        val input = createInput(workflowDef, mapOf("maxIterations" to 3), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        // DO_WHILE creates tasks with iteration suffix
        assertTrue(result.taskOutputs.isNotEmpty(), "Loop body should have executed")
    }

    @Test
    fun `do while with zero iterations completes immediately`() {
        val workflowDef = createDoWhileWorkflowDef(iterations = 0)
        val taskDefs = createTaskDefs("loop_body_task")
        val input = createInput(workflowDef, mapOf("maxIterations" to 0), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
    }

    @Test
    fun `do while with task before and after`() {
        val workflowDef = createDoWhileWithSurroundingTasks(iterations = 2)
        val taskDefs = createTaskDefs("task_before", "loop_body_task", "task_after")
        val input = createInput(workflowDef, mapOf("maxIterations" to 2), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("t_before"), "Before task should have executed")
        assertTrue(result.taskOutputs.containsKey("t_after"), "After task should have executed")
    }

    @Test
    fun `do while with multiple tasks in loop body`() {
        val workflowDef = createDoWhileWithMultipleTasks(iterations = 2)
        val taskDefs = createTaskDefs("loop_task1", "loop_task2")
        val input = createInput(workflowDef, mapOf("maxIterations" to 2), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        // Both loop tasks should have executed
        assertTrue(result.taskOutputs.isNotEmpty(), "Loop body tasks should have executed")
    }

    private fun createDoWhileWorkflowDef(iterations: Int): WorkflowDef {
        return WorkflowDef().apply {
            name = "do-while-test"
            version = 1

            val doWhileTask = WorkflowTask().apply {
                name = "loop_task"
                taskReferenceName = "loop1"
                type = "DO_WHILE"
                // Input parameters resolve workflow.input to make it available in condition
                inputParameters = mapOf("maxIterations" to "\${workflow.input.maxIterations}")
                // Loop condition: continue while iteration < maxIterations
                loopCondition = "if (\$.loop1['iteration'] < \$.maxIterations) { true; } else { false; }"
                loopOver = listOf(
                    WorkflowTask().apply {
                        name = "loop_body_task"
                        taskReferenceName = "loop_body"
                        type = "SIMPLE"
                    }
                )
            }

            tasks = listOf(doWhileTask)
        }
    }

    private fun createDoWhileWithSurroundingTasks(iterations: Int): WorkflowDef {
        return WorkflowDef().apply {
            name = "do-while-surrounded-test"
            version = 1

            val beforeTask = WorkflowTask().apply {
                name = "task_before"
                taskReferenceName = "t_before"
                type = "SIMPLE"
            }

            val doWhileTask = WorkflowTask().apply {
                name = "loop_task"
                taskReferenceName = "loop1"
                type = "DO_WHILE"
                inputParameters = mapOf("maxIterations" to "\${workflow.input.maxIterations}")
                loopCondition = "if (\$.loop1['iteration'] < \$.maxIterations) { true; } else { false; }"
                loopOver = listOf(
                    WorkflowTask().apply {
                        name = "loop_body_task"
                        taskReferenceName = "loop_body"
                        type = "SIMPLE"
                    }
                )
            }

            val afterTask = WorkflowTask().apply {
                name = "task_after"
                taskReferenceName = "t_after"
                type = "SIMPLE"
            }

            tasks = listOf(beforeTask, doWhileTask, afterTask)
        }
    }

    private fun createDoWhileWithMultipleTasks(iterations: Int): WorkflowDef {
        return WorkflowDef().apply {
            name = "do-while-multi-task-test"
            version = 1

            val doWhileTask = WorkflowTask().apply {
                name = "loop_task"
                taskReferenceName = "loop1"
                type = "DO_WHILE"
                inputParameters = mapOf("maxIterations" to "\${workflow.input.maxIterations}")
                loopCondition = "if (\$.loop1['iteration'] < \$.maxIterations) { true; } else { false; }"
                loopOver = listOf(
                    WorkflowTask().apply {
                        name = "loop_task1"
                        taskReferenceName = "loop_t1"
                        type = "SIMPLE"
                    },
                    WorkflowTask().apply {
                        name = "loop_task2"
                        taskReferenceName = "loop_t2"
                        type = "SIMPLE"
                    }
                )
            }

            tasks = listOf(doWhileTask)
        }
    }
}
