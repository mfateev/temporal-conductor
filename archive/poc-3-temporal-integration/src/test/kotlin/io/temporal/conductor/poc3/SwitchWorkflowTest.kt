package io.temporal.conductor.poc3

import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Test SWITCH task type for conditional branching.
 */
class SwitchWorkflowTest : BaseWorkflowTest() {

    @Test
    fun `switch branches to case A when input is A`() {
        val workflowDef = createSwitchWorkflowDef()
        val taskDefs = createTaskDefs("task_a", "task_b", "task_default")
        val input = createInput(workflowDef, mapOf("switchValue" to "A"), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("t_a"), "Task A should have executed")
        assertFalse(result.taskOutputs.containsKey("t_b"), "Task B should not have executed")
        assertFalse(result.taskOutputs.containsKey("t_default"), "Default task should not have executed")
    }

    @Test
    fun `switch branches to case B when input is B`() {
        val workflowDef = createSwitchWorkflowDef()
        val taskDefs = createTaskDefs("task_a", "task_b", "task_default")
        val input = createInput(workflowDef, mapOf("switchValue" to "B"), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertFalse(result.taskOutputs.containsKey("t_a"), "Task A should not have executed")
        assertTrue(result.taskOutputs.containsKey("t_b"), "Task B should have executed")
        assertFalse(result.taskOutputs.containsKey("t_default"), "Default task should not have executed")
    }

    @Test
    fun `switch branches to default when input matches no case`() {
        val workflowDef = createSwitchWorkflowDef()
        val taskDefs = createTaskDefs("task_a", "task_b", "task_default")
        val input = createInput(workflowDef, mapOf("switchValue" to "C"), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertFalse(result.taskOutputs.containsKey("t_a"), "Task A should not have executed")
        assertFalse(result.taskOutputs.containsKey("t_b"), "Task B should not have executed")
        assertTrue(result.taskOutputs.containsKey("t_default"), "Default task should have executed")
    }

    @Test
    fun `switch with task before and after`() {
        val workflowDef = createSwitchWithSurroundingTasks()
        val taskDefs = createTaskDefs("task_before", "task_a", "task_b", "task_after")
        val input = createInput(workflowDef, mapOf("switchValue" to "A"), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("t_before"), "Before task should have executed")
        assertTrue(result.taskOutputs.containsKey("t_a"), "Task A should have executed")
        assertTrue(result.taskOutputs.containsKey("t_after"), "After task should have executed")
    }

    private fun createSwitchWorkflowDef(): WorkflowDef {
        return WorkflowDef().apply {
            name = "switch-test"
            version = 1

            val switchTask = WorkflowTask().apply {
                name = "switch_task"
                taskReferenceName = "switch1"
                type = "SWITCH"
                evaluatorType = "value-param"
                expression = "switchValue"
                inputParameters = mapOf("switchValue" to "\${workflow.input.switchValue}")
                decisionCases = mapOf(
                    "A" to listOf(
                        WorkflowTask().apply {
                            name = "task_a"
                            taskReferenceName = "t_a"
                            type = "SIMPLE"
                        }
                    ),
                    "B" to listOf(
                        WorkflowTask().apply {
                            name = "task_b"
                            taskReferenceName = "t_b"
                            type = "SIMPLE"
                        }
                    )
                )
                defaultCase = listOf(
                    WorkflowTask().apply {
                        name = "task_default"
                        taskReferenceName = "t_default"
                        type = "SIMPLE"
                    }
                )
            }

            tasks = listOf(switchTask)
        }
    }

    private fun createSwitchWithSurroundingTasks(): WorkflowDef {
        return WorkflowDef().apply {
            name = "switch-surrounded-test"
            version = 1

            val beforeTask = WorkflowTask().apply {
                name = "task_before"
                taskReferenceName = "t_before"
                type = "SIMPLE"
            }

            val switchTask = WorkflowTask().apply {
                name = "switch_task"
                taskReferenceName = "switch1"
                type = "SWITCH"
                evaluatorType = "value-param"
                expression = "switchValue"
                inputParameters = mapOf("switchValue" to "\${workflow.input.switchValue}")
                decisionCases = mapOf(
                    "A" to listOf(
                        WorkflowTask().apply {
                            name = "task_a"
                            taskReferenceName = "t_a"
                            type = "SIMPLE"
                        }
                    ),
                    "B" to listOf(
                        WorkflowTask().apply {
                            name = "task_b"
                            taskReferenceName = "t_b"
                            type = "SIMPLE"
                        }
                    )
                )
            }

            val afterTask = WorkflowTask().apply {
                name = "task_after"
                taskReferenceName = "t_after"
                type = "SIMPLE"
            }

            tasks = listOf(beforeTask, switchTask, afterTask)
        }
    }
}
