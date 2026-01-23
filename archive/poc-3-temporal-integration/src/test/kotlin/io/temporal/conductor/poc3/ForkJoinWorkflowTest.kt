package io.temporal.conductor.poc3

import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Test FORK_JOIN task type for parallel execution.
 */
class ForkJoinWorkflowTest : BaseWorkflowTest() {

    @Test
    fun `fork join executes two branches in parallel`() {
        val workflowDef = createForkJoinWorkflowDef(2)
        val taskDefs = createTaskDefs("branch1_task", "branch2_task")
        val input = createInput(workflowDef, emptyMap(), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("b1"), "Branch 1 should have executed")
        assertTrue(result.taskOutputs.containsKey("b2"), "Branch 2 should have executed")
    }

    @Test
    fun `fork join executes three branches in parallel`() {
        val workflowDef = createForkJoinWorkflowDef(3)
        val taskDefs = createTaskDefs("branch1_task", "branch2_task", "branch3_task")
        val input = createInput(workflowDef, emptyMap(), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("b1"), "Branch 1 should have executed")
        assertTrue(result.taskOutputs.containsKey("b2"), "Branch 2 should have executed")
        assertTrue(result.taskOutputs.containsKey("b3"), "Branch 3 should have executed")
    }

    @Test
    fun `fork join with task before and after`() {
        val workflowDef = createForkJoinWithSurroundingTasks()
        val taskDefs = createTaskDefs("task_before", "branch1_task", "branch2_task", "task_after")
        val input = createInput(workflowDef, emptyMap(), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("t_before"), "Before task should have executed")
        assertTrue(result.taskOutputs.containsKey("b1"), "Branch 1 should have executed")
        assertTrue(result.taskOutputs.containsKey("b2"), "Branch 2 should have executed")
        assertTrue(result.taskOutputs.containsKey("t_after"), "After task should have executed")
    }

    @Test
    fun `fork join with multiple tasks per branch`() {
        val workflowDef = createForkJoinWithMultipleTasksPerBranch()
        val taskDefs = createTaskDefs("branch1_task1", "branch1_task2", "branch2_task1", "branch2_task2")
        val input = createInput(workflowDef, emptyMap(), taskDefs)

        val result = executeWorkflow(input)

        assertEquals("COMPLETED", result.status)
        assertTrue(result.taskOutputs.containsKey("b1_t1"), "Branch 1 task 1 should have executed")
        assertTrue(result.taskOutputs.containsKey("b1_t2"), "Branch 1 task 2 should have executed")
        assertTrue(result.taskOutputs.containsKey("b2_t1"), "Branch 2 task 1 should have executed")
        assertTrue(result.taskOutputs.containsKey("b2_t2"), "Branch 2 task 2 should have executed")
    }

    private fun createForkJoinWorkflowDef(branchCount: Int): WorkflowDef {
        return WorkflowDef().apply {
            name = "fork-join-test"
            version = 1

            val forkTasks = (1..branchCount).map { i ->
                listOf(
                    WorkflowTask().apply {
                        name = "branch${i}_task"
                        taskReferenceName = "b$i"
                        type = "SIMPLE"
                    }
                )
            }

            val forkTask = WorkflowTask().apply {
                name = "fork_task"
                taskReferenceName = "fork1"
                type = "FORK_JOIN"
                this.forkTasks = forkTasks
            }

            val joinTask = WorkflowTask().apply {
                name = "join_task"
                taskReferenceName = "join1"
                type = "JOIN"
                joinOn = (1..branchCount).map { "b$it" }
            }

            tasks = listOf(forkTask, joinTask)
        }
    }

    private fun createForkJoinWithSurroundingTasks(): WorkflowDef {
        return WorkflowDef().apply {
            name = "fork-join-surrounded-test"
            version = 1

            val beforeTask = WorkflowTask().apply {
                name = "task_before"
                taskReferenceName = "t_before"
                type = "SIMPLE"
            }

            val forkTask = WorkflowTask().apply {
                name = "fork_task"
                taskReferenceName = "fork1"
                type = "FORK_JOIN"
                forkTasks = listOf(
                    listOf(
                        WorkflowTask().apply {
                            name = "branch1_task"
                            taskReferenceName = "b1"
                            type = "SIMPLE"
                        }
                    ),
                    listOf(
                        WorkflowTask().apply {
                            name = "branch2_task"
                            taskReferenceName = "b2"
                            type = "SIMPLE"
                        }
                    )
                )
            }

            val joinTask = WorkflowTask().apply {
                name = "join_task"
                taskReferenceName = "join1"
                type = "JOIN"
                joinOn = listOf("b1", "b2")
            }

            val afterTask = WorkflowTask().apply {
                name = "task_after"
                taskReferenceName = "t_after"
                type = "SIMPLE"
            }

            tasks = listOf(beforeTask, forkTask, joinTask, afterTask)
        }
    }

    private fun createForkJoinWithMultipleTasksPerBranch(): WorkflowDef {
        return WorkflowDef().apply {
            name = "fork-join-multi-task-test"
            version = 1

            val forkTask = WorkflowTask().apply {
                name = "fork_task"
                taskReferenceName = "fork1"
                type = "FORK_JOIN"
                forkTasks = listOf(
                    listOf(
                        WorkflowTask().apply {
                            name = "branch1_task1"
                            taskReferenceName = "b1_t1"
                            type = "SIMPLE"
                        },
                        WorkflowTask().apply {
                            name = "branch1_task2"
                            taskReferenceName = "b1_t2"
                            type = "SIMPLE"
                        }
                    ),
                    listOf(
                        WorkflowTask().apply {
                            name = "branch2_task1"
                            taskReferenceName = "b2_t1"
                            type = "SIMPLE"
                        },
                        WorkflowTask().apply {
                            name = "branch2_task2"
                            taskReferenceName = "b2_t2"
                            type = "SIMPLE"
                        }
                    )
                )
            }

            val joinTask = WorkflowTask().apply {
                name = "join_task"
                taskReferenceName = "join1"
                type = "JOIN"
                joinOn = listOf("b1_t2", "b2_t2")
            }

            tasks = listOf(forkTask, joinTask)
        }
    }
}
