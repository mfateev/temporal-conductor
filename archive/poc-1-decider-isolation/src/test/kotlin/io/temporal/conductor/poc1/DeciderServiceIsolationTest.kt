package io.temporal.conductor.poc1

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import com.netflix.conductor.core.execution.DeciderOutcomeAccessor
import com.netflix.conductor.core.execution.DeciderService
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * POC 1: DeciderService Isolation Test
 *
 * Success Criteria:
 * 1. DeciderService instantiates without Spring context
 * 2. decide() returns DeciderOutcome with t1 as first task to schedule
 * 3. After marking t1 complete, decide() returns t2
 * 4. After marking t2 complete, workflow reaches terminal state
 */
class DeciderServiceIsolationTest {

    private lateinit var metadataDAO: InMemoryMetadataDAO
    private lateinit var deciderService: DeciderService

    @BeforeEach
    fun setup() {
        metadataDAO = InMemoryMetadataDAO()
        val factory = DeciderServiceFactory(metadataDAO)
        deciderService = factory.create()
    }

    @Test
    fun `POC 1 - DeciderService instantiates without Spring context`() {
        // Success criteria 1: DeciderService instantiates
        assertNotNull(deciderService)
        println("✅ SUCCESS: DeciderService instantiated without Spring context")
    }

    @Test
    fun `POC 1 - Simple 2-task workflow schedules correctly`() {
        // Create task definitions
        val task1Def = TaskDef().apply {
            name = "task1"
            retryCount = 0
            timeoutSeconds = 60
            responseTimeoutSeconds = 60
        }
        val task2Def = TaskDef().apply {
            name = "task2"
            retryCount = 0
            timeoutSeconds = 60
            responseTimeoutSeconds = 60
        }
        metadataDAO.createTaskDef(task1Def)
        metadataDAO.createTaskDef(task2Def)

        // Create workflow definition with 2 sequential tasks
        val workflowDef = WorkflowDef().apply {
            name = "simple-test"
            version = 1
            schemaVersion = 2
            tasks = listOf(
                WorkflowTask().apply {
                    name = "task1"
                    taskReferenceName = "t1"
                    type = TaskType.SIMPLE.name
                    taskDefinition = task1Def
                },
                WorkflowTask().apply {
                    name = "task2"
                    taskReferenceName = "t2"
                    type = TaskType.SIMPLE.name
                    taskDefinition = task2Def
                }
            )
        }

        // Create workflow model (runtime instance)
        val workflow = WorkflowModel().apply {
            workflowId = UUID.randomUUID().toString()
            workflowDefinition = workflowDef
            status = WorkflowModel.Status.RUNNING
            input = mapOf("input1" to "value1")
        }

        // First decision - should schedule t1
        println("\n--- First Decision (new workflow) ---")
        var outcome = deciderService.decide(workflow)
        var tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
        var isComplete = DeciderOutcomeAccessor.isComplete(outcome)

        // Success criteria 2: decide() returns t1 as first task
        assertEquals(1, tasksToSchedule.size, "Should schedule exactly 1 task")
        val task1 = tasksToSchedule[0]
        assertEquals("t1", task1.referenceTaskName, "First task should be t1")
        assertEquals(TaskModel.Status.SCHEDULED, task1.status)
        assertFalse(isComplete, "Workflow should not be complete")
        println("✅ SUCCESS: decide() returned t1 as first task to schedule")

        // Add task1 to workflow and mark it complete
        workflow.tasks.add(task1)
        task1.status = TaskModel.Status.COMPLETED
        task1.outputData = mapOf("result" to "task1-done")

        // Second decision - should schedule t2
        println("\n--- Second Decision (after t1 complete) ---")
        outcome = deciderService.decide(workflow)
        tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
        isComplete = DeciderOutcomeAccessor.isComplete(outcome)

        // Success criteria 3: After t1 complete, decide() returns t2
        assertEquals(1, tasksToSchedule.size, "Should schedule exactly 1 task")
        val task2 = tasksToSchedule[0]
        assertEquals("t2", task2.referenceTaskName, "Second task should be t2")
        assertEquals(TaskModel.Status.SCHEDULED, task2.status)
        assertFalse(isComplete, "Workflow should not be complete yet")
        println("✅ SUCCESS: After t1 complete, decide() returned t2")

        // Add task2 to workflow and mark it complete
        workflow.tasks.add(task2)
        task2.status = TaskModel.Status.COMPLETED
        task2.outputData = mapOf("result" to "task2-done")

        // Third decision - workflow should complete
        println("\n--- Third Decision (after t2 complete) ---")
        outcome = deciderService.decide(workflow)
        tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
        isComplete = DeciderOutcomeAccessor.isComplete(outcome)

        // Success criteria 4: Workflow reaches terminal state
        assertTrue(tasksToSchedule.isEmpty(), "No more tasks to schedule")
        assertTrue(isComplete, "Workflow should be complete")
        println("✅ SUCCESS: After t2 complete, workflow reached terminal state")

        println("\n" + "=".repeat(50))
        println("POC 1 VALIDATION COMPLETE - ALL CRITERIA PASSED")
        println("=".repeat(50))
    }

    @Test
    fun `POC 1 - Workflow with expressions evaluates correctly`() {
        // Create task definition
        val taskDef = TaskDef().apply {
            name = "echo-task"
            retryCount = 0
        }
        metadataDAO.createTaskDef(taskDef)

        // Create workflow with expression in task input
        val workflowDef = WorkflowDef().apply {
            name = "expression-test"
            version = 1
            schemaVersion = 2
            tasks = listOf(
                WorkflowTask().apply {
                    name = "echo-task"
                    taskReferenceName = "echo"
                    type = TaskType.SIMPLE.name
                    taskDefinition = taskDef
                    inputParameters = mapOf(
                        "message" to "\${workflow.input.greeting}",
                        "static" to "hello"
                    )
                }
            )
        }

        val workflow = WorkflowModel().apply {
            workflowId = UUID.randomUUID().toString()
            workflowDefinition = workflowDef
            status = WorkflowModel.Status.RUNNING
            input = mapOf("greeting" to "Hello from workflow!")
        }

        val outcome = deciderService.decide(workflow)
        val tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)

        assertEquals(1, tasksToSchedule.size)
        val task = tasksToSchedule[0]

        // Verify expression was evaluated
        assertEquals("Hello from workflow!", task.inputData["message"])
        assertEquals("hello", task.inputData["static"])
        println("✅ SUCCESS: Expression evaluation works correctly")
    }
}
