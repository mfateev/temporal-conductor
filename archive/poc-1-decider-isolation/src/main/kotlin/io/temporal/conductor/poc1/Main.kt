package io.temporal.conductor.poc1

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import com.netflix.conductor.core.execution.DeciderOutcomeAccessor
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import java.util.UUID

/**
 * POC 1: DeciderService Isolation
 *
 * This demonstrates that DeciderService can be instantiated and used
 * without a Spring context, validating the core assumption for the
 * Temporal-Conductor integration.
 */
fun main() {
    println("=".repeat(60))
    println("POC 1: DeciderService Isolation Test")
    println("=".repeat(60))

    // Step 1: Create MetadataDAO and DeciderService without Spring
    println("\n[1] Creating DeciderService without Spring context...")
    val metadataDAO = InMemoryMetadataDAO()
    val factory = DeciderServiceFactory(metadataDAO)
    val deciderService = factory.create()
    println("    ✅ DeciderService created successfully!")

    // Step 2: Create task definitions
    println("\n[2] Creating task definitions...")
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
    println("    ✅ Task definitions created: task1, task2")

    // Step 3: Create workflow definition
    println("\n[3] Creating workflow definition...")
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
    println("    ✅ Workflow definition created: simple-test (2 tasks)")

    // Step 4: Create workflow instance
    println("\n[4] Creating workflow instance...")
    val workflow = WorkflowModel().apply {
        workflowId = UUID.randomUUID().toString()
        workflowDefinition = workflowDef
        status = WorkflowModel.Status.RUNNING
        input = mapOf("input1" to "value1")
    }
    println("    ✅ Workflow instance created: ${workflow.workflowId}")

    // Step 5: First decision
    println("\n[5] First decision (new workflow)...")
    var outcome = deciderService.decide(workflow)
    val tasks1 = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    val complete1 = DeciderOutcomeAccessor.isComplete(outcome)
    println("    Tasks to schedule: ${tasks1.map { it.referenceTaskName }}")
    println("    Is complete: $complete1")

    if (tasks1.size == 1 && tasks1[0].referenceTaskName == "t1") {
        println("    ✅ Correct: First task t1 scheduled")
    } else {
        println("    ❌ ERROR: Expected t1 to be scheduled")
        return
    }

    // Step 6: Complete t1 and decide again
    println("\n[6] Completing t1 and deciding again...")
    val task1 = tasks1[0]
    workflow.tasks.add(task1)
    task1.status = TaskModel.Status.COMPLETED
    task1.outputData = mapOf("result" to "task1-done")

    outcome = deciderService.decide(workflow)
    val tasks2 = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    val complete2 = DeciderOutcomeAccessor.isComplete(outcome)
    println("    Tasks to schedule: ${tasks2.map { it.referenceTaskName }}")
    println("    Is complete: $complete2")

    if (tasks2.size == 1 && tasks2[0].referenceTaskName == "t2") {
        println("    ✅ Correct: Second task t2 scheduled")
    } else {
        println("    ❌ ERROR: Expected t2 to be scheduled")
        return
    }

    // Step 7: Complete t2 and decide again
    println("\n[7] Completing t2 and deciding again...")
    val task2 = tasks2[0]
    workflow.tasks.add(task2)
    task2.status = TaskModel.Status.COMPLETED
    task2.outputData = mapOf("result" to "task2-done")

    outcome = deciderService.decide(workflow)
    val tasks3 = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    val complete3 = DeciderOutcomeAccessor.isComplete(outcome)
    println("    Tasks to schedule: ${tasks3.map { it.referenceTaskName }}")
    println("    Is complete: $complete3")

    if (tasks3.isEmpty() && complete3) {
        println("    ✅ Correct: Workflow completed")
    } else {
        println("    ❌ ERROR: Workflow should be complete")
        return
    }

    // Summary
    println("\n" + "=".repeat(60))
    println("POC 1 RESULT: SUCCESS")
    println("=".repeat(60))
    println("""
    |
    | DeciderService successfully:
    | 1. ✅ Instantiated without Spring context
    | 2. ✅ Scheduled t1 as first task
    | 3. ✅ Scheduled t2 after t1 completed
    | 4. ✅ Marked workflow complete after t2
    |
    | This validates the core assumption that Conductor's DeciderService
    | can be used as a library without requiring the full Spring context.
    |
    """.trimMargin())
}
