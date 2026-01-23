package io.temporal.conductor.poc2

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import com.netflix.conductor.core.execution.DeciderOutcomeAccessor
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import java.util.UUID

/**
 * POC 2: Determinism Audit
 *
 * This runs various workflow scenarios through DeciderService with
 * ByteBuddy instrumentation to detect non-deterministic calls.
 */
fun main() {
    println("=".repeat(70))
    println("POC 2: Determinism Audit")
    println("=".repeat(70))

    // Install instrumentation BEFORE creating any Conductor objects
    NonDeterminismDetector.install()

    // Clear any previous recordings
    NonDeterminismCollector.clear()

    // Enable collection
    NonDeterminismCollector.enabled = true

    println("\n[1] Creating DeciderService and running test scenarios...")

    val metadataDAO = InMemoryMetadataDAO()
    val factory = DeciderServiceFactory(metadataDAO)
    val deciderService = factory.create()

    // Scenario 1: Simple 2-task workflow
    println("\n--- Scenario 1: Simple 2-task sequential workflow ---")
    runSimpleWorkflow(deciderService, metadataDAO)

    // Scenario 2: Workflow with expressions
    println("\n--- Scenario 2: Workflow with input expressions ---")
    runExpressionWorkflow(deciderService, metadataDAO)

    // Scenario 3: Multiple decisions (simulating workflow progress)
    println("\n--- Scenario 3: Complete workflow lifecycle ---")
    runCompleteWorkflowLifecycle(deciderService, metadataDAO)

    // Disable collection
    NonDeterminismCollector.enabled = false

    // Print the audit report
    NonDeterminismCollector.printReport()

    // Provide recommendations
    printRecommendations()
}

private fun runSimpleWorkflow(
    deciderService: com.netflix.conductor.core.execution.DeciderService,
    metadataDAO: InMemoryMetadataDAO
) {
    val taskDef = TaskDef().apply {
        name = "simple-task"
        retryCount = 0
        timeoutSeconds = 60
        responseTimeoutSeconds = 60
    }
    metadataDAO.createTaskDef(taskDef)

    val workflowDef = WorkflowDef().apply {
        name = "simple-workflow"
        version = 1
        schemaVersion = 2
        tasks = listOf(
            WorkflowTask().apply {
                name = "simple-task"
                taskReferenceName = "t1"
                type = TaskType.SIMPLE.name
                taskDefinition = taskDef
            }
        )
    }

    val workflow = WorkflowModel().apply {
        workflowId = UUID.randomUUID().toString()
        workflowDefinition = workflowDef
        status = WorkflowModel.Status.RUNNING
        input = mapOf("key" to "value")
    }

    val outcome = deciderService.decide(workflow)
    val tasks = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    println("   Scheduled tasks: ${tasks.map { it.referenceTaskName }}")
}

private fun runExpressionWorkflow(
    deciderService: com.netflix.conductor.core.execution.DeciderService,
    metadataDAO: InMemoryMetadataDAO
) {
    val taskDef = TaskDef().apply {
        name = "expression-task"
        retryCount = 0
    }
    metadataDAO.createTaskDef(taskDef)

    val workflowDef = WorkflowDef().apply {
        name = "expression-workflow"
        version = 1
        schemaVersion = 2
        tasks = listOf(
            WorkflowTask().apply {
                name = "expression-task"
                taskReferenceName = "expr"
                type = TaskType.SIMPLE.name
                taskDefinition = taskDef
                inputParameters = mapOf(
                    "message" to "\${workflow.input.greeting}",
                    "timestamp" to "\${workflow.input.ts}",
                    "computed" to "\${workflow.input.a + workflow.input.b}"
                )
            }
        )
    }

    val workflow = WorkflowModel().apply {
        workflowId = UUID.randomUUID().toString()
        workflowDefinition = workflowDef
        status = WorkflowModel.Status.RUNNING
        input = mapOf(
            "greeting" to "Hello",
            "ts" to System.currentTimeMillis(),
            "a" to 10,
            "b" to 20
        )
    }

    val outcome = deciderService.decide(workflow)
    val tasks = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    println("   Scheduled tasks: ${tasks.map { it.referenceTaskName }}")
    println("   Task input: ${tasks.firstOrNull()?.inputData}")
}

private fun runCompleteWorkflowLifecycle(
    deciderService: com.netflix.conductor.core.execution.DeciderService,
    metadataDAO: InMemoryMetadataDAO
) {
    val task1Def = TaskDef().apply {
        name = "lifecycle-task1"
        retryCount = 0
        timeoutSeconds = 60
        responseTimeoutSeconds = 60
    }
    val task2Def = TaskDef().apply {
        name = "lifecycle-task2"
        retryCount = 0
        timeoutSeconds = 60
        responseTimeoutSeconds = 60
    }
    metadataDAO.createTaskDef(task1Def)
    metadataDAO.createTaskDef(task2Def)

    val workflowDef = WorkflowDef().apply {
        name = "lifecycle-workflow"
        version = 1
        schemaVersion = 2
        tasks = listOf(
            WorkflowTask().apply {
                name = "lifecycle-task1"
                taskReferenceName = "lc1"
                type = TaskType.SIMPLE.name
                taskDefinition = task1Def
            },
            WorkflowTask().apply {
                name = "lifecycle-task2"
                taskReferenceName = "lc2"
                type = TaskType.SIMPLE.name
                taskDefinition = task2Def
            }
        )
    }

    val workflow = WorkflowModel().apply {
        workflowId = UUID.randomUUID().toString()
        workflowDefinition = workflowDef
        status = WorkflowModel.Status.RUNNING
        input = mapOf("key" to "value")
    }

    // Decision 1: Schedule first task
    var outcome = deciderService.decide(workflow)
    var tasks = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    println("   Decision 1 - Scheduled: ${tasks.map { it.referenceTaskName }}")

    // Complete first task
    val task1 = tasks[0]
    workflow.tasks.add(task1)
    task1.status = TaskModel.Status.COMPLETED
    task1.outputData = mapOf("result" to "done")

    // Decision 2: Schedule second task
    outcome = deciderService.decide(workflow)
    tasks = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)
    println("   Decision 2 - Scheduled: ${tasks.map { it.referenceTaskName }}")

    // Complete second task
    val task2 = tasks[0]
    workflow.tasks.add(task2)
    task2.status = TaskModel.Status.COMPLETED
    task2.outputData = mapOf("result" to "done")

    // Decision 3: Workflow complete
    outcome = deciderService.decide(workflow)
    val isComplete = DeciderOutcomeAccessor.isComplete(outcome)
    println("   Decision 3 - Workflow complete: $isComplete")
}

private fun printRecommendations() {
    val blocking = NonDeterminismCollector.getUniqueCalls()
        .filter { it.category == NonDeterministicCall.Category.BLOCKING }

    println("\n" + "=".repeat(70))
    println("RECOMMENDATIONS")
    println("=".repeat(70))

    if (blocking.isEmpty()) {
        println("""
            |
            |✅ No blocking non-deterministic calls detected in core scheduling logic.
            |
            |This means DeciderService.decide() can run inside a Temporal workflow
            |without modifications for basic SIMPLE task workflows.
            |
            |HOWEVER, you should still:
            |1. Test with more complex task types (SWITCH, FORK_JOIN, DO_WHILE)
            |2. Test with JavaScript expressions (potential GraalJS non-determinism)
            |3. Monitor for non-determinism in production with Temporal's workflow replay testing
            |
        """.trimMargin())
    } else {
        println("""
            |
            |⚠️  Found ${blocking.size} blocking non-deterministic call(s) that need attention:
            |
        """.trimMargin())

        blocking.forEach { call ->
            println("   • ${call.methodName}")
            println("     Location: ${call.conductorFrames.firstOrNull() ?: "unknown"}")
            println("     Recommendation: ${getRecommendation(call)}")
            println()
        }

        println("""
            |
            |MITIGATION STRATEGIES:
            |
            |1. IDGenerator (UUID.randomUUID):
            |   - Create extension point in Conductor fork
            |   - Allow custom ID generation via interface
            |   - In Temporal workflow, use Workflow.sideEffect() or deterministic IDs
            |
            |2. Time-based checks (currentTimeMillis):
            |   - Fork and add WorkflowClock interface
            |   - Pass workflow time from Temporal's Workflow.currentTimeMillis()
            |
            |3. Random values:
            |   - Use Workflow.sideEffect() for truly random values
            |   - Or use deterministic seed based on workflow ID
            |
        """.trimMargin())
    }
}

private fun getRecommendation(call: NonDeterministicCall): String {
    return when {
        call.methodName.contains("UUID") -> "Add IDGenerator interface extension point"
        call.methodName.contains("currentTimeMillis") -> "Add WorkflowClock interface extension point"
        call.methodName.contains("Random") || call.methodName.contains("random") ->
            "Use Workflow.sideEffect() or deterministic seed"
        else -> "Evaluate impact and consider wrapping in sideEffect()"
    }
}
