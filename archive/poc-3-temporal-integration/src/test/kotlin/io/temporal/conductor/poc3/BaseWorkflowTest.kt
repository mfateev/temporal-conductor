package io.temporal.conductor.poc3

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.conductor.poc3.activities.TaskExecutionActivitiesImpl
import io.temporal.conductor.poc3.model.ConductorWorkflowInput
import io.temporal.conductor.poc3.model.ConductorWorkflowOutput
import io.temporal.conductor.poc3.workflow.ConductorWorkflow
import io.temporal.conductor.poc3.workflow.ConductorWorkflowImpl
import io.temporal.conductor.poc3.worker.ConductorWorker
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.time.Duration

/**
 * Base class for Conductor workflow tests using Temporal's test environment.
 */
abstract class BaseWorkflowTest {

    protected lateinit var testEnv: TestWorkflowEnvironment
    protected lateinit var worker: Worker
    protected lateinit var client: WorkflowClient

    protected val objectMapper = jacksonObjectMapper()

    @BeforeEach
    fun setUp() {
        testEnv = TestWorkflowEnvironment.newInstance()
        worker = testEnv.newWorker(ConductorWorker.TASK_QUEUE)

        // Register workflow and activities
        worker.registerWorkflowImplementationTypes(ConductorWorkflowImpl::class.java)
        worker.registerActivitiesImplementations(TaskExecutionActivitiesImpl())

        client = testEnv.workflowClient
        testEnv.start()
    }

    @AfterEach
    fun tearDown() {
        testEnv.close()
    }

    /**
     * Execute a workflow and return the result.
     */
    protected fun executeWorkflow(input: ConductorWorkflowInput): ConductorWorkflowOutput {
        val options = WorkflowOptions.newBuilder()
            .setTaskQueue(ConductorWorker.TASK_QUEUE)
            .setWorkflowExecutionTimeout(Duration.ofMinutes(5))
            .build()

        val workflow = client.newWorkflowStub(ConductorWorkflow::class.java, options)
        return workflow.execute(input)
    }

    /**
     * Create a simple WorkflowDef with sequential tasks.
     */
    protected fun createSimpleWorkflowDef(
        name: String,
        vararg taskNames: String
    ): WorkflowDef {
        val workflowDef = WorkflowDef()
        workflowDef.name = name
        workflowDef.version = 1
        workflowDef.tasks = taskNames.mapIndexed { index, taskName ->
            WorkflowTask().apply {
                this.name = taskName
                this.taskReferenceName = "t$index"
                this.type = "SIMPLE"
            }
        }
        return workflowDef
    }

    /**
     * Create TaskDef map for simple tasks.
     */
    protected fun createTaskDefs(vararg taskNames: String): Map<String, String> {
        return taskNames.associate { name ->
            val taskDef = TaskDef()
            taskDef.name = name
            taskDef.timeoutSeconds = 0  // Disable Conductor timeouts
            taskDef.responseTimeoutSeconds = 0
            name to objectMapper.writeValueAsString(taskDef)
        }
    }

    /**
     * Create workflow input from a WorkflowDef.
     */
    protected fun createInput(
        workflowDef: WorkflowDef,
        workflowInput: Map<String, Any> = emptyMap(),
        taskDefs: Map<String, String> = emptyMap()
    ): ConductorWorkflowInput {
        return ConductorWorkflowInput(
            workflowDefJson = objectMapper.writeValueAsString(workflowDef),
            workflowInput = workflowInput,
            taskDefsJson = taskDefs
        )
    }
}
