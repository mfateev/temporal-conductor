package io.temporal.conductor.poc3.worker

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.conductor.poc3.activities.TaskExecutionActivitiesImpl
import io.temporal.conductor.poc3.workflow.ConductorWorkflowImpl
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import org.slf4j.LoggerFactory

/**
 * Temporal worker for executing Conductor workflows.
 *
 * This worker registers:
 * - ConductorWorkflowImpl for workflow execution
 * - TaskExecutionActivitiesImpl for task execution
 */
class ConductorWorker(
    private val targetHost: String = "localhost:7233",
    private val namespace: String = "default",
    private val taskQueue: String = TASK_QUEUE
) {
    private val logger = LoggerFactory.getLogger(ConductorWorker::class.java)

    private lateinit var factory: WorkerFactory
    private lateinit var client: WorkflowClient

    fun start() {
        logger.info("Starting Conductor worker...")
        logger.info("Target: {}, Namespace: {}, TaskQueue: {}", targetHost, namespace, taskQueue)

        // Create service stubs
        val serviceOptions = WorkflowServiceStubsOptions.newBuilder()
            .setTarget(targetHost)
            .build()
        val service = WorkflowServiceStubs.newServiceStubs(serviceOptions)

        // Create workflow client
        val clientOptions = WorkflowClientOptions.newBuilder()
            .setNamespace(namespace)
            .build()
        client = WorkflowClient.newInstance(service, clientOptions)

        // Create worker factory and worker
        factory = WorkerFactory.newInstance(client)
        val worker = factory.newWorker(taskQueue)

        // Register workflow implementation
        worker.registerWorkflowImplementationTypes(ConductorWorkflowImpl::class.java)

        // Register activity implementations
        worker.registerActivitiesImplementations(TaskExecutionActivitiesImpl())

        // Start the worker
        factory.start()
        logger.info("Conductor worker started successfully")
    }

    fun stop() {
        logger.info("Stopping Conductor worker...")
        factory.shutdown()
        logger.info("Conductor worker stopped")
    }

    fun getClient(): WorkflowClient = client

    companion object {
        const val TASK_QUEUE = "conductor-workflow-task-queue"
    }
}

/**
 * Main entry point for running the worker standalone.
 */
fun main(args: Array<String>) {
    val host = args.getOrNull(0) ?: "localhost:7233"
    val namespace = args.getOrNull(1) ?: "default"

    val worker = ConductorWorker(targetHost = host, namespace = namespace)

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        worker.stop()
    })

    worker.start()

    // Keep running
    println("Worker started. Press Ctrl+C to stop.")
    Thread.currentThread().join()
}
