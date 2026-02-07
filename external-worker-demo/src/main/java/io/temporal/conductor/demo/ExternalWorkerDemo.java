/*
 * Copyright 2024 Temporal Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.temporal.conductor.demo;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demo application that starts a Temporal worker listening on a custom task queue.
 *
 * <p>This worker executes tasks that are routed to it using the {@code taskName@taskQueue}
 * naming convention in Conductor workflow definitions.
 *
 * <p>Usage:
 * <pre>
 * # Start with default task queue "external-workers"
 * ./gradlew :external-worker-demo:run
 *
 * # Start with custom task queue
 * ./gradlew :external-worker-demo:run --args="my-custom-queue"
 * </pre>
 *
 * <p>Environment variables:
 * <ul>
 *   <li>{@code TEMPORAL_ADDRESS} - Temporal server address (default: localhost:7233)</li>
 *   <li>{@code TEMPORAL_NAMESPACE} - Temporal namespace (default: conductor)</li>
 * </ul>
 */
public class ExternalWorkerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ExternalWorkerDemo.class);

    private static final String DEFAULT_TASK_QUEUE = "external-workers";
    private static final String DEFAULT_TEMPORAL_ADDRESS = "localhost:7233";
    private static final String DEFAULT_NAMESPACE = "conductor";

    public static void main(String[] args) {
        String taskQueue = args.length > 0 ? args[0] : DEFAULT_TASK_QUEUE;
        String temporalAddress = System.getenv().getOrDefault("TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS);
        String namespace = System.getenv().getOrDefault("TEMPORAL_NAMESPACE", DEFAULT_NAMESPACE);

        logger.info("Starting external worker demo");
        logger.info("  Temporal address: {}", temporalAddress);
        logger.info("  Namespace: {}", namespace);
        logger.info("  Task queue: {}", taskQueue);

        // Create connection to Temporal
        WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder()
                        .setTarget(temporalAddress)
                        .build());

        WorkflowClient client = WorkflowClient.newInstance(service,
                io.temporal.client.WorkflowClientOptions.newBuilder()
                        .setNamespace(namespace)
                        .build());

        // Create worker factory
        WorkerFactory factory = WorkerFactory.newInstance(client);

        // Create worker for the specified task queue
        Worker worker = factory.newWorker(taskQueue);

        // Register the dynamic activity implementation
        worker.registerActivitiesImplementations(new ExternalTaskActivities());

        logger.info("External worker registered on task queue: {}", taskQueue);
        logger.info("Ready to process tasks routed with @{} suffix", taskQueue);

        // Start the worker
        factory.start();

        logger.info("Worker started. Press Ctrl+C to stop.");

        // Keep the worker running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down worker...");
            factory.shutdown();
        }));
    }
}
