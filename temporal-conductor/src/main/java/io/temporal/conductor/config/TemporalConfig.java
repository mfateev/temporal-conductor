/*
 * Copyright Temporal Technologies Inc.
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

package io.temporal.conductor.config;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.conductor.activity.TaskExecutionActivitiesImpl;
import io.temporal.conductor.workflow.ConductorWorkflowImpl;
import io.temporal.conductor.workflow.DefinitionWorkflowImpl;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Temporal configuration for the conductor-server-temporal application.
 *
 * <p>This configuration is only active when the "temporal" profile is enabled.
 * It sets up the Temporal WorkflowClient, WorkerFactory, and registers the
 * ConductorWorkflow implementation.
 */
@Configuration
@Profile("temporal")
public class TemporalConfig {

    private static final Logger logger = LoggerFactory.getLogger(TemporalConfig.class);

    // Support standard Temporal env vars (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE)
    // with fallback to Spring properties for backward compatibility
    @Value("${TEMPORAL_ADDRESS:${temporal.service-address:localhost:7234}}")
    private String serviceAddress;

    @Value("${TEMPORAL_NAMESPACE:${temporal.namespace:conductor}}")
    private String namespace;

    @Value("${TEMPORAL_TASK_QUEUE:${temporal.task-queue:conductor-workflows}}")
    private String taskQueue;

    /**
     * Creates the WorkflowServiceStubs for connecting to the Temporal server.
     *
     * @return configured WorkflowServiceStubs
     */
    @Bean
    public WorkflowServiceStubs workflowServiceStubs() {
        logger.info("Connecting to Temporal server at: {}", serviceAddress);
        WorkflowServiceStubsOptions options = WorkflowServiceStubsOptions.newBuilder()
                .setTarget(serviceAddress)
                .build();
        return WorkflowServiceStubs.newServiceStubs(options);
    }

    /**
     * Creates the WorkflowClient for interacting with Temporal workflows.
     *
     * @param serviceStubs the service stubs for Temporal connection
     * @return configured WorkflowClient
     */
    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs serviceStubs) {
        logger.info("Creating WorkflowClient for namespace: {}", namespace);
        WorkflowClientOptions options = WorkflowClientOptions.newBuilder()
                .setNamespace(namespace)
                .build();
        return WorkflowClient.newInstance(serviceStubs, options);
    }

    /**
     * Creates the WorkerFactory for managing Temporal workers.
     *
     * @param workflowClient the workflow client
     * @return configured WorkerFactory
     */
    @Bean
    public WorkerFactory workerFactory(WorkflowClient workflowClient) {
        return WorkerFactory.newInstance(workflowClient);
    }

    /**
     * Creates and starts a Worker that processes ConductorWorkflow tasks.
     *
     * @param workerFactory the worker factory
     * @return configured and started Worker
     */
    @Bean
    public Worker conductorWorker(WorkerFactory workerFactory) {
        logger.info("Creating worker for task queue: {}", taskQueue);
        Worker worker = workerFactory.newWorker(taskQueue);
        worker.registerWorkflowImplementationTypes(ConductorWorkflowImpl.class,
                DefinitionWorkflowImpl.class);
        worker.registerActivitiesImplementations(new TaskExecutionActivitiesImpl());
        workerFactory.start();
        logger.info("Temporal worker started successfully");
        return worker;
    }

    /**
     * Returns the configured task queue name.
     *
     * @return the task queue name
     */
    public String getTaskQueue() {
        return taskQueue;
    }
}
