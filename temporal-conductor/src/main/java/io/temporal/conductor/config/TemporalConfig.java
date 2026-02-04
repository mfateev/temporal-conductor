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
import io.temporal.conductor.activity.EventPublishActivity;
import io.temporal.conductor.activity.ExtensionTaskExecutionActivityImpl;
import io.temporal.conductor.activity.TaskExecutionActivitiesImpl;
import io.temporal.conductor.workflow.ConductorWorkflowImpl;
import io.temporal.conductor.workflow.DefinitionWorkflowImpl;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Temporal worker configuration for the conductor-server-temporal application.
 *
 * <p>This configuration is only active when the "temporal" profile is enabled.
 * It uses the WorkflowClient provided by the Temporal Spring Boot starter and
 * manually registers the DynamicWorkflow and DynamicActivity implementations
 * since they cannot be auto-discovered.
 *
 * <p>Connection configuration is handled by the Spring Boot starter via
 * spring.temporal.* properties in application.yml.
 */
@Configuration
@Profile("temporal")
public class TemporalConfig {

    private static final Logger logger = LoggerFactory.getLogger(TemporalConfig.class);

    @Value("${temporal.task-queue:conductor-workflows}")
    private String taskQueue;

    /**
     * Creates the WorkerFactory for managing Temporal workers.
     * Uses the WorkflowClient provided by the Spring Boot starter.
     *
     * @param workflowClient the workflow client from Spring Boot starter
     * @return configured WorkerFactory
     */
    @Bean
    public WorkerFactory workerFactory(WorkflowClient workflowClient) {
        logger.info("Creating WorkerFactory using Spring Boot starter WorkflowClient");
        return WorkerFactory.newInstance(workflowClient);
    }

    /**
     * Creates a Worker that processes Conductor workflows.
     *
     * <p>ConductorWorkflowImpl implements DynamicWorkflow, allowing the Temporal
     * workflow type to match the Conductor workflow name (e.g., "hello_workflow").
     * This requires manual registration since DynamicWorkflow cannot be auto-discovered.
     *
     * @param workerFactory the worker factory
     * @param taskExecutionActivities the dynamic activity for task execution
     * @param eventPublishActivity the activity for EVENT task publishing
     * @param extensionTaskActivity the activity for extension task execution
     * @return configured Worker
     */
    @Bean
    public Worker conductorWorker(
            WorkerFactory workerFactory,
            TaskExecutionActivitiesImpl taskExecutionActivities,
            EventPublishActivity eventPublishActivity,
            ExtensionTaskExecutionActivityImpl extensionTaskActivity) {
        logger.info("Creating worker for task queue: {}", taskQueue);
        Worker worker = workerFactory.newWorker(taskQueue);

        // Register DynamicWorkflow implementations - handles any workflow type
        worker.registerWorkflowImplementationTypes(
                ConductorWorkflowImpl.class,
                DefinitionWorkflowImpl.class);

        // Register activity implementations (Spring beans with injected dependencies)
        worker.registerActivitiesImplementations(
                taskExecutionActivities, eventPublishActivity, extensionTaskActivity);

        logger.info("Worker configured with DynamicWorkflow and activities (including {} extension tasks)",
                extensionTaskActivity.getRegisteredTaskTypes().size());
        return worker;
    }

    /**
     * Starts the WorkerFactory when the application is ready.
     */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> workerFactoryStarter(WorkerFactory workerFactory) {
        return event -> {
            logger.info("Starting Temporal WorkerFactory");
            workerFactory.start();
            logger.info("Temporal worker started successfully on task queue: {}", taskQueue);
        };
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
