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

package io.temporal.conductor.e2e;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;

/**
 * Base class for E2E tests providing Docker Compose lifecycle management,
 * Temporal WorkflowClient, and Conductor REST API WebClient.
 */
public abstract class AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(AbstractE2ETest.class);
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected static final String TEMPORAL_SERVICE = "temporal";
    protected static final int TEMPORAL_PORT = 7233;
    protected static final String CONDUCTOR_SERVICE = "conductor-server";
    protected static final int CONDUCTOR_PORT = 8080;
    protected static final String NAMESPACE = "conductor";

    protected static ComposeContainer composeContainer;
    protected static WorkflowServiceStubs workflowServiceStubs;
    protected static WorkflowClient workflowClient;
    protected static WebClient conductorClient;

    @BeforeAll
    static void startContainers() {
        File composeFile = new File("src/e2eTest/resources/docker-compose-e2e.yml");
        if (!composeFile.exists()) {
            throw new IllegalStateException("docker-compose-e2e.yml not found at: " + composeFile.getAbsolutePath());
        }

        log.info("Starting Docker Compose stack from: {}", composeFile.getAbsolutePath());

        composeContainer = new ComposeContainer(composeFile)
                .withExposedService(TEMPORAL_SERVICE, TEMPORAL_PORT,
                        Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(2)))
                .withExposedService(CONDUCTOR_SERVICE, CONDUCTOR_PORT,
                        Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(3)))
                .withLocalCompose(true);

        composeContainer.start();

        // Get mapped ports
        String temporalHost = composeContainer.getServiceHost(TEMPORAL_SERVICE, TEMPORAL_PORT);
        int temporalPort = composeContainer.getServicePort(TEMPORAL_SERVICE, TEMPORAL_PORT);
        String temporalAddress = temporalHost + ":" + temporalPort;

        String conductorHost = composeContainer.getServiceHost(CONDUCTOR_SERVICE, CONDUCTOR_PORT);
        int conductorPort = composeContainer.getServicePort(CONDUCTOR_SERVICE, CONDUCTOR_PORT);
        String conductorBaseUrl = "http://" + conductorHost + ":" + conductorPort;

        log.info("Temporal server available at: {}", temporalAddress);
        log.info("Conductor server available at: {}", conductorBaseUrl);

        // Initialize Temporal client
        workflowServiceStubs = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder()
                        .setTarget(temporalAddress)
                        .build());

        workflowClient = WorkflowClient.newInstance(
                workflowServiceStubs,
                WorkflowClientOptions.newBuilder()
                        .setNamespace(NAMESPACE)
                        .build());

        // Initialize Conductor REST client
        conductorClient = WebClient.builder()
                .baseUrl(conductorBaseUrl)
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .build();

        // Wait for Conductor to be fully ready
        await().atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> {
                    try {
                        String health = conductorClient.get()
                                .uri("/actuator/health/readiness")
                                .retrieve()
                                .bodyToMono(String.class)
                                .block(Duration.ofSeconds(5));
                        return health != null && health.contains("UP");
                    } catch (Exception e) {
                        log.debug("Waiting for Conductor readiness: {}", e.getMessage());
                        return false;
                    }
                });

        log.info("Docker Compose stack is ready");
    }

    @AfterAll
    static void stopContainers() {
        if (workflowServiceStubs != null) {
            workflowServiceStubs.shutdown();
        }
        if (composeContainer != null) {
            composeContainer.stop();
        }
    }

    // ==================== Helper Methods ====================

    /**
     * Register a workflow definition via Conductor REST API.
     */
    protected void registerWorkflowDef(WorkflowDef workflowDef) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(workflowDef);
            conductorClient.post()
                    .uri("/api/metadata/workflow")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(json)
                    .retrieve()
                    .toBodilessEntity()
                    .block(Duration.ofSeconds(10));
            log.info("Registered workflow definition: {}", workflowDef.getName());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize workflow definition", e);
        }
    }

    /**
     * Register task definitions via Conductor REST API.
     */
    protected void registerTaskDefs(List<TaskDef> taskDefs) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(taskDefs);
            conductorClient.post()
                    .uri("/api/metadata/taskdefs")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(json)
                    .retrieve()
                    .toBodilessEntity()
                    .block(Duration.ofSeconds(10));
            log.info("Registered {} task definitions", taskDefs.size());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize task definitions", e);
        }
    }

    /**
     * Start a workflow via Conductor REST API.
     *
     * @return workflow ID
     */
    protected String startWorkflow(String workflowName, int version, Map<String, Object> input) {
        Map<String, Object> request = new HashMap<>();
        request.put("name", workflowName);
        request.put("version", version);
        request.put("input", input != null ? input : Collections.emptyMap());

        try {
            String json = OBJECT_MAPPER.writeValueAsString(request);
            String workflowId = conductorClient.post()
                    .uri("/api/workflow")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(json)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(Duration.ofSeconds(10));
            log.info("Started workflow {} with ID: {}", workflowName, workflowId);
            // Remove quotes if present
            return workflowId != null ? workflowId.replace("\"", "") : null;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize workflow start request", e);
        }
    }

    /**
     * Get workflow status via Conductor REST API.
     */
    protected WorkflowStatusResponse getWorkflowStatus(String workflowId) {
        String json = conductorClient.get()
                .uri("/api/workflow/{workflowId}", workflowId)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        try {
            return OBJECT_MAPPER.readValue(json, WorkflowStatusResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse workflow status", e);
        }
    }

    /**
     * Wait for workflow to complete.
     */
    protected WorkflowStatusResponse waitForWorkflowCompletion(String workflowId, Duration timeout) {
        return await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> getWorkflowStatus(workflowId),
                        status -> "COMPLETED".equals(status.getStatus())
                                || "FAILED".equals(status.getStatus())
                                || "TERMINATED".equals(status.getStatus())
                                || "TIMED_OUT".equals(status.getStatus()));
    }

    /**
     * Get workflow execution history from Temporal.
     */
    protected List<HistoryEvent> getWorkflowHistory(String workflowId) {
        GetWorkflowExecutionHistoryResponse response = workflowServiceStubs.blockingStub()
                .getWorkflowExecutionHistory(
                        GetWorkflowExecutionHistoryRequest.newBuilder()
                                .setNamespace(NAMESPACE)
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .build())
                                .build());
        return response.getHistory().getEventsList();
    }

    /**
     * Extract the Temporal workflow type from history.
     */
    protected String getWorkflowTypeFromHistory(String workflowId) {
        List<HistoryEvent> events = getWorkflowHistory(workflowId);
        for (HistoryEvent event : events) {
            if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
                return event.getWorkflowExecutionStartedEventAttributes()
                        .getWorkflowType()
                        .getName();
            }
        }
        throw new IllegalStateException("Workflow type not found in history for: " + workflowId);
    }

    /**
     * Extract all activity types from workflow history.
     */
    protected List<String> getActivityTypesFromHistory(String workflowId) {
        List<String> activityTypes = new ArrayList<>();
        List<HistoryEvent> events = getWorkflowHistory(workflowId);
        for (HistoryEvent event : events) {
            if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
                String activityType = event.getActivityTaskScheduledEventAttributes()
                        .getActivityType()
                        .getName();
                activityTypes.add(activityType);
            }
        }
        return activityTypes;
    }

    /**
     * Count consecutive activity scheduled events (for parallel verification).
     */
    protected int getMaxConsecutiveActivityScheduledEvents(String workflowId) {
        List<HistoryEvent> events = getWorkflowHistory(workflowId);
        int maxConsecutive = 0;
        int currentConsecutive = 0;

        for (HistoryEvent event : events) {
            if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
                currentConsecutive++;
                maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
            } else {
                currentConsecutive = 0;
            }
        }
        return maxConsecutive;
    }

    // ==================== Workflow Definition Builders ====================

    /**
     * Create a simple workflow definition with one task.
     */
    protected WorkflowDef createSimpleWorkflowDef(String workflowName, String taskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask task = new WorkflowTask();
        task.setName(taskName);
        task.setTaskReferenceName(taskName + "_ref");
        task.setType(TaskType.SIMPLE.name());

        workflowDef.setTasks(Collections.singletonList(task));
        return workflowDef;
    }

    /**
     * Create a workflow definition with multiple sequential tasks.
     */
    protected WorkflowDef createSequentialWorkflowDef(String workflowName, List<String> taskNames) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();
        for (String taskName : taskNames) {
            WorkflowTask task = new WorkflowTask();
            task.setName(taskName);
            task.setTaskReferenceName(taskName + "_ref");
            task.setType(TaskType.SIMPLE.name());
            tasks.add(task);
        }

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a workflow definition with FORK_JOIN.
     */
    protected WorkflowDef createForkJoinWorkflowDef(String workflowName, List<String> branchTaskNames, String finalTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // FORK task
        WorkflowTask forkTask = new WorkflowTask();
        forkTask.setName("fork_ref");
        forkTask.setTaskReferenceName("fork_ref");
        forkTask.setType(TaskType.FORK_JOIN.name());

        List<List<WorkflowTask>> forkTasks = new ArrayList<>();
        List<String> joinOn = new ArrayList<>();

        for (String branchTaskName : branchTaskNames) {
            WorkflowTask branchTask = new WorkflowTask();
            branchTask.setName(branchTaskName);
            branchTask.setTaskReferenceName(branchTaskName + "_ref");
            branchTask.setType(TaskType.SIMPLE.name());
            forkTasks.add(Collections.singletonList(branchTask));
            joinOn.add(branchTaskName + "_ref");
        }

        forkTask.setForkTasks(forkTasks);
        tasks.add(forkTask);

        // JOIN task
        WorkflowTask joinTask = new WorkflowTask();
        joinTask.setName("join_ref");
        joinTask.setTaskReferenceName("join_ref");
        joinTask.setType(TaskType.JOIN.name());
        joinTask.setJoinOn(joinOn);
        tasks.add(joinTask);

        // Final task after join
        if (finalTaskName != null) {
            WorkflowTask finalTask = new WorkflowTask();
            finalTask.setName(finalTaskName);
            finalTask.setTaskReferenceName(finalTaskName + "_ref");
            finalTask.setType(TaskType.SIMPLE.name());
            tasks.add(finalTask);
        }

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create task definitions for a list of task names.
     */
    protected List<TaskDef> createTaskDefs(List<String> taskNames) {
        List<TaskDef> taskDefs = new ArrayList<>();
        for (String taskName : taskNames) {
            TaskDef taskDef = new TaskDef();
            taskDef.setName(taskName);
            taskDefs.add(taskDef);
        }
        return taskDefs;
    }
}
