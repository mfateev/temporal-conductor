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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.ComposeContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;

/**
 * Base class for E2E tests providing Docker Compose lifecycle management,
 * Temporal WorkflowClient, and Conductor REST API WebClient.
 *
 * <p>Uses singleton container pattern - containers are started once and reused
 * across all test classes for faster test execution.
 *
 * <p>All tests inherit a 90-second timeout - tests should fail fast rather than hang.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
public abstract class AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(AbstractE2ETest.class);
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final String NAMESPACE = "conductor";

    protected static ComposeContainer composeContainer;
    protected static WorkflowServiceStubs workflowServiceStubs;
    protected static WorkflowClient workflowClient;
    protected static WebClient conductorClient;

    @BeforeAll
    @Timeout(value = 5, unit = TimeUnit.MINUTES)  // Container startup (image should be pre-built by Gradle)
    static void initializeSharedContainers() {
        // Use singleton pattern - containers are started once and reused across all test classes
        SharedE2EContainers shared = SharedE2EContainers.getInstance();
        composeContainer = shared.getComposeContainer();
        workflowServiceStubs = shared.getWorkflowServiceStubs();
        workflowClient = shared.getWorkflowClient();
        conductorClient = shared.getConductorClient();
        log.info("Using shared E2E containers");
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
     * Uses pagination to retrieve all events for large histories.
     */
    protected List<HistoryEvent> getWorkflowHistory(String workflowId) {
        List<HistoryEvent> allEvents = new ArrayList<>();
        com.google.protobuf.ByteString nextPageToken = com.google.protobuf.ByteString.EMPTY;

        do {
            GetWorkflowExecutionHistoryRequest.Builder requestBuilder =
                    GetWorkflowExecutionHistoryRequest.newBuilder()
                            .setNamespace(NAMESPACE)
                            .setExecution(WorkflowExecution.newBuilder()
                                    .setWorkflowId(workflowId)
                                    .build());

            if (!nextPageToken.isEmpty()) {
                requestBuilder.setNextPageToken(nextPageToken);
            }

            GetWorkflowExecutionHistoryResponse response = workflowServiceStubs.blockingStub()
                    .getWorkflowExecutionHistory(requestBuilder.build());

            allEvents.addAll(response.getHistory().getEventsList());
            nextPageToken = response.getNextPageToken();
        } while (!nextPageToken.isEmpty());

        return allEvents;
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
     * Extract all activity types from workflow history across all continue-as-new runs.
     * This traverses the full chain of workflow executions to count total activities.
     */
    protected List<String> getActivityTypesFromHistory(String workflowId) {
        List<String> activityTypes = new ArrayList<>();

        // Get all events from all runs by traversing continue-as-new chain
        List<HistoryEvent> allEvents = getWorkflowHistoryAllRuns(workflowId);

        for (HistoryEvent event : allEvents) {
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
     * Get workflow history for all runs, traversing continue-as-new chain backward
     * from the latest run to the first run, then concatenating in chronological order.
     */
    protected List<HistoryEvent> getWorkflowHistoryAllRuns(String workflowId) {
        List<List<HistoryEvent>> allRunHistories = new ArrayList<>();
        String currentRunId = null; // null means latest run

        // Traverse backward from latest run to first run
        while (true) {
            List<HistoryEvent> runHistory = getWorkflowHistoryForRun(workflowId, currentRunId);
            allRunHistories.add(0, runHistory); // Prepend to maintain chronological order

            // Check if this run was continued from a previous run
            String previousRunId = null;
            for (HistoryEvent event : runHistory) {
                if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED) {
                    var attrs = event.getWorkflowExecutionStartedEventAttributes();
                    if (!attrs.getContinuedExecutionRunId().isEmpty()) {
                        previousRunId = attrs.getContinuedExecutionRunId();
                    }
                    break;
                }
            }

            if (previousRunId == null || previousRunId.isEmpty()) {
                // This is the first run, we're done
                break;
            }

            currentRunId = previousRunId;
        }

        // Concatenate all histories in chronological order
        List<HistoryEvent> allEvents = new ArrayList<>();
        for (List<HistoryEvent> runHistory : allRunHistories) {
            allEvents.addAll(runHistory);
        }

        log.debug("Retrieved history from {} continue-as-new runs, total {} events",
                allRunHistories.size(), allEvents.size());

        return allEvents;
    }

    /**
     * Get workflow history for a specific run.
     * @param workflowId the workflow ID
     * @param runId the run ID, or null for the latest run
     */
    protected List<HistoryEvent> getWorkflowHistoryForRun(String workflowId, String runId) {
        List<HistoryEvent> allEvents = new ArrayList<>();
        com.google.protobuf.ByteString nextPageToken = com.google.protobuf.ByteString.EMPTY;

        do {
            WorkflowExecution.Builder executionBuilder = WorkflowExecution.newBuilder()
                    .setWorkflowId(workflowId);
            if (runId != null && !runId.isEmpty()) {
                executionBuilder.setRunId(runId);
            }

            GetWorkflowExecutionHistoryRequest.Builder requestBuilder =
                    GetWorkflowExecutionHistoryRequest.newBuilder()
                            .setNamespace(NAMESPACE)
                            .setExecution(executionBuilder.build());

            if (!nextPageToken.isEmpty()) {
                requestBuilder.setNextPageToken(nextPageToken);
            }

            GetWorkflowExecutionHistoryResponse response = workflowServiceStubs.blockingStub()
                    .getWorkflowExecutionHistory(requestBuilder.build());

            allEvents.addAll(response.getHistory().getEventsList());
            nextPageToken = response.getNextPageToken();
        } while (!nextPageToken.isEmpty());

        return allEvents;
    }

    /**
     * Count consecutive activity scheduled events (for parallel verification).
     * Only looks at single run history since consecutive events are measured within a run.
     */
    protected int getMaxConsecutiveActivityScheduledEvents(String workflowId) {
        // For consecutive events, we only look at single run since they wouldn't be consecutive across runs
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

    /**
     * Create a workflow definition with a slow task (for pause/resume testing).
     * The slow task sleeps for the specified duration, keeping the workflow RUNNING.
     */
    protected WorkflowDef createSlowWorkflowDef(String workflowName, long delayMs) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask slowTask = new WorkflowTask();
        slowTask.setName("slow_task");
        slowTask.setTaskReferenceName("slow_task_ref");
        slowTask.setType(TaskType.SIMPLE.name());
        // Set input with delay duration
        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("delayMs", delayMs);
        slowTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(slowTask));
        return workflowDef;
    }

    // ==================== Workflow Control Methods ====================

    /**
     * Pause a running workflow via Conductor REST API.
     */
    protected void pauseWorkflow(String workflowId) {
        conductorClient.put()
                .uri("/api/workflow/{workflowId}/pause", workflowId)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
        log.info("Paused workflow: {}", workflowId);
    }

    /**
     * Resume a paused workflow via Conductor REST API.
     */
    protected void resumeWorkflow(String workflowId) {
        conductorClient.put()
                .uri("/api/workflow/{workflowId}/resume", workflowId)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
        log.info("Resumed workflow: {}", workflowId);
    }

    /**
     * Wait for workflow to reach a specific status.
     */
    protected WorkflowStatusResponse waitForWorkflowStatus(String workflowId, String expectedStatus, Duration timeout) {
        return await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> getWorkflowStatus(workflowId),
                        status -> expectedStatus.equals(status.getStatus()));
    }

    /**
     * Get a search attribute value from Temporal workflow execution.
     */
    protected String getSearchAttribute(String workflowId, String attributeName) {
        var response = workflowServiceStubs.blockingStub()
                .describeWorkflowExecution(
                        io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest.newBuilder()
                                .setNamespace(NAMESPACE)
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .build())
                                .build());

        var searchAttributes = response.getWorkflowExecutionInfo().getSearchAttributes();
        var indexedFields = searchAttributes.getIndexedFieldsMap();

        if (indexedFields.containsKey(attributeName)) {
            var payload = indexedFields.get(attributeName);
            // The value is JSON encoded, extract the string value
            String data = payload.getData().toStringUtf8();
            // Remove quotes if present (JSON string encoding)
            if (data.startsWith("\"") && data.endsWith("\"")) {
                return data.substring(1, data.length() - 1);
            }
            return data;
        }
        return null;
    }
}
