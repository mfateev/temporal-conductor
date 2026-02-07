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
package io.temporal.conductor.workflow;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.activity.Activity;
import io.temporal.activity.DynamicActivity;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.converter.EncodedValues;
import io.temporal.conductor.activity.TaskExecutionActivitiesImpl;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for external worker functionality using TestWorkflowEnvironment.
 *
 * <p>Verifies that tasks with names in the format {@code taskName@taskQueue} are:
 * <ul>
 *   <li>Routed to the correct task queue</li>
 *   <li>Registered with the base name (without @taskQueue suffix) as the activity type</li>
 * </ul>
 */
@Timeout(60)
class ExternalWorkerTest {

    private static final Logger log = LoggerFactory.getLogger(ExternalWorkerTest.class);

    private static final String DEFAULT_TASK_QUEUE = "conductor-test-queue";
    private static final String EXTERNAL_TASK_QUEUE = "external-workers";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TestWorkflowEnvironment testEnv;
    private Worker defaultWorker;
    private Worker externalWorker;
    private WorkflowClient client;

    @BeforeEach
    void setUp() {
        TestEnvironmentOptions options = TestEnvironmentOptions.newBuilder()
                .registerSearchAttribute("ConductorWorkflowType", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorWorkflowVersion", IndexedValueType.INDEXED_VALUE_TYPE_INT)
                .registerSearchAttribute("ConductorStatus", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorCorrelationId", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorPriority", IndexedValueType.INDEXED_VALUE_TYPE_INT)
                .registerSearchAttribute("ConductorFailedTaskNames", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD_LIST)
                .registerSearchAttribute("ConductorOwnerApp", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .build();
        testEnv = TestWorkflowEnvironment.newInstance(options);

        // Worker on default task queue (handles workflow and internal activities)
        defaultWorker = testEnv.newWorker(DEFAULT_TASK_QUEUE);
        defaultWorker.registerWorkflowImplementationTypes(ConductorWorkflowImpl.class);
        defaultWorker.registerActivitiesImplementations(new TaskExecutionActivitiesImpl());

        // Worker on external task queue (simulates external worker)
        externalWorker = testEnv.newWorker(EXTERNAL_TASK_QUEUE);
        externalWorker.registerActivitiesImplementations(new ExternalWorkerActivity());

        testEnv.start();
        client = testEnv.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    @Test
    void testTaskRoutedToExternalWorker() throws Exception {
        // Create workflow with task that routes to external queue
        String taskName = "process_order@" + EXTERNAL_TASK_QUEUE;
        ConductorWorkflowInput input = createExternalTaskWorkflowInput(taskName);

        String workflowId = "test-external-worker-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(DEFAULT_TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("external-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Get workflow history to verify activity type
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);

        assertEquals(1, activityTypes.size(), "Should have exactly one activity");

        // KEY ASSERTION: Activity type should be the base name without @taskQueue suffix
        assertEquals("process_order", activityTypes.get(0),
                "Activity type should be base name without @taskQueue suffix");
    }

    @Test
    void testActivityScheduledOnCorrectTaskQueue() throws Exception {
        // Create workflow with task that routes to external queue
        String taskName = "process_order@" + EXTERNAL_TASK_QUEUE;
        ConductorWorkflowInput input = createExternalTaskWorkflowInput(taskName);

        String workflowId = "test-external-queue-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(DEFAULT_TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("external-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Get workflow history to verify task queue
        List<String> taskQueues = getActivityTaskQueuesFromHistory(workflowId);
        log.info("Activity task queues from history: {}", taskQueues);

        assertEquals(1, taskQueues.size(), "Should have exactly one activity");
        assertEquals(EXTERNAL_TASK_QUEUE, taskQueues.get(0),
                "Activity should be scheduled on external task queue");
    }

    @Test
    void testTaskWithoutSuffixUsesDefaultQueue() throws Exception {
        // Create workflow with task that uses default queue (no suffix)
        String taskName = "internal_task";
        ConductorWorkflowInput input = createExternalTaskWorkflowInput(taskName);

        String workflowId = "test-default-queue-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(DEFAULT_TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("external-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify activity type equals task name (no parsing needed)
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        assertEquals("internal_task", activityTypes.get(0));

        // Verify task queue is empty (uses workflow's default)
        List<String> taskQueues = getActivityTaskQueuesFromHistory(workflowId);
        // When no task queue is specified, it inherits from workflow
        assertTrue(taskQueues.get(0).isEmpty() || taskQueues.get(0).equals(DEFAULT_TASK_QUEUE),
                "Activity should use default task queue");
    }

    @Test
    void testMixedInternalAndExternalTasks() throws Exception {
        ConductorWorkflowInput input = createMixedTasksWorkflowInput();

        String workflowId = "test-mixed-tasks-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(DEFAULT_TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("mixed-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify activity types
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);

        assertEquals(3, activityTypes.size(), "Should have 3 activities");
        assertEquals("internal_task", activityTypes.get(0), "First task should be internal");
        assertEquals("external_task", activityTypes.get(1), "Second task should be external (base name)");
        assertEquals("internal_task", activityTypes.get(2), "Third task should be internal");
    }

    // Helper: Extract activity types from workflow history
    private List<String> getActivityTypesFromHistory(String workflowId) {
        List<String> activityTypes = new ArrayList<>();
        GetWorkflowExecutionHistoryResponse response = testEnv.getWorkflowServiceStubs()
                .blockingStub()
                .getWorkflowExecutionHistory(
                        GetWorkflowExecutionHistoryRequest.newBuilder()
                                .setNamespace(client.getOptions().getNamespace())
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .build())
                                .build());

        for (HistoryEvent event : response.getHistory().getEventsList()) {
            if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
                String activityType = event.getActivityTaskScheduledEventAttributes()
                        .getActivityType()
                        .getName();
                activityTypes.add(activityType);
            }
        }
        return activityTypes;
    }

    // Helper: Extract activity task queues from workflow history
    private List<String> getActivityTaskQueuesFromHistory(String workflowId) {
        List<String> taskQueues = new ArrayList<>();
        GetWorkflowExecutionHistoryResponse response = testEnv.getWorkflowServiceStubs()
                .blockingStub()
                .getWorkflowExecutionHistory(
                        GetWorkflowExecutionHistoryRequest.newBuilder()
                                .setNamespace(client.getOptions().getNamespace())
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .build())
                                .build());

        for (HistoryEvent event : response.getHistory().getEventsList()) {
            if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
                String taskQueue = event.getActivityTaskScheduledEventAttributes()
                        .getTaskQueue()
                        .getName();
                taskQueues.add(taskQueue);
            }
        }
        return taskQueues;
    }

    // Helper: Create workflow input with a single task
    private ConductorWorkflowInput createExternalTaskWorkflowInput(String taskDefName) throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("external-workflow");
        workflowDef.setVersion(1);

        WorkflowTask task = new WorkflowTask();
        task.setName(taskDefName);
        task.setTaskReferenceName("task_ref");
        task.setType(TaskType.SIMPLE.name());

        workflowDef.setTasks(Collections.singletonList(task));

        // Task definition uses the full name including @suffix
        TaskDef taskDef = new TaskDef();
        taskDef.setName(taskDefName);

        Map<String, String> taskDefsJson = new HashMap<>();
        taskDefsJson.put(taskDefName, OBJECT_MAPPER.writeValueAsString(taskDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    // Helper: Create workflow with mixed internal and external tasks
    private ConductorWorkflowInput createMixedTasksWorkflowInput() throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("mixed-workflow");
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // Internal task 1
        WorkflowTask task1 = new WorkflowTask();
        task1.setName("internal_task");
        task1.setTaskReferenceName("internal_task_1_ref");
        task1.setType(TaskType.SIMPLE.name());
        tasks.add(task1);

        // External task
        WorkflowTask task2 = new WorkflowTask();
        task2.setName("external_task@" + EXTERNAL_TASK_QUEUE);
        task2.setTaskReferenceName("external_task_ref");
        task2.setType(TaskType.SIMPLE.name());
        tasks.add(task2);

        // Internal task 2
        WorkflowTask task3 = new WorkflowTask();
        task3.setName("internal_task");
        task3.setTaskReferenceName("internal_task_2_ref");
        task3.setType(TaskType.SIMPLE.name());
        tasks.add(task3);

        workflowDef.setTasks(tasks);

        Map<String, String> taskDefsJson = new HashMap<>();

        TaskDef internalTaskDef = new TaskDef();
        internalTaskDef.setName("internal_task");
        taskDefsJson.put("internal_task", OBJECT_MAPPER.writeValueAsString(internalTaskDef));

        TaskDef externalTaskDef = new TaskDef();
        externalTaskDef.setName("external_task@" + EXTERNAL_TASK_QUEUE);
        taskDefsJson.put("external_task@" + EXTERNAL_TASK_QUEUE, OBJECT_MAPPER.writeValueAsString(externalTaskDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    /**
     * Dynamic activity for external worker that handles any activity type.
     */
    public static class ExternalWorkerActivity implements DynamicActivity {

        private static final Logger activityLog = LoggerFactory.getLogger(ExternalWorkerActivity.class);

        @Override
        @SuppressWarnings("unchecked")
        public Object execute(EncodedValues args) {
            String activityType = Activity.getExecutionContext().getInfo().getActivityType();
            String taskRefName = args.get(0, String.class);
            String conductorTaskType = args.get(1, String.class);
            Map<String, Object> inputData = args.get(2, Map.class);

            activityLog.info("EXTERNAL WORKER: Executing activity '{}' for task '{}'",
                    activityType, taskRefName);

            // Build output
            Map<String, Object> output = new HashMap<>();
            if (inputData != null) {
                output.putAll(inputData);
            }
            output.put("_processedBy", "external-worker");
            output.put("_activityType", activityType);

            // Return result in TaskExecutionResult format
            Map<String, Object> result = new HashMap<>();
            result.put("output", output);
            result.put("status", "COMPLETED");
            result.put("failureReason", null);

            return result;
        }
    }
}
