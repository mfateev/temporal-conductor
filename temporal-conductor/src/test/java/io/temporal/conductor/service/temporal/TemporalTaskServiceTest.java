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

package io.temporal.conductor.service.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.client.WorkflowClient;
import io.temporal.conductor.activity.TaskExecutionActivitiesImpl;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.StartWorkflowRequest;
import io.temporal.conductor.dto.TaskSummary;
import io.temporal.conductor.workflow.ConductorWorkflowImpl;
import io.temporal.conductor.workflow.DefinitionWorkflowImpl;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for TemporalTaskService.
 */
class TemporalTaskServiceTest {

    private static final String TASK_QUEUE = "conductor-test-queue";
    private static final String NAMESPACE = "default";

    private TestWorkflowEnvironment testEnv;
    private WorkflowClient client;
    private TemporalTaskService taskService;
    private TemporalWorkflowService workflowService;
    private TemporalMetadataService metadataService;

    @BeforeEach
    void setUp() {
        TestEnvironmentOptions options = TestEnvironmentOptions.newBuilder()
                .registerSearchAttribute("ConductorWorkflowType",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorWorkflowVersion",
                        IndexedValueType.INDEXED_VALUE_TYPE_INT)
                .registerSearchAttribute("ConductorStatus",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorCorrelationId",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorPriority",
                        IndexedValueType.INDEXED_VALUE_TYPE_INT)
                .registerSearchAttribute("ConductorFailedTaskNames",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD_LIST)
                .registerSearchAttribute("ConductorOwnerApp",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorDefinitionType",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorDefinitionName",
                        IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD)
                .registerSearchAttribute("ConductorDefinitionVersion",
                        IndexedValueType.INDEXED_VALUE_TYPE_INT)
                .build();

        testEnv = TestWorkflowEnvironment.newInstance(options);
        Worker worker = testEnv.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(ConductorWorkflowImpl.class,
                DefinitionWorkflowImpl.class);
        worker.registerActivitiesImplementations(new TaskExecutionActivitiesImpl());
        testEnv.start();

        client = testEnv.getWorkflowClient();
        taskService = new TemporalTaskService(client);
        metadataService = new TemporalMetadataService(client, new ObjectMapper(),
                TASK_QUEUE, NAMESPACE);
        workflowService = new TemporalWorkflowService(
                client, metadataService, TASK_QUEUE, NAMESPACE);
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    @Test
    void testAddAndGetTaskLogs() {
        String taskId = "test-task-123";

        taskService.addTaskLog(taskId, "First log entry");
        taskService.addTaskLog(taskId, "Second log entry");
        taskService.addTaskLog(taskId, "Third log entry");

        List<TaskExecLog> logs = taskService.getTaskLogs(taskId);

        assertEquals(3, logs.size());
        assertEquals("First log entry", logs.get(0).getLog());
        assertEquals("Second log entry", logs.get(1).getLog());
        assertEquals("Third log entry", logs.get(2).getLog());
    }

    @Test
    void testGetTaskLogsEmpty() {
        List<TaskExecLog> logs = taskService.getTaskLogs("non-existent-task");

        assertNotNull(logs);
        assertTrue(logs.isEmpty());
    }

    @Test
    void testTaskLogContainsTimestamp() {
        String taskId = "timestamp-test-task";

        long beforeTime = System.currentTimeMillis();
        taskService.addTaskLog(taskId, "Log with timestamp");
        long afterTime = System.currentTimeMillis();

        List<TaskExecLog> logs = taskService.getTaskLogs(taskId);

        assertEquals(1, logs.size());
        TaskExecLog log = logs.get(0);
        assertTrue(log.getCreatedTime() >= beforeTime);
        assertTrue(log.getCreatedTime() <= afterTime);
    }

    @Test
    void testTaskLogContainsTaskId() {
        String taskId = "task-id-test";

        taskService.addTaskLog(taskId, "Test log");

        List<TaskExecLog> logs = taskService.getTaskLogs(taskId);

        assertEquals(1, logs.size());
        assertEquals(taskId, logs.get(0).getTaskId());
    }

    @Test
    void testPollReturnsNull() {
        // External polling is not supported in this POC
        Task task = taskService.poll("SIMPLE", "worker-1", null);

        assertNull(task);
    }

    @Test
    void testSearchTasksReturnsEmpty() {
        SearchResult<TaskSummary> result = taskService.searchTasks(
                0, 10, null, null, null);

        assertNotNull(result);
        assertEquals(0, result.getTotalHits());
        assertTrue(result.getResults().isEmpty());
    }

    @Test
    void testGetTaskWithoutMapping() {
        // When task is not mapped, should return placeholder
        Task task = taskService.getTask("unmapped-task-id");

        assertNotNull(task);
        assertEquals("unmapped-task-id", task.getTaskId());
        assertEquals(Task.Status.SCHEDULED, task.getStatus());
    }

    @Test
    void testRegisterTaskMapping() {
        String taskId = "mapped-task-id";
        String workflowId = "workflow-123";

        taskService.registerTaskMapping(taskId, workflowId);

        // The mapping should be stored (internal state)
        // We can verify indirectly through getTask behavior
        Task task = taskService.getTask(taskId);
        assertNotNull(task);
    }

    @Test
    void testUpdateTask() {
        // Register workflow and task definitions
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        // Start workflow
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");
        String workflowId = workflowService.startWorkflow(request);

        // Wait for workflow to progress
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Create task result
        TaskResult taskResult = new TaskResult();
        taskResult.setTaskId("test-task-id");
        taskResult.setWorkflowInstanceId(workflowId);
        taskResult.setStatus(TaskResult.Status.COMPLETED);
        taskResult.setOutputData(Map.of("result", "success"));

        // Update task (should not throw)
        String returnedTaskId = taskService.updateTask(taskResult);

        assertEquals("test-task-id", returnedTaskId);
    }

    @Test
    void testUpdateTaskWithoutWorkflowId() {
        TaskResult taskResult = new TaskResult();
        taskResult.setTaskId("orphan-task-id");
        taskResult.setStatus(TaskResult.Status.COMPLETED);

        // Should handle gracefully when workflow ID is not found
        String returnedTaskId = taskService.updateTask(taskResult);

        assertEquals("orphan-task-id", returnedTaskId);
    }

    @Test
    void testMultipleTaskLogsIsolation() {
        String taskId1 = "task-1";
        String taskId2 = "task-2";

        taskService.addTaskLog(taskId1, "Task 1 log");
        taskService.addTaskLog(taskId2, "Task 2 log");

        List<TaskExecLog> logs1 = taskService.getTaskLogs(taskId1);
        List<TaskExecLog> logs2 = taskService.getTaskLogs(taskId2);

        assertEquals(1, logs1.size());
        assertEquals(1, logs2.size());
        assertEquals("Task 1 log", logs1.get(0).getLog());
        assertEquals("Task 2 log", logs2.get(0).getLog());
    }

    @Test
    void testGetTaskFromWorkflow() {
        // Register workflow and task definitions
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        // Start workflow
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");
        String workflowId = workflowService.startWorkflow(request);

        // Wait for workflow to complete
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Get workflow to find task ID
        var workflow = workflowService.getWorkflow(workflowId, true);

        if (workflow.getTasks() != null && !workflow.getTasks().isEmpty()) {
            String taskId = workflow.getTasks().get(0).getTaskId();

            // Register mapping
            taskService.registerTaskMapping(taskId, workflowId);

            // Get task via task service
            Task task = taskService.getTask(taskId);

            assertNotNull(task);
            assertEquals(taskId, task.getTaskId());
        }
    }

    private WorkflowDef createSimpleWorkflowDef() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("test-workflow");
        workflowDef.setVersion(1);

        WorkflowTask task = new WorkflowTask();
        task.setName("simple_task");
        task.setTaskReferenceName("simple_task_ref");
        task.setType(TaskType.SIMPLE.name());

        workflowDef.setTasks(Collections.singletonList(task));
        return workflowDef;
    }
}
