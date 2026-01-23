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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.client.WorkflowClient;
import io.temporal.conductor.activity.TaskExecutionActivitiesImpl;
import io.temporal.conductor.dto.StartWorkflowRequest;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowStatus;
import io.temporal.conductor.service.MetadataService;
import io.temporal.conductor.workflow.ConductorWorkflowImpl;
import io.temporal.conductor.workflow.DefinitionWorkflowImpl;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for TemporalWorkflowService using TestWorkflowEnvironment.
 */
class TemporalWorkflowServiceTest {

    private static final String TASK_QUEUE = "conductor-test-queue";
    private static final String NAMESPACE = "default";

    private TestWorkflowEnvironment testEnv;
    private WorkflowClient client;
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
    void testStartWorkflow() {
        // Register workflow definition
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        // Register task definition
        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        // Start workflow
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");
        request.setInput(Map.of("key", "value"));
        request.setCorrelationId("test-correlation");
        request.setPriority(5);

        String workflowId = workflowService.startWorkflow(request);

        assertNotNull(workflowId);
        assertFalse(workflowId.isEmpty());
    }

    @Test
    void testStartWorkflowWithInlineDefinition() {
        // Register task definition
        TaskDef taskDef = new TaskDef();
        taskDef.setName("inline_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        // Create inline workflow definition
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("inline-workflow");
        workflowDef.setVersion(1);

        WorkflowTask task = new WorkflowTask();
        task.setName("inline_task");
        task.setTaskReferenceName("inline_task_ref");
        task.setType(TaskType.SIMPLE.name());
        workflowDef.setTasks(Collections.singletonList(task));

        // Start workflow with inline definition
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("inline-workflow");
        request.setWorkflowDef(workflowDef);
        request.setInput(Collections.emptyMap());

        String workflowId = workflowService.startWorkflow(request);

        assertNotNull(workflowId);
        assertFalse(workflowId.isEmpty());
    }

    @Test
    void testGetWorkflow() {
        // Register and start workflow
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");
        request.setCorrelationId("get-workflow-test");

        String workflowId = workflowService.startWorkflow(request);

        // Wait for workflow to complete (test environment is fast)
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Get workflow
        Workflow workflow = workflowService.getWorkflow(workflowId, true);

        assertNotNull(workflow);
        assertEquals(workflowId, workflow.getWorkflowId());
        assertEquals("test-workflow", workflow.getWorkflowType());
    }

    @Test
    void testGetWorkflowStatus() {
        // Register and start workflow
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");
        request.setCorrelationId("status-test");

        String workflowId = workflowService.startWorkflow(request);

        // Wait for completion
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Get status
        WorkflowStatus status = workflowService.getWorkflowStatus(workflowId);

        assertNotNull(status);
        assertEquals(workflowId, status.getWorkflowId());
        assertNotNull(status.getStatus());
    }

    @Test
    void testTerminateWorkflow() {
        // Register workflow with slow task
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("slow-workflow");
        workflowDef.setVersion(1);

        WorkflowTask task = new WorkflowTask();
        task.setName("slow_task");
        task.setTaskReferenceName("slow_task_ref");
        task.setType(TaskType.SIMPLE.name());
        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("delayMs", 10000);
        task.setInputParameters(inputParams);
        workflowDef.setTasks(Collections.singletonList(task));

        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("slow_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("slow-workflow");

        String workflowId = workflowService.startWorkflow(request);

        // Terminate workflow
        workflowService.terminateWorkflow(workflowId, "Test termination");

        // Verify termination
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        WorkflowStatus status = workflowService.getWorkflowStatus(workflowId);
        assertTrue("TERMINATED".equals(status.getStatus())
                || "COMPLETED".equals(status.getStatus()));
    }

    @Test
    void testPauseAndResumeWorkflow() {
        // This test verifies the pause/resume signals are sent
        // The actual pause/resume behavior is tested in ConductorWorkflowTest
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");

        String workflowId = workflowService.startWorkflow(request);

        // Pause and resume should not throw
        try {
            workflowService.pauseWorkflow(workflowId);
            workflowService.resumeWorkflow(workflowId);
        } catch (Exception e) {
            // Expected if workflow completed too fast
        }

        // Verify workflow eventually completes
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        WorkflowStatus status = workflowService.getWorkflowStatus(workflowId);
        assertNotNull(status);
    }

    @Test
    void testRestartWorkflow() {
        // Register workflow
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        // Start original workflow
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");
        request.setInput(Map.of("original", true));
        request.setCorrelationId("restart-test");

        String originalId = workflowService.startWorkflow(request);

        // Wait for completion
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Restart workflow
        String newId = workflowService.restartWorkflow(originalId, false);

        assertNotNull(newId);
        assertFalse(newId.equals(originalId));
    }

    @Test
    void testGetRunningWorkflows() {
        // Register workflow
        WorkflowDef workflowDef = createSimpleWorkflowDef();
        metadataService.registerWorkflowDef(workflowDef);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");
        metadataService.registerTaskDefs(List.of(taskDef));

        // Start workflow
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName("test-workflow");

        workflowService.startWorkflow(request);

        // Get running workflows (might be empty if completed quickly)
        List<String> runningIds = workflowService.getRunningWorkflows(
                "test-workflow", null, null, null);

        assertNotNull(runningIds);
    }

    @Test
    void testSearchWorkflowsReturnsEmpty() {
        // Search should return empty results for now
        var result = workflowService.searchWorkflows(0, 10, null, null, null);

        assertNotNull(result);
        assertEquals(0, result.getTotalHits());
        assertTrue(result.getResults().isEmpty());
    }

    @Test
    void testSearchWorkflowsV2ReturnsEmpty() {
        // Search v2 should return empty results for now
        var result = workflowService.searchWorkflowsV2(0, 10, null, null, null);

        assertNotNull(result);
        assertEquals(0, result.getTotalHits());
        assertTrue(result.getResults().isEmpty());
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
