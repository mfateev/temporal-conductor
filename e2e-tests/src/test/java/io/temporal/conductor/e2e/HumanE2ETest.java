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

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.client.WorkflowStub;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for HUMAN task type against real Temporal server.
 * HUMAN tasks wait indefinitely for external completion via signal.
 */
class HumanE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(HumanE2ETest.class);

    @Test
    void testSimpleHumanTask() throws Exception {
        String workflowName = "human_simple_e2e";

        // Create workflow with HUMAN task
        WorkflowDef workflowDef = createWorkflowWithHumanTask(workflowName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with HUMAN task: {}", workflowId);

        // Wait for HUMAN task to be IN_PROGRESS
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            WorkflowStatusResponse status = getWorkflowStatus(workflowId);
            return status.getTasks().stream()
                    .anyMatch(t -> "human_task_ref".equals(t.getReferenceTaskName())
                            && "IN_PROGRESS".equals(t.getStatus()));
        });

        log.info("HUMAN task is IN_PROGRESS, sending completion signal");

        // Complete the HUMAN task via signal
        Map<String, Object> output = new HashMap<>();
        output.put("approved", true);
        output.put("approver", "test-user");
        completeTaskViaSignal(workflowId, "human_task_ref", output);

        // Wait for workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify HUMAN task completed with output
        WorkflowStatusResponse.TaskInfo humanTask = status.getTasks().stream()
                .filter(t -> "human_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(humanTask);
        assertEquals("COMPLETED", humanTask.getStatus());
        assertEquals(true, humanTask.getOutputData().get("approved"));
        assertEquals("test-user", humanTask.getOutputData().get("approver"));
    }

    @Test
    void testHumanTaskWithInputData() throws Exception {
        String workflowName = "human_input_e2e";

        // Create workflow with HUMAN task that has input data
        WorkflowDef workflowDef = createWorkflowWithHumanTaskAndInput(workflowName);
        registerWorkflowDef(workflowDef);

        // Start workflow with input
        Map<String, Object> input = new HashMap<>();
        input.put("requestId", "REQ-12345");
        input.put("amount", 5000);

        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started workflow with HUMAN task and input: {}", workflowId);

        // Wait for HUMAN task to be IN_PROGRESS
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            WorkflowStatusResponse status = getWorkflowStatus(workflowId);
            return status.getTasks().stream()
                    .anyMatch(t -> "approval_task_ref".equals(t.getReferenceTaskName())
                            && "IN_PROGRESS".equals(t.getStatus()));
        });

        // Verify HUMAN task has the input data
        WorkflowStatusResponse statusInProgress = getWorkflowStatus(workflowId);
        WorkflowStatusResponse.TaskInfo humanTaskInProgress = statusInProgress.getTasks().stream()
                .filter(t -> "approval_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(humanTaskInProgress);
        assertEquals("REQ-12345", humanTaskInProgress.getInputData().get("requestId"));
        assertEquals(5000, humanTaskInProgress.getInputData().get("amount"));

        // Complete the HUMAN task
        Map<String, Object> output = new HashMap<>();
        output.put("approved", true);
        output.put("comments", "Looks good");
        completeTaskViaSignal(workflowId, "approval_task_ref", output);

        // Wait for workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());
    }

    @Test
    void testHumanTaskFollowedBySimpleTask() throws Exception {
        String workflowName = "human_then_simple_e2e";
        String simpleTaskName = "after_human_task";

        // Register simple task
        registerTaskDefs(createTaskDefs(List.of(simpleTaskName)));

        // Create workflow with HUMAN -> SIMPLE sequence
        WorkflowDef workflowDef = createWorkflowWithHumanAndSimpleTask(workflowName, simpleTaskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with HUMAN then SIMPLE: {}", workflowId);

        // Wait for HUMAN task to be IN_PROGRESS
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            WorkflowStatusResponse status = getWorkflowStatus(workflowId);
            return status.getTasks().stream()
                    .anyMatch(t -> "human_task_ref".equals(t.getReferenceTaskName())
                            && "IN_PROGRESS".equals(t.getStatus()));
        });

        // Complete the HUMAN task
        completeTaskViaSignal(workflowId, "human_task_ref", Collections.emptyMap());

        // Wait for workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify both tasks completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        assertTrue(completedTaskRefs.contains("human_task_ref"),
                "HUMAN task should be completed");
        assertTrue(completedTaskRefs.contains(simpleTaskName + "_ref"),
                "Simple task after HUMAN should be completed");
    }

    @Test
    void testHumanTaskRejection() throws Exception {
        String workflowName = "human_rejection_e2e";

        // Create workflow with HUMAN task
        WorkflowDef workflowDef = createWorkflowWithHumanTask(workflowName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow for rejection test: {}", workflowId);

        // Wait for HUMAN task to be IN_PROGRESS
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            WorkflowStatusResponse status = getWorkflowStatus(workflowId);
            return status.getTasks().stream()
                    .anyMatch(t -> "human_task_ref".equals(t.getReferenceTaskName())
                            && "IN_PROGRESS".equals(t.getStatus()));
        });

        // Complete the HUMAN task with rejection output
        Map<String, Object> output = new HashMap<>();
        output.put("approved", false);
        output.put("reason", "Budget exceeded");
        completeTaskViaSignal(workflowId, "human_task_ref", output);

        // Wait for workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify task output contains rejection
        WorkflowStatusResponse.TaskInfo humanTask = status.getTasks().stream()
                .filter(t -> "human_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(humanTask);
        assertEquals(false, humanTask.getOutputData().get("approved"));
        assertEquals("Budget exceeded", humanTask.getOutputData().get("reason"));
    }

    @Test
    void testHumanTaskCompletionViaRestApi() throws Exception {
        String workflowName = "human_rest_api_e2e";

        // Create workflow with HUMAN task
        WorkflowDef workflowDef = createWorkflowWithHumanTask(workflowName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow for REST API completion test: {}", workflowId);

        // Wait for HUMAN task to be IN_PROGRESS and get its taskId
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            WorkflowStatusResponse status = getWorkflowStatus(workflowId);
            return status.getTasks().stream()
                    .anyMatch(t -> "human_task_ref".equals(t.getReferenceTaskName())
                            && "IN_PROGRESS".equals(t.getStatus()));
        });

        // Get the task ID from workflow status
        WorkflowStatusResponse statusInProgress = getWorkflowStatus(workflowId);
        WorkflowStatusResponse.TaskInfo humanTaskInProgress = statusInProgress.getTasks().stream()
                .filter(t -> "human_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(humanTaskInProgress);
        String taskId = humanTaskInProgress.getTaskId();
        log.info("HUMAN task ID: {}", taskId);

        // Complete the HUMAN task via REST API (POST /api/tasks)
        Map<String, Object> output = new HashMap<>();
        output.put("approved", true);
        output.put("completedVia", "REST_API");
        completeTaskViaRestApi(workflowId, taskId, output);

        // Wait for workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify HUMAN task completed with output
        WorkflowStatusResponse.TaskInfo humanTask = status.getTasks().stream()
                .filter(t -> "human_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(humanTask);
        assertEquals("COMPLETED", humanTask.getStatus());
        assertEquals(true, humanTask.getOutputData().get("approved"));
        assertEquals("REST_API", humanTask.getOutputData().get("completedVia"));
    }

    // ==================== Helper Methods ====================

    /**
     * Send completeTask signal to workflow via Temporal client.
     */
    private void completeTaskViaSignal(String workflowId, String taskRefName, Map<String, Object> output) {
        WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
        workflowStub.signal("completeTask", taskRefName, output);
        log.info("Sent completeTask signal to workflow {} for task {}", workflowId, taskRefName);
    }

    /**
     * Complete a task via Conductor REST API (POST /api/tasks).
     * This tests the full API path: REST -> Service -> Signal -> Workflow
     */
    private void completeTaskViaRestApi(String workflowId, String taskId, Map<String, Object> output) {
        try {
            Map<String, Object> request = new HashMap<>();
            request.put("taskId", taskId);
            request.put("workflowInstanceId", workflowId);
            request.put("status", "COMPLETED");
            request.put("outputData", output);

            String json = OBJECT_MAPPER.writeValueAsString(request);
            conductorClient.post()
                    .uri("/api/tasks")
                    .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                    .bodyValue(json)
                    .retrieve()
                    .toBodilessEntity()
                    .block(Duration.ofSeconds(10));
            log.info("Completed task via REST API: taskId={}, workflowId={}", taskId, workflowId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to complete task via REST API", e);
        }
    }

    /**
     * Create a workflow with a single HUMAN task.
     */
    private WorkflowDef createWorkflowWithHumanTask(String workflowName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask humanTask = new WorkflowTask();
        humanTask.setName("human_task");
        humanTask.setTaskReferenceName("human_task_ref");
        humanTask.setType(TaskType.HUMAN.name());

        workflowDef.setTasks(Collections.singletonList(humanTask));
        return workflowDef;
    }

    /**
     * Create a workflow with HUMAN task that includes input data from workflow input.
     */
    private WorkflowDef createWorkflowWithHumanTaskAndInput(String workflowName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask humanTask = new WorkflowTask();
        humanTask.setName("approval_task");
        humanTask.setTaskReferenceName("approval_task_ref");
        humanTask.setType(TaskType.HUMAN.name());

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("requestId", "${workflow.input.requestId}");
        inputParams.put("amount", "${workflow.input.amount}");
        humanTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(humanTask));
        return workflowDef;
    }

    /**
     * Create a workflow with HUMAN task followed by a SIMPLE task.
     */
    private WorkflowDef createWorkflowWithHumanAndSimpleTask(String workflowName, String simpleTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // HUMAN task
        WorkflowTask humanTask = new WorkflowTask();
        humanTask.setName("human_task");
        humanTask.setTaskReferenceName("human_task_ref");
        humanTask.setType(TaskType.HUMAN.name());
        tasks.add(humanTask);

        // SIMPLE task after HUMAN
        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName(simpleTaskName);
        simpleTask.setTaskReferenceName(simpleTaskName + "_ref");
        simpleTask.setType(TaskType.SIMPLE.name());
        tasks.add(simpleTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }
}
