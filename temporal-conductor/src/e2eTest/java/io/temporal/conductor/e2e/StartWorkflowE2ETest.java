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

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for START_WORKFLOW task type against real Temporal server.
 * START_WORKFLOW is fire-and-forget - it starts a child workflow but doesn't wait for completion.
 */
class StartWorkflowE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(StartWorkflowE2ETest.class);

    @Test
    void testSimpleStartWorkflow() throws Exception {
        String parentWorkflowName = "parent_start_workflow_e2e";
        String childWorkflowName = "child_start_workflow_e2e";
        String childTaskName = "child_start_task";
        String finalTaskName = "parent_final_task";

        // Register child workflow's task
        registerTaskDefs(createTaskDefs(List.of(childTaskName, finalTaskName)));

        // Register child workflow
        WorkflowDef childWorkflowDef = createSimpleWorkflowDef(childWorkflowName, childTaskName);
        registerWorkflowDef(childWorkflowDef);

        // Register parent workflow with START_WORKFLOW task
        WorkflowDef parentWorkflowDef = createParentWorkflowWithStartWorkflow(
                parentWorkflowName, childWorkflowName, 1, finalTaskName);
        registerWorkflowDef(parentWorkflowDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentWorkflowName, 1, Collections.emptyMap());
        log.info("Started parent workflow: {}", workflowId);

        // Wait for parent workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify START_WORKFLOW task completed immediately (fire-and-forget)
        assertNotNull(status.getTasks());
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("start_workflow_ref"),
                "START_WORKFLOW task should be completed");
        assertTrue(completedTaskRefs.contains(finalTaskName + "_ref"),
                "Final task after START_WORKFLOW should be completed");

        // Verify START_WORKFLOW task output contains child workflow ID
        WorkflowStatusResponse.TaskInfo startWorkflowTask = status.getTasks().stream()
                .filter(t -> "start_workflow_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(startWorkflowTask);
        assertNotNull(startWorkflowTask.getOutputData());

        // The output should contain workflowId of the child workflow
        Object childWorkflowId = startWorkflowTask.getOutputData().get("workflowId");
        assertNotNull(childWorkflowId, "START_WORKFLOW output should contain workflowId");
        log.info("Child workflow ID from START_WORKFLOW output: {}", childWorkflowId);
    }

    @Test
    void testStartWorkflowFireAndForget() throws Exception {
        // Test that parent workflow completes independently of child workflow
        String parentWorkflowName = "parent_fire_forget_e2e";
        String childWorkflowName = "child_fire_forget_e2e";
        String childTaskName = "slow_child_task";
        String finalTaskName = "parent_final";

        // Register tasks
        registerTaskDefs(createTaskDefs(List.of(childTaskName, finalTaskName)));

        // Register child workflow (it will run independently)
        WorkflowDef childWorkflowDef = createSimpleWorkflowDef(childWorkflowName, childTaskName);
        registerWorkflowDef(childWorkflowDef);

        // Register parent workflow
        WorkflowDef parentWorkflowDef = createParentWorkflowWithStartWorkflow(
                parentWorkflowName, childWorkflowName, 1, finalTaskName);
        registerWorkflowDef(parentWorkflowDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentWorkflowName, 1, Collections.emptyMap());
        log.info("Started parent workflow: {}", workflowId);

        // Parent workflow should complete quickly (doesn't wait for child)
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());

        // Get child workflow ID from START_WORKFLOW task output
        WorkflowStatusResponse.TaskInfo startWorkflowTask = status.getTasks().stream()
                .filter(t -> "start_workflow_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(startWorkflowTask);
        String childWorkflowId = (String) startWorkflowTask.getOutputData().get("workflowId");
        assertNotNull(childWorkflowId);

        log.info("Parent completed, child workflow {} running independently", childWorkflowId);

        // Wait for child workflow to complete independently
        WorkflowStatusResponse childStatus = waitForWorkflowCompletion(childWorkflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", childStatus.getStatus());
        log.info("Child workflow {} completed independently", childWorkflowId);
    }

    @Test
    void testMultipleStartWorkflowTasks() throws Exception {
        // Test multiple START_WORKFLOW tasks in sequence
        String parentWorkflowName = "parent_multi_start_e2e";
        String childWorkflowName = "child_multi_start_e2e";
        String childTaskName = "child_multi_task";

        // Register tasks
        registerTaskDefs(createTaskDefs(List.of(childTaskName)));

        // Register child workflow
        WorkflowDef childWorkflowDef = createSimpleWorkflowDef(childWorkflowName, childTaskName);
        registerWorkflowDef(childWorkflowDef);

        // Register parent workflow with multiple START_WORKFLOW tasks
        WorkflowDef parentWorkflowDef = createWorkflowWithMultipleStartWorkflows(
                parentWorkflowName, childWorkflowName, 3);
        registerWorkflowDef(parentWorkflowDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentWorkflowName, 1, Collections.emptyMap());
        log.info("Started parent workflow with multiple START_WORKFLOW tasks: {}", workflowId);

        // Wait for parent completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify all START_WORKFLOW tasks completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("start_workflow_ref_0"),
                "First START_WORKFLOW task should be completed");
        assertTrue(completedTaskRefs.contains("start_workflow_ref_1"),
                "Second START_WORKFLOW task should be completed");
        assertTrue(completedTaskRefs.contains("start_workflow_ref_2"),
                "Third START_WORKFLOW task should be completed");

        // Verify each START_WORKFLOW task has unique child workflow ID
        Set<String> childWorkflowIds = status.getTasks().stream()
                .filter(t -> t.getReferenceTaskName().startsWith("start_workflow_ref_"))
                .map(t -> (String) t.getOutputData().get("workflowId"))
                .collect(Collectors.toSet());

        assertEquals(3, childWorkflowIds.size(), "Should have 3 unique child workflow IDs");
        log.info("Started 3 independent child workflows: {}", childWorkflowIds);
    }

    // ==================== Helper Methods ====================

    /**
     * Create a parent workflow with a START_WORKFLOW task followed by a final task.
     */
    private WorkflowDef createParentWorkflowWithStartWorkflow(
            String parentName, String childWorkflowName, int childVersion, String finalTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(parentName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // START_WORKFLOW task
        WorkflowTask startWorkflowTask = new WorkflowTask();
        startWorkflowTask.setName("start_workflow_task");
        startWorkflowTask.setTaskReferenceName("start_workflow_ref");
        startWorkflowTask.setType(TaskType.START_WORKFLOW.name());

        Map<String, Object> inputParams = new HashMap<>();
        Map<String, Object> startWorkflowConfig = new HashMap<>();
        startWorkflowConfig.put("name", childWorkflowName);
        startWorkflowConfig.put("version", childVersion);
        startWorkflowConfig.put("input", Collections.emptyMap());
        inputParams.put("startWorkflow", startWorkflowConfig);
        startWorkflowTask.setInputParameters(inputParams);

        tasks.add(startWorkflowTask);

        // Final task after START_WORKFLOW
        WorkflowTask finalTask = new WorkflowTask();
        finalTask.setName(finalTaskName);
        finalTask.setTaskReferenceName(finalTaskName + "_ref");
        finalTask.setType(TaskType.SIMPLE.name());
        tasks.add(finalTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a workflow with multiple START_WORKFLOW tasks in sequence.
     */
    private WorkflowDef createWorkflowWithMultipleStartWorkflows(
            String parentName, String childWorkflowName, int numStartWorkflows) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(parentName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        for (int i = 0; i < numStartWorkflows; i++) {
            WorkflowTask startWorkflowTask = new WorkflowTask();
            startWorkflowTask.setName("start_workflow_task_" + i);
            startWorkflowTask.setTaskReferenceName("start_workflow_ref_" + i);
            startWorkflowTask.setType(TaskType.START_WORKFLOW.name());

            Map<String, Object> inputParams = new HashMap<>();
            Map<String, Object> startWorkflowConfig = new HashMap<>();
            startWorkflowConfig.put("name", childWorkflowName);
            startWorkflowConfig.put("version", 1);
            Map<String, Object> childInput = new HashMap<>();
            childInput.put("index", i);
            startWorkflowConfig.put("input", childInput);
            inputParams.put("startWorkflow", startWorkflowConfig);
            startWorkflowTask.setInputParameters(inputParams);

            tasks.add(startWorkflowTask);
        }

        workflowDef.setTasks(tasks);
        return workflowDef;
    }
}
