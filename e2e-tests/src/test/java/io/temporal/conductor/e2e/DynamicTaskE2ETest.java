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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for DYNAMIC task type against real Temporal server.
 */
class DynamicTaskE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(DynamicTaskE2ETest.class);

    @Test
    void testDynamicTaskFromWorkflowInput() throws Exception {
        String workflowName = "dynamic_task_from_input_e2e";

        // Register multiple potential target tasks
        List<String> potentialTasks = List.of("task_option_a", "task_option_b", "task_option_c");
        registerTaskDefs(createTaskDefs(potentialTasks));

        // Create workflow with DYNAMIC task
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask dynamicTask = new WorkflowTask();
        dynamicTask.setName("choose_task");
        dynamicTask.setTaskReferenceName("choose_task_ref");
        dynamicTask.setType(TaskType.DYNAMIC.name());
        dynamicTask.setDynamicTaskNameParam("selectedTask");

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("selectedTask", "${workflow.input.taskToRun}");
        dynamicTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(dynamicTask));
        registerWorkflowDef(workflowDef);

        // Start workflow with task_option_b selected
        Map<String, Object> input = new HashMap<>();
        input.put("taskToRun", "task_option_b");
        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started dynamic task workflow: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify the DYNAMIC task completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("choose_task_ref"),
                "DYNAMIC task should be completed");
    }

    @Test
    void testDynamicTaskWithDifferentInputs() throws Exception {
        String workflowName = "dynamic_task_multi_option_e2e";

        // Register target tasks
        List<String> targetTasks = List.of("fast_processor", "slow_processor");
        registerTaskDefs(createTaskDefs(targetTasks));

        // Create workflow
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask dynamicTask = new WorkflowTask();
        dynamicTask.setName("process_task");
        dynamicTask.setTaskReferenceName("process_task_ref");
        dynamicTask.setType(TaskType.DYNAMIC.name());
        dynamicTask.setDynamicTaskNameParam("processorName");

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("processorName", "${workflow.input.processor}");
        inputParams.put("data", "${workflow.input.data}");
        dynamicTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(dynamicTask));
        registerWorkflowDef(workflowDef);

        // Test with fast_processor
        Map<String, Object> input1 = new HashMap<>();
        input1.put("processor", "fast_processor");
        input1.put("data", "test-data-1");
        String workflowId1 = startWorkflow(workflowName, 1, input1);
        log.info("Started workflow with fast_processor: {}", workflowId1);

        WorkflowStatusResponse status1 = waitForWorkflowCompletion(workflowId1, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status1.getStatus());

        // Test with slow_processor
        Map<String, Object> input2 = new HashMap<>();
        input2.put("processor", "slow_processor");
        input2.put("data", "test-data-2");
        String workflowId2 = startWorkflow(workflowName, 1, input2);
        log.info("Started workflow with slow_processor: {}", workflowId2);

        WorkflowStatusResponse status2 = waitForWorkflowCompletion(workflowId2, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status2.getStatus());

        log.info("Both dynamic task workflows completed successfully");
    }

    @Test
    void testDynamicTaskInSequence() throws Exception {
        // Test DYNAMIC task in a sequence with other tasks
        String workflowName = "dynamic_task_sequence_e2e";

        // Register tasks
        List<String> tasks = List.of("prepare_task", "dynamic_target_task", "finish_task");
        registerTaskDefs(createTaskDefs(tasks));

        // Create workflow: prepare -> DYNAMIC -> finish
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask prepareTask = new WorkflowTask();
        prepareTask.setName("prepare_task");
        prepareTask.setTaskReferenceName("prepare_task_ref");
        prepareTask.setType(TaskType.SIMPLE.name());

        WorkflowTask dynamicTask = new WorkflowTask();
        dynamicTask.setName("dynamic_step");
        dynamicTask.setTaskReferenceName("dynamic_step_ref");
        dynamicTask.setType(TaskType.DYNAMIC.name());
        dynamicTask.setDynamicTaskNameParam("nextTask");

        Map<String, Object> dynamicInputParams = new HashMap<>();
        dynamicInputParams.put("nextTask", "dynamic_target_task");
        dynamicTask.setInputParameters(dynamicInputParams);

        WorkflowTask finishTask = new WorkflowTask();
        finishTask.setName("finish_task");
        finishTask.setTaskReferenceName("finish_task_ref");
        finishTask.setType(TaskType.SIMPLE.name());

        workflowDef.setTasks(List.of(prepareTask, dynamicTask, finishTask));
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started sequence workflow with DYNAMIC task: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify all tasks completed in sequence
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed tasks: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("prepare_task_ref"), "Prepare task should complete");
        assertTrue(completedTaskRefs.contains("dynamic_step_ref"), "Dynamic task should complete");
        assertTrue(completedTaskRefs.contains("finish_task_ref"), "Finish task should complete");
    }
}
