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
 * E2E tests for FORK_JOIN_DYNAMIC task type against real Temporal server.
 */
class ForkJoinDynamicE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ForkJoinDynamicE2ETest.class);

    @Test
    void testForkJoinDynamicWithForkTaskInputs() throws Exception {
        // Test FORK_JOIN_DYNAMIC using the simple forkTaskInputs approach
        String workflowName = "fork_join_dynamic_simple_e2e";
        String dynamicTaskName = "dynamic_worker";

        // Register the task definition for dynamically created tasks
        registerTaskDefs(createTaskDefs(List.of(dynamicTaskName)));

        // Create workflow definition
        WorkflowDef workflowDef = createForkJoinDynamicWorkflowWithInputs(
                workflowName, dynamicTaskName, 3);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started FORK_JOIN_DYNAMIC workflow: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        if (!"COMPLETED".equals(status.getStatus())) {
            log.error("Workflow {} failed with status: {}, reason: {}",
                    workflowId, status.getStatus(), status.getReasonForIncompletion());
            if (status.getTasks() != null) {
                for (WorkflowStatusResponse.TaskInfo task : status.getTasks()) {
                    log.error("  Task: {} ({}), status: {}",
                            task.getReferenceTaskName(), task.getTaskType(), task.getStatus());
                }
            }
        }
        assertEquals("COMPLETED", status.getStatus());

        // Verify all tasks completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);

        // Should have dynamically created tasks completed
        assertTrue(completedTaskRefs.contains("_" + dynamicTaskName + "_0"),
                "First dynamic task should be completed");
        assertTrue(completedTaskRefs.contains("_" + dynamicTaskName + "_1"),
                "Second dynamic task should be completed");
        assertTrue(completedTaskRefs.contains("_" + dynamicTaskName + "_2"),
                "Third dynamic task should be completed");
        assertTrue(completedTaskRefs.contains("join_ref"),
                "JOIN task should be completed");
    }

    @Test
    void testForkJoinDynamicWithDynamicForkTasksParam() throws Exception {
        // Test FORK_JOIN_DYNAMIC using dynamicForkTasksParam approach
        String workflowName = "fork_join_dynamic_tasks_param_e2e";

        // Register task definitions for the dynamically specified tasks
        registerTaskDefs(createTaskDefs(List.of("compute_a", "compute_b")));

        // Create workflow definition with dynamicForkTasksParam
        WorkflowDef workflowDef = createForkJoinDynamicWorkflowWithTasksParam(workflowName);
        registerWorkflowDef(workflowDef);

        // Start workflow with dynamic task definitions in input
        Map<String, Object> workflowInput = createDynamicForkTasksInput();
        String workflowId = startWorkflow(workflowName, 1, workflowInput);
        log.info("Started FORK_JOIN_DYNAMIC workflow with tasks param: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        if (!"COMPLETED".equals(status.getStatus())) {
            log.error("Workflow {} failed with status: {}, reason: {}",
                    workflowId, status.getStatus(), status.getReasonForIncompletion());
            if (status.getTasks() != null) {
                for (WorkflowStatusResponse.TaskInfo task : status.getTasks()) {
                    log.error("  Task: {} ({}), status: {}",
                            task.getReferenceTaskName(), task.getTaskType(), task.getStatus());
                }
            }
        }
        assertEquals("COMPLETED", status.getStatus());

        // Verify dynamically created tasks completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);

        assertTrue(completedTaskRefs.contains("compute_a_ref"),
                "compute_a task should be completed");
        assertTrue(completedTaskRefs.contains("compute_b_ref"),
                "compute_b task should be completed");
    }

    @Test
    void testForkJoinDynamicParallelExecution() throws Exception {
        // Verify that FORK_JOIN_DYNAMIC schedules tasks in parallel
        String workflowName = "fork_join_dynamic_parallel_e2e";
        String dynamicTaskName = "parallel_worker";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(dynamicTaskName)));

        // Create workflow with 5 parallel branches
        WorkflowDef workflowDef = createForkJoinDynamicWorkflowWithInputs(
                workflowName, dynamicTaskName, 5);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started parallel FORK_JOIN_DYNAMIC workflow: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify parallel scheduling
        int maxConsecutive = getMaxConsecutiveActivityScheduledEvents(workflowId);
        log.info("Max consecutive ActivityTaskScheduled events: {}", maxConsecutive);

        assertTrue(maxConsecutive >= 5,
                "Expected at least 5 consecutive ActivityTaskScheduled events for parallel " +
                        "FORK_JOIN_DYNAMIC branches, but found " + maxConsecutive);
    }

    @Test
    void testForkJoinDynamicWithFinalTask() throws Exception {
        // Test FORK_JOIN_DYNAMIC followed by a task after JOIN
        String workflowName = "fork_join_dynamic_with_final_e2e";
        String dynamicTaskName = "batch_worker";
        String finalTaskName = "aggregator";

        // Register task definitions
        registerTaskDefs(createTaskDefs(List.of(dynamicTaskName, finalTaskName)));

        // Create workflow with final task after JOIN
        WorkflowDef workflowDef = createForkJoinDynamicWithFinalTask(
                workflowName, dynamicTaskName, 3, finalTaskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started FORK_JOIN_DYNAMIC workflow with final task: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify final task completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        assertTrue(completedTaskRefs.contains(finalTaskName + "_ref"),
                "Final task after JOIN should be completed");
    }

    // ==================== Helper Methods ====================

    /**
     * Create a workflow definition with FORK_JOIN_DYNAMIC using forkTaskInputs.
     */
    private WorkflowDef createForkJoinDynamicWorkflowWithInputs(
            String workflowName, String taskName, int numBranches) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // FORK_JOIN_DYNAMIC task
        WorkflowTask dynamicFork = new WorkflowTask();
        dynamicFork.setName("dynamic_fork");
        dynamicFork.setTaskReferenceName("dynamic_fork_ref");
        dynamicFork.setType(TaskType.FORK_JOIN_DYNAMIC.name());

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("forkTaskName", taskName);
        inputParams.put("forkTaskType", TaskType.SIMPLE.name());

        // Create fork inputs for each branch
        List<Map<String, Object>> forkInputs = new ArrayList<>();
        for (int i = 0; i < numBranches; i++) {
            Map<String, Object> taskInput = new HashMap<>();
            taskInput.put("index", i);
            taskInput.put("data", "item-" + i);
            forkInputs.add(taskInput);
        }
        inputParams.put("forkTaskInputs", forkInputs);
        dynamicFork.setInputParameters(inputParams);

        tasks.add(dynamicFork);

        // JOIN task
        WorkflowTask joinTask = new WorkflowTask();
        joinTask.setName("join");
        joinTask.setTaskReferenceName("join_ref");
        joinTask.setType(TaskType.JOIN.name());
        tasks.add(joinTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a workflow definition with FORK_JOIN_DYNAMIC using dynamicForkTasksParam.
     */
    private WorkflowDef createForkJoinDynamicWorkflowWithTasksParam(String workflowName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // FORK_JOIN_DYNAMIC task using dynamicForkTasksParam
        WorkflowTask dynamicFork = new WorkflowTask();
        dynamicFork.setName("dynamic_fork");
        dynamicFork.setTaskReferenceName("dynamic_fork_ref");
        dynamicFork.setType(TaskType.FORK_JOIN_DYNAMIC.name());
        dynamicFork.setDynamicForkTasksParam("dynamicTasks");
        dynamicFork.setDynamicForkTasksInputParamName("dynamicTasksInput");

        // Pass through input parameters from workflow input
        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("dynamicTasks", "${workflow.input.dynamicTasks}");
        inputParams.put("dynamicTasksInput", "${workflow.input.dynamicTasksInput}");
        dynamicFork.setInputParameters(inputParams);

        tasks.add(dynamicFork);

        // JOIN task
        WorkflowTask joinTask = new WorkflowTask();
        joinTask.setName("join");
        joinTask.setTaskReferenceName("join_ref");
        joinTask.setType(TaskType.JOIN.name());
        tasks.add(joinTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create workflow input with dynamic task definitions for dynamicForkTasksParam approach.
     */
    private Map<String, Object> createDynamicForkTasksInput() {
        // Define the dynamic tasks
        List<Map<String, Object>> dynamicTasks = new ArrayList<>();

        Map<String, Object> task1 = new HashMap<>();
        task1.put("name", "compute_a");
        task1.put("taskReferenceName", "compute_a_ref");
        task1.put("type", TaskType.SIMPLE.name());
        dynamicTasks.add(task1);

        Map<String, Object> task2 = new HashMap<>();
        task2.put("name", "compute_b");
        task2.put("taskReferenceName", "compute_b_ref");
        task2.put("type", TaskType.SIMPLE.name());
        dynamicTasks.add(task2);

        // Define inputs for each task
        Map<String, Map<String, Object>> dynamicTasksInput = new HashMap<>();
        Map<String, Object> input1 = new HashMap<>();
        input1.put("value", 100);
        dynamicTasksInput.put("compute_a_ref", input1);

        Map<String, Object> input2 = new HashMap<>();
        input2.put("value", 200);
        dynamicTasksInput.put("compute_b_ref", input2);

        Map<String, Object> workflowInput = new HashMap<>();
        workflowInput.put("dynamicTasks", dynamicTasks);
        workflowInput.put("dynamicTasksInput", dynamicTasksInput);
        return workflowInput;
    }

    /**
     * Create a workflow definition with FORK_JOIN_DYNAMIC and a final task after JOIN.
     */
    private WorkflowDef createForkJoinDynamicWithFinalTask(
            String workflowName, String taskName, int numBranches, String finalTaskName) {
        WorkflowDef workflowDef = createForkJoinDynamicWorkflowWithInputs(
                workflowName, taskName, numBranches);

        // Add final task after JOIN
        WorkflowTask finalTask = new WorkflowTask();
        finalTask.setName(finalTaskName);
        finalTask.setTaskReferenceName(finalTaskName + "_ref");
        finalTask.setType(TaskType.SIMPLE.name());
        workflowDef.getTasks().add(finalTask);

        return workflowDef;
    }
}
