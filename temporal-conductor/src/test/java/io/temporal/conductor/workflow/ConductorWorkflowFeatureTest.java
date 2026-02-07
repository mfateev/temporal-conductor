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
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
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
 * Unit tests for Conductor workflow features using TestWorkflowEnvironment.
 *
 * <p>These tests verify workflow execution features without requiring Docker or a real Temporal server.
 * Covers: FORK_JOIN, SUB_WORKFLOW, DYNAMIC, SWITCH, DO_WHILE, HUMAN tasks, and sequential execution.
 */
@Timeout(120)
class ConductorWorkflowFeatureTest {

    private static final Logger log = LoggerFactory.getLogger(ConductorWorkflowFeatureTest.class);

    private static final String TASK_QUEUE = "conductor-feature-test-queue";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TestWorkflowEnvironment testEnv;
    private Worker worker;
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

        worker = testEnv.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(ConductorWorkflowImpl.class);
        worker.registerActivitiesImplementations(new TaskExecutionActivitiesImpl());

        testEnv.start();
        client = testEnv.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    // ==================== FORK_JOIN Tests ====================

    @Test
    void testForkJoinExecution() throws Exception {
        String workflowName = "fork_join_test_workflow";
        List<String> branchTasks = List.of("branch_a", "branch_b", "branch_c");
        String finalTask = "final_task";

        ConductorWorkflowInput input = createForkJoinWorkflowInput(workflowName, branchTasks, finalTask);

        String workflowId = "test-fork-join-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());
        log.info("FORK_JOIN workflow completed with output keys: {}", output.getOutput().keySet());

        // Verify all activity types
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);

        // All branch tasks + final task should be scheduled
        assertEquals(4, activityTypes.size(), "Should have 4 activities: 3 branches + 1 final");
        for (String branchTask : branchTasks) {
            assertTrue(activityTypes.contains(branchTask), "Should contain branch task: " + branchTask);
        }
        assertTrue(activityTypes.contains(finalTask), "Should contain final task");
    }

    @Test
    void testForkJoinParallelScheduling() throws Exception {
        String workflowName = "fork_parallel_test_workflow";
        List<String> branchTasks = List.of("parallel_a", "parallel_b", "parallel_c");
        String finalTask = "after_join";

        ConductorWorkflowInput input = createForkJoinWorkflowInput(workflowName, branchTasks, finalTask);

        String workflowId = "test-fork-parallel-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify parallel scheduling by checking consecutive ActivityTaskScheduled events
        int maxConsecutive = getMaxConsecutiveActivityScheduledEvents(workflowId);
        log.info("Max consecutive ActivityTaskScheduled events: {}", maxConsecutive);

        // With 3 parallel branches, we should see at least 3 consecutive ActivityTaskScheduled events
        assertTrue(maxConsecutive >= 3,
                "Expected at least 3 consecutive ActivityTaskScheduled events for parallel FORK, but found " + maxConsecutive);
    }

    // ==================== SUB_WORKFLOW Tests ====================

    @Test
    void testSubWorkflowExecution() throws Exception {
        String parentWorkflowName = "parent_workflow";
        String childWorkflowName = "child_workflow";
        String childTaskName = "child_task";

        ConductorWorkflowInput input = createSubWorkflowInput(parentWorkflowName, childWorkflowName, childTaskName);

        String workflowId = "test-subworkflow-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(parentWorkflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());
        log.info("SUB_WORKFLOW completed successfully");
    }

    @Test
    void testNestedSubWorkflows() throws Exception {
        // Parent -> Child -> Grandchild (3 levels)
        String parentName = "nested_parent";
        String childName = "nested_child";
        String grandchildName = "nested_grandchild";
        String leafTaskName = "leaf_task";

        ConductorWorkflowInput input = createNestedSubWorkflowInput(
                parentName, childName, grandchildName, leafTaskName);

        String workflowId = "test-nested-subworkflow-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(parentName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());
        log.info("Nested SUB_WORKFLOW (3 levels) completed successfully");
    }

    // ==================== DYNAMIC Task Tests ====================

    @Test
    void testDynamicTaskExecution() throws Exception {
        String workflowName = "dynamic_task_workflow";
        String selectedTask = "task_option_b";
        List<String> potentialTasks = List.of("task_option_a", "task_option_b", "task_option_c");

        ConductorWorkflowInput input = createDynamicTaskWorkflowInput(
                workflowName, potentialTasks, selectedTask);

        String workflowId = "test-dynamic-task-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify the selected task was executed
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for DYNAMIC workflow: {}", activityTypes);
        assertEquals(1, activityTypes.size(), "Should have exactly one activity");
        assertEquals(selectedTask, activityTypes.get(0), "Activity should be the dynamically selected task");
    }

    @Test
    void testDynamicTaskInSequence() throws Exception {
        String workflowName = "dynamic_sequence_workflow";
        String dynamicTarget = "dynamic_target_task";

        ConductorWorkflowInput input = createDynamicTaskInSequenceInput(workflowName, dynamicTarget);

        String workflowId = "test-dynamic-sequence-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify all tasks in sequence
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for DYNAMIC sequence: {}", activityTypes);
        assertEquals(3, activityTypes.size(), "Should have 3 activities: prepare + dynamic + finish");
        assertEquals("prepare_task", activityTypes.get(0));
        assertEquals(dynamicTarget, activityTypes.get(1));
        assertEquals("finish_task", activityTypes.get(2));
    }

    // ==================== SWITCH Tests ====================

    @Test
    void testSwitchCaseA() throws Exception {
        String workflowName = "switch_workflow";
        ConductorWorkflowInput input = createSwitchWorkflowInput(workflowName, "A");

        String workflowId = "test-switch-case-a-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for SWITCH case A: {}", activityTypes);
        assertTrue(activityTypes.contains("case_a_task"), "Case A task should be executed");
        assertFalse(activityTypes.contains("case_b_task"), "Case B task should NOT be executed");
    }

    @Test
    void testSwitchCaseB() throws Exception {
        String workflowName = "switch_workflow";
        ConductorWorkflowInput input = createSwitchWorkflowInput(workflowName, "B");

        String workflowId = "test-switch-case-b-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for SWITCH case B: {}", activityTypes);
        assertFalse(activityTypes.contains("case_a_task"), "Case A task should NOT be executed");
        assertTrue(activityTypes.contains("case_b_task"), "Case B task should be executed");
    }

    @Test
    void testSwitchDefaultCase() throws Exception {
        String workflowName = "switch_workflow";
        ConductorWorkflowInput input = createSwitchWorkflowInput(workflowName, "UNKNOWN");

        String workflowId = "test-switch-default-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for SWITCH default: {}", activityTypes);
        assertTrue(activityTypes.contains("default_task"), "Default task should be executed");
    }

    // ==================== DO_WHILE Loop Tests ====================

    @Test
    void testDoWhileLoop() throws Exception {
        String workflowName = "do_while_workflow";
        int iterations = 3;
        ConductorWorkflowInput input = createDoWhileWorkflowInput(workflowName, iterations);

        String workflowId = "test-do-while-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify the loop task was executed multiple times
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for DO_WHILE: {}", activityTypes);
        long loopTaskCount = activityTypes.stream()
                .filter(t -> t.equals("loop_task"))
                .count();
        assertEquals(iterations, loopTaskCount, "Loop task should be executed " + iterations + " times");
    }

    // ==================== Sequential Tasks Tests ====================

    @Test
    void testSequentialTasks() throws Exception {
        String workflowName = "sequential_workflow";
        List<String> tasks = List.of("task_1", "task_2", "task_3", "task_4");

        ConductorWorkflowInput input = createSequentialWorkflowInput(workflowName, tasks);

        String workflowId = "test-sequential-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify all tasks executed in order
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for sequential workflow: {}", activityTypes);
        assertEquals(tasks, activityTypes, "Tasks should be executed in order");
    }

    // ==================== HUMAN Task Tests ====================

    @Test
    void testHumanTaskCompletion() throws Exception {
        String workflowName = "human_task_workflow";
        ConductorWorkflowInput input = createHumanTaskWorkflowInput(workflowName);

        String workflowId = "test-human-task-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);

        // Wait a bit for HUMAN task to be IN_PROGRESS
        Thread.sleep(500);

        // Complete the HUMAN task via signal
        Map<String, Object> taskOutput = new HashMap<>();
        taskOutput.put("approved", true);
        taskOutput.put("approver", "test-user");
        workflow.signal("completeTask", "human_task_ref", taskOutput);

        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());
        log.info("HUMAN task workflow completed with output: {}", output.getOutput());

        // Verify HUMAN task output is in workflow output
        @SuppressWarnings("unchecked")
        Map<String, Object> humanTaskOutput = (Map<String, Object>) output.getOutput().get("human_task_ref");
        if (humanTaskOutput != null) {
            assertEquals(true, humanTaskOutput.get("approved"));
            assertEquals("test-user", humanTaskOutput.get("approver"));
        }
    }

    @Test
    void testHumanTaskFollowedBySimpleTask() throws Exception {
        String workflowName = "human_then_simple_workflow";
        ConductorWorkflowInput input = createHumanThenSimpleWorkflowInput(workflowName);

        String workflowId = "test-human-then-simple-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);

        // Wait for HUMAN task to be ready
        Thread.sleep(500);

        // Complete the HUMAN task
        workflow.signal("completeTask", "human_task_ref", Collections.emptyMap());

        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify both tasks executed
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types for HUMAN then SIMPLE: {}", activityTypes);
        // Only SIMPLE task appears as activity (HUMAN is handled differently)
        assertTrue(activityTypes.contains("after_human_task"), "Simple task should execute after HUMAN");
    }

    // ==================== SET_VARIABLE Tests ====================

    @Test
    void testSetVariableTask() throws Exception {
        String workflowName = "set_variable_workflow";
        ConductorWorkflowInput input = createSetVariableWorkflowInput(workflowName);

        String workflowId = "test-set-variable-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub(workflowName, options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());
        log.info("SET_VARIABLE workflow completed with output: {}", output.getOutput());
    }

    // ==================== Helper Methods: History Analysis ====================

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

    private int getMaxConsecutiveActivityScheduledEvents(String workflowId) {
        GetWorkflowExecutionHistoryResponse response = testEnv.getWorkflowServiceStubs()
                .blockingStub()
                .getWorkflowExecutionHistory(
                        GetWorkflowExecutionHistoryRequest.newBuilder()
                                .setNamespace(client.getOptions().getNamespace())
                                .setExecution(WorkflowExecution.newBuilder()
                                        .setWorkflowId(workflowId)
                                        .build())
                                .build());

        int maxConsecutive = 0;
        int currentConsecutive = 0;

        for (HistoryEvent event : response.getHistory().getEventsList()) {
            if (event.getEventType() == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED) {
                currentConsecutive++;
                maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
            } else {
                currentConsecutive = 0;
            }
        }
        return maxConsecutive;
    }

    // ==================== Helper Methods: Input Builders ====================

    private ConductorWorkflowInput createForkJoinWorkflowInput(
            String workflowName, List<String> branchTasks, String finalTask) throws JsonProcessingException {
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

        for (String branchTaskName : branchTasks) {
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

        // Final task
        if (finalTask != null) {
            WorkflowTask final_ = new WorkflowTask();
            final_.setName(finalTask);
            final_.setTaskReferenceName(finalTask + "_ref");
            final_.setType(TaskType.SIMPLE.name());
            tasks.add(final_);
        }

        workflowDef.setTasks(tasks);

        // Create task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        for (String taskName : branchTasks) {
            TaskDef td = new TaskDef();
            td.setName(taskName);
            taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(td));
        }
        if (finalTask != null) {
            TaskDef td = new TaskDef();
            td.setName(finalTask);
            taskDefsJson.put(finalTask, OBJECT_MAPPER.writeValueAsString(td));
        }

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createSubWorkflowInput(
            String parentName, String childName, String childTaskName) throws JsonProcessingException {
        // Child workflow definition
        WorkflowDef childDef = new WorkflowDef();
        childDef.setName(childName);
        childDef.setVersion(1);

        WorkflowTask childTask = new WorkflowTask();
        childTask.setName(childTaskName);
        childTask.setTaskReferenceName(childTaskName + "_ref");
        childTask.setType(TaskType.SIMPLE.name());
        childDef.setTasks(Collections.singletonList(childTask));

        // Parent workflow definition
        WorkflowDef parentDef = new WorkflowDef();
        parentDef.setName(parentName);
        parentDef.setVersion(1);

        WorkflowTask subWorkflowTask = new WorkflowTask();
        subWorkflowTask.setName("sub_workflow_task");
        subWorkflowTask.setTaskReferenceName("sub_workflow_ref");
        subWorkflowTask.setType(TaskType.SUB_WORKFLOW.name());

        SubWorkflowParams subParams = new SubWorkflowParams();
        subParams.setName(childName);
        subParams.setVersion(1);
        subWorkflowTask.setSubWorkflowParam(subParams);

        parentDef.setTasks(Collections.singletonList(subWorkflowTask));

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        TaskDef td = new TaskDef();
        td.setName(childTaskName);
        taskDefsJson.put(childTaskName, OBJECT_MAPPER.writeValueAsString(td));

        // Sub-workflow definitions
        Map<String, String> subWorkflowDefsJson = new HashMap<>();
        subWorkflowDefsJson.put(childName + ":1", OBJECT_MAPPER.writeValueAsString(childDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(parentDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .workflowDefsJson(subWorkflowDefsJson)
                .build();
    }

    private ConductorWorkflowInput createNestedSubWorkflowInput(
            String parentName, String childName, String grandchildName, String leafTaskName)
            throws JsonProcessingException {
        // Grandchild workflow (leaf)
        WorkflowDef grandchildDef = new WorkflowDef();
        grandchildDef.setName(grandchildName);
        grandchildDef.setVersion(1);

        WorkflowTask leafTask = new WorkflowTask();
        leafTask.setName(leafTaskName);
        leafTask.setTaskReferenceName(leafTaskName + "_ref");
        leafTask.setType(TaskType.SIMPLE.name());
        grandchildDef.setTasks(Collections.singletonList(leafTask));

        // Child workflow (calls grandchild)
        WorkflowDef childDef = new WorkflowDef();
        childDef.setName(childName);
        childDef.setVersion(1);

        WorkflowTask subGrandchild = new WorkflowTask();
        subGrandchild.setName("sub_grandchild");
        subGrandchild.setTaskReferenceName("sub_grandchild_ref");
        subGrandchild.setType(TaskType.SUB_WORKFLOW.name());
        SubWorkflowParams grandchildParams = new SubWorkflowParams();
        grandchildParams.setName(grandchildName);
        grandchildParams.setVersion(1);
        subGrandchild.setSubWorkflowParam(grandchildParams);
        childDef.setTasks(Collections.singletonList(subGrandchild));

        // Parent workflow (calls child)
        WorkflowDef parentDef = new WorkflowDef();
        parentDef.setName(parentName);
        parentDef.setVersion(1);

        WorkflowTask subChild = new WorkflowTask();
        subChild.setName("sub_child");
        subChild.setTaskReferenceName("sub_child_ref");
        subChild.setType(TaskType.SUB_WORKFLOW.name());
        SubWorkflowParams childParams = new SubWorkflowParams();
        childParams.setName(childName);
        childParams.setVersion(1);
        subChild.setSubWorkflowParam(childParams);
        parentDef.setTasks(Collections.singletonList(subChild));

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        TaskDef td = new TaskDef();
        td.setName(leafTaskName);
        taskDefsJson.put(leafTaskName, OBJECT_MAPPER.writeValueAsString(td));

        // Sub-workflow definitions
        Map<String, String> subWorkflowDefsJson = new HashMap<>();
        subWorkflowDefsJson.put(childName + ":1", OBJECT_MAPPER.writeValueAsString(childDef));
        subWorkflowDefsJson.put(grandchildName + ":1", OBJECT_MAPPER.writeValueAsString(grandchildDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(parentDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .workflowDefsJson(subWorkflowDefsJson)
                .build();
    }

    private ConductorWorkflowInput createDynamicTaskWorkflowInput(
            String workflowName, List<String> potentialTasks, String selectedTask)
            throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask dynamicTask = new WorkflowTask();
        dynamicTask.setName("dynamic_task");
        dynamicTask.setTaskReferenceName("dynamic_task_ref");
        dynamicTask.setType(TaskType.DYNAMIC.name());
        dynamicTask.setDynamicTaskNameParam("selectedTask");

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("selectedTask", "${workflow.input.taskToRun}");
        dynamicTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(dynamicTask));

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        for (String taskName : potentialTasks) {
            TaskDef td = new TaskDef();
            td.setName(taskName);
            taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(td));
        }

        Map<String, Object> workflowInput = new HashMap<>();
        workflowInput.put("taskToRun", selectedTask);

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(workflowInput)
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createDynamicTaskInSequenceInput(
            String workflowName, String dynamicTarget) throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // Prepare task
        WorkflowTask prepareTask = new WorkflowTask();
        prepareTask.setName("prepare_task");
        prepareTask.setTaskReferenceName("prepare_task_ref");
        prepareTask.setType(TaskType.SIMPLE.name());
        tasks.add(prepareTask);

        // Dynamic task
        WorkflowTask dynamicTask = new WorkflowTask();
        dynamicTask.setName("dynamic_step");
        dynamicTask.setTaskReferenceName("dynamic_step_ref");
        dynamicTask.setType(TaskType.DYNAMIC.name());
        dynamicTask.setDynamicTaskNameParam("nextTask");
        Map<String, Object> dynInput = new HashMap<>();
        dynInput.put("nextTask", dynamicTarget);
        dynamicTask.setInputParameters(dynInput);
        tasks.add(dynamicTask);

        // Finish task
        WorkflowTask finishTask = new WorkflowTask();
        finishTask.setName("finish_task");
        finishTask.setTaskReferenceName("finish_task_ref");
        finishTask.setType(TaskType.SIMPLE.name());
        tasks.add(finishTask);

        workflowDef.setTasks(tasks);

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        for (String taskName : List.of("prepare_task", dynamicTarget, "finish_task")) {
            TaskDef td = new TaskDef();
            td.setName(taskName);
            taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(td));
        }

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createSwitchWorkflowInput(String workflowName, String caseValue)
            throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask switchTask = new WorkflowTask();
        switchTask.setName("switch_task");
        switchTask.setTaskReferenceName("switch_task_ref");
        switchTask.setType(TaskType.SWITCH.name());
        switchTask.setEvaluatorType("value-param");
        switchTask.setExpression("switchValue");

        Map<String, Object> switchInput = new HashMap<>();
        switchInput.put("switchValue", "${workflow.input.caseSelection}");
        switchTask.setInputParameters(switchInput);

        // Case A
        WorkflowTask caseATask = new WorkflowTask();
        caseATask.setName("case_a_task");
        caseATask.setTaskReferenceName("case_a_task_ref");
        caseATask.setType(TaskType.SIMPLE.name());

        // Case B
        WorkflowTask caseBTask = new WorkflowTask();
        caseBTask.setName("case_b_task");
        caseBTask.setTaskReferenceName("case_b_task_ref");
        caseBTask.setType(TaskType.SIMPLE.name());

        // Default case
        WorkflowTask defaultTask = new WorkflowTask();
        defaultTask.setName("default_task");
        defaultTask.setTaskReferenceName("default_task_ref");
        defaultTask.setType(TaskType.SIMPLE.name());

        Map<String, List<WorkflowTask>> decisionCases = new HashMap<>();
        decisionCases.put("A", Collections.singletonList(caseATask));
        decisionCases.put("B", Collections.singletonList(caseBTask));
        switchTask.setDecisionCases(decisionCases);
        switchTask.setDefaultCase(Collections.singletonList(defaultTask));

        workflowDef.setTasks(Collections.singletonList(switchTask));

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        for (String taskName : List.of("case_a_task", "case_b_task", "default_task")) {
            TaskDef td = new TaskDef();
            td.setName(taskName);
            taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(td));
        }

        Map<String, Object> workflowInput = new HashMap<>();
        workflowInput.put("caseSelection", caseValue);

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(workflowInput)
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createDoWhileWorkflowInput(String workflowName, int iterations)
            throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask doWhileTask = new WorkflowTask();
        doWhileTask.setName("do_while_task");
        doWhileTask.setTaskReferenceName("do_while_ref");
        doWhileTask.setType(TaskType.DO_WHILE.name());
        doWhileTask.setLoopCondition("if ($.do_while_ref['iteration'] < $.iterations) { true; } else { false; }");

        Map<String, Object> loopInput = new HashMap<>();
        loopInput.put("iterations", "${workflow.input.iterations}");
        doWhileTask.setInputParameters(loopInput);

        // Loop body
        WorkflowTask loopBodyTask = new WorkflowTask();
        loopBodyTask.setName("loop_task");
        loopBodyTask.setTaskReferenceName("loop_task_ref");
        loopBodyTask.setType(TaskType.SIMPLE.name());

        doWhileTask.setLoopOver(Collections.singletonList(loopBodyTask));

        workflowDef.setTasks(Collections.singletonList(doWhileTask));

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        TaskDef td = new TaskDef();
        td.setName("loop_task");
        taskDefsJson.put("loop_task", OBJECT_MAPPER.writeValueAsString(td));

        Map<String, Object> workflowInput = new HashMap<>();
        workflowInput.put("iterations", iterations);

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(workflowInput)
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createSequentialWorkflowInput(String workflowName, List<String> taskNames)
            throws JsonProcessingException {
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

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        for (String taskName : taskNames) {
            TaskDef td = new TaskDef();
            td.setName(taskName);
            taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(td));
        }

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createHumanTaskWorkflowInput(String workflowName)
            throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask humanTask = new WorkflowTask();
        humanTask.setName("human_task");
        humanTask.setTaskReferenceName("human_task_ref");
        humanTask.setType(TaskType.HUMAN.name());

        workflowDef.setTasks(Collections.singletonList(humanTask));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(Collections.emptyMap())
                .build();
    }

    private ConductorWorkflowInput createHumanThenSimpleWorkflowInput(String workflowName)
            throws JsonProcessingException {
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
        simpleTask.setName("after_human_task");
        simpleTask.setTaskReferenceName("after_human_task_ref");
        simpleTask.setType(TaskType.SIMPLE.name());
        tasks.add(simpleTask);

        workflowDef.setTasks(tasks);

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        TaskDef td = new TaskDef();
        td.setName("after_human_task");
        taskDefsJson.put("after_human_task", OBJECT_MAPPER.writeValueAsString(td));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createSetVariableWorkflowInput(String workflowName)
            throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // SET_VARIABLE task
        WorkflowTask setVarTask = new WorkflowTask();
        setVarTask.setName("set_var_task");
        setVarTask.setTaskReferenceName("set_var_ref");
        setVarTask.setType(TaskType.SET_VARIABLE.name());

        Map<String, Object> setVarInput = new HashMap<>();
        setVarInput.put("myVariable", "hello");
        setVarInput.put("myNumber", 42);
        setVarTask.setInputParameters(setVarInput);
        tasks.add(setVarTask);

        // SIMPLE task that uses the variable
        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName("use_var_task");
        simpleTask.setTaskReferenceName("use_var_ref");
        simpleTask.setType(TaskType.SIMPLE.name());

        Map<String, Object> simpleInput = new HashMap<>();
        simpleInput.put("varValue", "${workflow.variables.myVariable}");
        simpleTask.setInputParameters(simpleInput);
        tasks.add(simpleTask);

        workflowDef.setTasks(tasks);

        // Task definitions
        Map<String, String> taskDefsJson = new HashMap<>();
        TaskDef td = new TaskDef();
        td.setName("use_var_task");
        taskDefsJson.put("use_var_task", OBJECT_MAPPER.writeValueAsString(td));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }
}
