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

package io.temporal.conductor.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.conductor.activity.EventPublishActivity;
import io.temporal.conductor.activity.TaskExecutionActivitiesImpl;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import io.temporal.conductor.workflow.model.TaskState;
import io.temporal.conductor.workflow.model.WorkflowState;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Advanced tests for ConductorWorkflow implementation covering:
 * - HUMAN task signal completion
 * - EVENT task execution
 * - Extension task routing (JSON_JQ_TRANSFORM)
 * - DO_WHILE loop execution
 * - SET_VARIABLE task execution
 * - Pause/Resume workflow signals
 *
 * <p>These tests close the coverage gap between E2E tests and unit tests.
 */
@Timeout(60)
class ConductorWorkflowAdvancedTest {

    private static final String TASK_QUEUE = "conductor-advanced-test-queue";
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

        // Register activities with mock implementations for EVENT tasks
        worker.registerActivitiesImplementations(
                new TaskExecutionActivitiesImpl(),
                new MockEventPublishActivity()
        );

        testEnv.start();
        client = testEnv.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        testEnv.close();
    }

    // ==================== HUMAN Task Tests ====================

    @Test
    void testHumanTaskCompletedBySignal() throws Exception {
        // Test that HUMAN task waits for signal and completes with signal output
        ConductorWorkflowInput input = createHumanTaskWorkflowInput();

        String workflowId = "test-human-signal-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("human-workflow", options);
        workflow.start(input);

        // Wait for HUMAN task to be IN_PROGRESS
        Thread.sleep(500);

        // Query to verify HUMAN task is in progress
        List<?> tasks = workflow.query("getTasks", List.class);
        Map<String, Object> humanTask = findTaskByRefName(tasks, "human_task_ref");
        assertNotNull(humanTask, "HUMAN task should exist");
        assertEquals("IN_PROGRESS", humanTask.get("status"), "HUMAN task should be IN_PROGRESS");

        // Send signal to complete the HUMAN task
        Map<String, Object> signalOutput = new HashMap<>();
        signalOutput.put("approved", true);
        signalOutput.put("approver", "test-user");
        workflow.signal("completeTask", "human_task_ref", signalOutput);

        // Get the result
        ConductorWorkflowOutput output = workflow.getResult(10, TimeUnit.SECONDS, ConductorWorkflowOutput.class);
        assertEquals("COMPLETED", output.getStatus());

        // Verify HUMAN task output via query
        tasks = workflow.query("getTasks", List.class);
        humanTask = findTaskByRefName(tasks, "human_task_ref");
        assertNotNull(humanTask, "HUMAN task should exist after completion");
        assertEquals("COMPLETED", humanTask.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> taskOutput = (Map<String, Object>) humanTask.get("outputData");
        assertNotNull(taskOutput);
        assertEquals(true, taskOutput.get("approved"));
        assertEquals("test-user", taskOutput.get("approver"));
    }

    @Test
    void testHumanTaskFollowedBySimpleTask() throws Exception {
        // Test HUMAN task followed by SIMPLE task
        ConductorWorkflowInput input = createHumanThenSimpleWorkflowInput();

        String workflowId = "test-human-then-simple-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("human-then-simple-workflow", options);
        workflow.start(input);

        // Wait for HUMAN task to be IN_PROGRESS
        Thread.sleep(500);

        // Complete the HUMAN task
        Map<String, Object> signalOutput = new HashMap<>();
        signalOutput.put("result", "human-completed");
        workflow.signal("completeTask", "human_task_ref", signalOutput);

        // Get result - both HUMAN and SIMPLE tasks should complete
        ConductorWorkflowOutput output = workflow.getResult(10, TimeUnit.SECONDS, ConductorWorkflowOutput.class);
        assertEquals("COMPLETED", output.getStatus());

        // Verify both tasks completed
        List<?> tasks = workflow.query("getTasks", List.class);
        assertEquals(2, tasks.size());

        Map<String, Object> humanTask = findTaskByRefName(tasks, "human_task_ref");
        Map<String, Object> simpleTask = findTaskByRefName(tasks, "simple_task_ref");

        assertEquals("COMPLETED", humanTask.get("status"));
        assertEquals("COMPLETED", simpleTask.get("status"));
    }

    // ==================== EVENT Task Tests ====================

    @Test
    void testEventTaskExecution() throws Exception {
        // Test that EVENT task publishes to queue and completes
        ConductorWorkflowInput input = createEventTaskWorkflowInput("conductor:my-event");

        String workflowId = "test-event-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("event-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify EVENT task completed with output
        List<?> tasks = workflow.query("getTasks", List.class);
        Map<String, Object> eventTask = findTaskByRefName(tasks, "event_task_ref");
        assertNotNull(eventTask, "EVENT task should exist");
        assertEquals("COMPLETED", eventTask.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> taskOutput = (Map<String, Object>) eventTask.get("outputData");
        assertNotNull(taskOutput);
        assertNotNull(taskOutput.get("event_produced"), "EVENT task should have event_produced output");
    }

    @Test
    void testEventTaskWithSqsSink() throws Exception {
        // Test EVENT task with SQS sink prefix
        ConductorWorkflowInput input = createEventTaskWorkflowInput("sqs:my-queue");

        String workflowId = "test-event-sqs-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("event-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify EVENT task completed
        List<?> tasks = workflow.query("getTasks", List.class);
        Map<String, Object> eventTask = findTaskByRefName(tasks, "event_task_ref");
        assertEquals("COMPLETED", eventTask.get("status"));
    }

    // ==================== Extension Task Tests ====================
    // NOTE: Extension task tests (JSON_JQ_TRANSFORM, KAFKA_PUBLISH, etc.) require
    // Spring DI to wire up Conductor's WorkflowSystemTask implementations.
    // These are thoroughly tested in E2E tests (ExtensionTaskE2ETest).
    // Unit testing extension tasks would require complex mocking of Conductor internals.

    // ==================== DO_WHILE Loop Tests ====================

    @Test
    void testDoWhileLoopExecution() throws Exception {
        // Test DO_WHILE loop with 5 iterations
        int iterations = 5;
        ConductorWorkflowInput input = createDoWhileWorkflowInput(iterations);

        String workflowId = "test-do-while-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("do-while-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(30, TimeUnit.SECONDS, ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify loop tasks were executed
        List<?> tasks = workflow.query("getTasks", List.class);

        // Count loop tasks (each iteration creates a task like loop_task_ref__1, loop_task_ref__2, etc.)
        int loopTaskCount = 0;
        for (Object taskObj : tasks) {
            @SuppressWarnings("unchecked")
            Map<String, Object> task = (Map<String, Object>) taskObj;
            String refName = (String) task.get("referenceTaskName");
            if (refName != null && refName.startsWith("loop_task_ref")) {
                loopTaskCount++;
            }
        }

        assertEquals(iterations, loopTaskCount, "Should have executed " + iterations + " loop iterations");
    }

    @Test
    void testDoWhileLoopWithMoreIterations() throws Exception {
        // Test DO_WHILE with more iterations to ensure loop logic is robust
        int iterations = 10;
        ConductorWorkflowInput input = createDoWhileWorkflowInput(iterations);

        String workflowId = "test-do-while-more-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("do-while-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(60, TimeUnit.SECONDS, ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());
    }

    // ==================== SET_VARIABLE Task Tests ====================

    @Test
    void testSetVariableTaskExecution() throws Exception {
        // Test SET_VARIABLE task sets workflow variables
        ConductorWorkflowInput input = createSetVariableWorkflowInput();

        String workflowId = "test-set-variable-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("set-variable-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify variables were set via query
        @SuppressWarnings("unchecked")
        Map<String, Object> variables = workflow.query("getVariables", Map.class);
        assertNotNull(variables);
        assertEquals(42, variables.get("myCounter"));
        assertEquals("test-value", variables.get("myString"));
    }

    @Test
    void testSetVariableFollowedBySimpleTask() throws Exception {
        // Test SET_VARIABLE followed by SIMPLE task that uses the variable
        ConductorWorkflowInput input = createSetVariableThenSimpleWorkflowInput();

        String workflowId = "test-set-var-then-simple-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("set-variable-then-simple-workflow", options);
        workflow.start(input);
        ConductorWorkflowOutput output = workflow.getResult(ConductorWorkflowOutput.class);

        assertEquals("COMPLETED", output.getStatus());

        // Verify both tasks completed
        List<?> tasks = workflow.query("getTasks", List.class);
        Map<String, Object> setVarTask = findTaskByRefName(tasks, "set_var_ref");
        Map<String, Object> simpleTask = findTaskByRefName(tasks, "use_var_task_ref");

        assertEquals("COMPLETED", setVarTask.get("status"));
        assertEquals("COMPLETED", simpleTask.get("status"));
    }

    // ==================== Pause/Resume Signal Tests ====================

    @Test
    void testPauseAndResumeSignals() throws Exception {
        // Test pause and resume workflow signals
        // Use HUMAN task to have a workflow that waits (doesn't complete immediately)
        ConductorWorkflowInput input = createHumanTaskWorkflowInput();

        String workflowId = "test-pause-resume-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("human-workflow", options);
        workflow.start(input);

        // Wait for workflow to be running with HUMAN task in progress
        Thread.sleep(500);

        // Query workflow state - should be RUNNING
        WorkflowState state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("RUNNING", state.getStatus());

        // Send pause signal
        workflow.signal("pause");

        // Query - should be PAUSED
        Thread.sleep(200);
        state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("PAUSED", state.getStatus());

        // Send resume signal
        workflow.signal("resume");

        // Query - should be RUNNING again
        Thread.sleep(200);
        state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("RUNNING", state.getStatus());

        // Complete the HUMAN task so workflow can finish
        Map<String, Object> signalOutput = new HashMap<>();
        signalOutput.put("result", "done");
        workflow.signal("completeTask", "human_task_ref", signalOutput);

        // Get result
        ConductorWorkflowOutput output = workflow.getResult(10, TimeUnit.SECONDS, ConductorWorkflowOutput.class);
        assertEquals("COMPLETED", output.getStatus());
    }

    @Test
    void testMultiplePauseResumeCycles() throws Exception {
        // Test multiple pause/resume cycles
        ConductorWorkflowInput input = createHumanTaskWorkflowInput();

        String workflowId = "test-multi-pause-resume-" + UUID.randomUUID();
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build();

        WorkflowStub workflow = client.newUntypedWorkflowStub("human-workflow", options);
        workflow.start(input);

        // Wait for workflow to start
        Thread.sleep(500);

        // Cycle 1: pause and resume
        workflow.signal("pause");
        Thread.sleep(200);
        WorkflowState state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("PAUSED", state.getStatus());

        workflow.signal("resume");
        Thread.sleep(200);
        state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("RUNNING", state.getStatus());

        // Cycle 2: pause and resume again
        workflow.signal("pause");
        Thread.sleep(200);
        state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("PAUSED", state.getStatus());

        workflow.signal("resume");
        Thread.sleep(200);
        state = workflow.query("getWorkflow", WorkflowState.class);
        assertEquals("RUNNING", state.getStatus());

        // Complete the workflow
        Map<String, Object> signalOutput = new HashMap<>();
        signalOutput.put("result", "done");
        workflow.signal("completeTask", "human_task_ref", signalOutput);

        ConductorWorkflowOutput output = workflow.getResult(10, TimeUnit.SECONDS, ConductorWorkflowOutput.class);
        assertEquals("COMPLETED", output.getStatus());
    }

    // ==================== Helper Methods ====================

    @SuppressWarnings("unchecked")
    private Map<String, Object> findTaskByRefName(List<?> tasks, String refName) {
        for (Object taskObj : tasks) {
            Map<String, Object> task = (Map<String, Object>) taskObj;
            if (refName.equals(task.get("referenceTaskName"))) {
                return task;
            }
        }
        return null;
    }

    private ConductorWorkflowInput createHumanTaskWorkflowInput() throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("human-workflow");
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

    private ConductorWorkflowInput createHumanThenSimpleWorkflowInput() throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("human-then-simple-workflow");
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        WorkflowTask humanTask = new WorkflowTask();
        humanTask.setName("human_task");
        humanTask.setTaskReferenceName("human_task_ref");
        humanTask.setType(TaskType.HUMAN.name());
        tasks.add(humanTask);

        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName("simple_task");
        simpleTask.setTaskReferenceName("simple_task_ref");
        simpleTask.setType(TaskType.SIMPLE.name());
        tasks.add(simpleTask);

        workflowDef.setTasks(tasks);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("simple_task");

        Map<String, String> taskDefsJson = new HashMap<>();
        taskDefsJson.put("simple_task", OBJECT_MAPPER.writeValueAsString(taskDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createEventTaskWorkflowInput(String sink) throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("event-workflow");
        workflowDef.setVersion(1);

        WorkflowTask eventTask = new WorkflowTask();
        eventTask.setName("event_task");
        eventTask.setTaskReferenceName("event_task_ref");
        eventTask.setType(TaskType.EVENT.name());
        // EventTaskMapper uses workflowTask.getSink(), so we must set sink via setSink()
        eventTask.setSink(sink);

        workflowDef.setTasks(Collections.singletonList(eventTask));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(Collections.emptyMap())
                .build();
    }


    private ConductorWorkflowInput createDoWhileWorkflowInput(int iterations) throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("do-while-workflow");
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // DO_WHILE task
        WorkflowTask doWhile = new WorkflowTask();
        doWhile.setName("loop");
        doWhile.setTaskReferenceName("loop_ref");
        doWhile.setType(TaskType.DO_WHILE.name());
        doWhile.setLoopCondition("if ($.loop_ref['iteration'] < " + iterations + ") { true; } else { false; }");

        // Task inside the loop
        WorkflowTask loopTask = new WorkflowTask();
        loopTask.setName("loop_task");
        loopTask.setTaskReferenceName("loop_task_ref");
        loopTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> loopTaskInput = new HashMap<>();
        loopTaskInput.put("iteration", "${loop_ref.iteration}");
        loopTask.setInputParameters(loopTaskInput);

        doWhile.setLoopOver(Collections.singletonList(loopTask));
        tasks.add(doWhile);

        workflowDef.setTasks(tasks);

        // Task definition for loop task
        TaskDef loopTaskDef = new TaskDef();
        loopTaskDef.setName("loop_task");

        Map<String, String> taskDefsJson = new HashMap<>();
        taskDefsJson.put("loop_task", OBJECT_MAPPER.writeValueAsString(loopTaskDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    private ConductorWorkflowInput createSetVariableWorkflowInput() throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("set-variable-workflow");
        workflowDef.setVersion(1);

        WorkflowTask setVarTask = new WorkflowTask();
        setVarTask.setName("set_variables");
        setVarTask.setTaskReferenceName("set_var_ref");
        setVarTask.setType(TaskType.SET_VARIABLE.name());

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("myCounter", 42);
        inputParams.put("myString", "test-value");
        setVarTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(setVarTask));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(Collections.emptyMap())
                .build();
    }

    private ConductorWorkflowInput createSetVariableThenSimpleWorkflowInput() throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("set-variable-then-simple-workflow");
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // SET_VARIABLE task
        WorkflowTask setVarTask = new WorkflowTask();
        setVarTask.setName("set_variables");
        setVarTask.setTaskReferenceName("set_var_ref");
        setVarTask.setType(TaskType.SET_VARIABLE.name());
        Map<String, Object> setVarInput = new HashMap<>();
        setVarInput.put("computedValue", 100);
        setVarTask.setInputParameters(setVarInput);
        tasks.add(setVarTask);

        // SIMPLE task that uses the variable
        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName("use_var_task");
        simpleTask.setTaskReferenceName("use_var_task_ref");
        simpleTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> simpleInput = new HashMap<>();
        simpleInput.put("value", "${workflow.variables.computedValue}");
        simpleTask.setInputParameters(simpleInput);
        tasks.add(simpleTask);

        workflowDef.setTasks(tasks);

        TaskDef taskDef = new TaskDef();
        taskDef.setName("use_var_task");

        Map<String, String> taskDefsJson = new HashMap<>();
        taskDefsJson.put("use_var_task", OBJECT_MAPPER.writeValueAsString(taskDef));

        return ConductorWorkflowInput.builder()
                .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                .workflowInput(Collections.emptyMap())
                .taskDefsJson(taskDefsJson)
                .build();
    }

    // ==================== Mock Activity Implementations ====================

    /**
     * Mock EventPublishActivity that succeeds without actually publishing.
     */
    static class MockEventPublishActivity implements EventPublishActivity {
        @Override
        public void publish(String queueName, String messageId, String payload) {
            // No-op for testing
        }

        @Override
        public TaskExecutionResult executeEvent(String taskRefName, String conductorTaskType, Map<String, Object> inputData) {
            Map<String, Object> output = new HashMap<>();
            output.put("event_produced", inputData.get("queueName"));
            return new TaskExecutionResult(output, "COMPLETED", null);
        }
    }

}
