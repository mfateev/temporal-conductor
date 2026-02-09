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
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInfo;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests verifying external worker functionality.
 *
 * <p>Tests that tasks with names in the format {@code taskName@taskQueue} are routed
 * to external workers listening on the specified task queue.
 */
class ExternalWorkerE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ExternalWorkerE2ETest.class);
    private static final String EXTERNAL_TASK_QUEUE = "external-workers";

    private static WorkerFactory externalWorkerFactory;

    @BeforeAll
    static void startExternalWorker() {
        // Create a worker factory for the external worker
        externalWorkerFactory = WorkerFactory.newInstance(workflowClient);

        // Create worker on the external task queue
        Worker worker = externalWorkerFactory.newWorker(EXTERNAL_TASK_QUEUE);

        // Register a dynamic activity that handles any activity type
        worker.registerActivitiesImplementations(new ExternalWorkerActivity());

        // Start the worker
        externalWorkerFactory.start();
        log.info("Started external worker on task queue: {}", EXTERNAL_TASK_QUEUE);
    }

    @AfterAll
    static void stopExternalWorker() {
        if (externalWorkerFactory != null) {
            externalWorkerFactory.shutdown();
            log.info("Stopped external worker");
        }
    }

    @Test
    void testTaskRoutedToExternalWorker() throws Exception {
        String workflowName = "external_worker_test_workflow";
        // Task name with @taskQueue suffix routes to external worker
        String taskName = "process_external@" + EXTERNAL_TASK_QUEUE;

        // Register task definition (the full name including @taskQueue)
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Create workflow with the external task
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask task = new WorkflowTask();
        task.setName(taskName);
        task.setTaskReferenceName("process_external_ref");
        task.setType(TaskType.SIMPLE.name());
        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("orderId", "12345");
        inputParams.put("customerName", "Test Customer");
        task.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(task));
        registerWorkflowDef(workflowDef);

        // Start workflow with input data
        Map<String, Object> workflowInput = new HashMap<>();
        workflowInput.put("orderId", "12345");
        workflowInput.put("customerName", "Test Customer");
        String workflowId = startWorkflow(workflowName, 1, workflowInput);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus(), "Workflow should complete successfully");

        // Verify the output contains external worker metadata
        Map<String, Object> output = status.getOutput();
        assertNotNull(output, "Workflow should have output");

        // The task output should be in the workflow output
        @SuppressWarnings("unchecked")
        Map<String, Object> taskOutput = (Map<String, Object>) output.get("process_external_ref");
        if (taskOutput != null) {
            assertEquals("external-worker", taskOutput.get("_processedBy"),
                    "Task should be processed by external worker");
        }

        // Verify activity was scheduled
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);
        assertEquals(1, activityTypes.size(), "Should have exactly one activity");

        // Activity type should be the base name without @taskQueue suffix
        // The TaskNameParser extracts "process_external" from "process_external@external-workers"
        String actualActivityType = activityTypes.get(0);
        assertEquals("process_external", actualActivityType,
                "Activity type should be base name without @taskQueue suffix. " +
                "If this fails, ensure the Docker image is rebuilt with --no-cache");
    }

    @Test
    void testMixedInternalAndExternalTasks() throws Exception {
        String workflowName = "mixed_worker_test_workflow";
        String internalTask = "internal_task";
        String externalTask = "external_task@" + EXTERNAL_TASK_QUEUE;

        // Register both task definitions
        registerTaskDefs(createTaskDefs(List.of(internalTask, externalTask)));

        // Create workflow with both internal and external tasks
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // First task - internal (default task queue)
        WorkflowTask task1 = new WorkflowTask();
        task1.setName(internalTask);
        task1.setTaskReferenceName("internal_task_ref");
        task1.setType(TaskType.SIMPLE.name());
        tasks.add(task1);

        // Second task - external
        WorkflowTask task2 = new WorkflowTask();
        task2.setName(externalTask);
        task2.setTaskReferenceName("external_task_ref");
        task2.setType(TaskType.SIMPLE.name());
        tasks.add(task2);

        // Third task - internal again
        WorkflowTask task3 = new WorkflowTask();
        task3.setName(internalTask);
        task3.setTaskReferenceName("internal_task_2_ref");
        task3.setType(TaskType.SIMPLE.name());
        tasks.add(task3);

        workflowDef.setTasks(tasks);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(45));
        assertEquals("COMPLETED", status.getStatus(), "Workflow should complete successfully");

        // Verify activity types
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);

        assertEquals(3, activityTypes.size(), "Should have 3 activities");
        assertEquals("internal_task", activityTypes.get(0), "First activity should be internal_task");
        assertEquals("external_task", activityTypes.get(1), "Second activity should be external_task (base name)");
        assertEquals("internal_task", activityTypes.get(2), "Third activity should be internal_task");
    }

    @Test
    void testTaskWithoutAtSymbolUsesDefaultQueue() throws Exception {
        String workflowName = "default_queue_test_workflow";
        String taskName = "simple_task"; // No @taskQueue suffix

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Create workflow
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus(), "Workflow should complete successfully");

        // Verify activity type
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        assertEquals(1, activityTypes.size(), "Should have exactly one activity");
        assertEquals(taskName, activityTypes.get(0), "Activity type should equal task name");
    }

    /**
     * Dynamic activity implementation for the external worker.
     */
    public static class ExternalWorkerActivity implements DynamicActivity {

        private static final Logger activityLog = LoggerFactory.getLogger(ExternalWorkerActivity.class);

        @Override
        @SuppressWarnings("unchecked")
        public Object execute(EncodedValues args) {
            ActivityInfo info = Activity.getExecutionContext().getInfo();
            String activityType = info.getActivityType();

            // Extract arguments: taskRefName, conductorTaskType, inputData
            String taskRefName = args.get(0, String.class);
            String conductorTaskType = args.get(1, String.class);
            Map<String, Object> inputData = args.get(2, Map.class);

            activityLog.info("EXTERNAL WORKER: Processing activity '{}' for task '{}'",
                    activityType, taskRefName);

            // Build output with metadata showing external processing
            Map<String, Object> output = new HashMap<>();
            if (inputData != null) {
                output.putAll(inputData);
            }
            output.put("_processedBy", "external-worker");
            output.put("_activityType", activityType);
            output.put("_taskRefName", taskRefName);

            // Return result in TaskExecutionResult format
            Map<String, Object> result = new HashMap<>();
            result.put("output", output);
            result.put("status", "COMPLETED");
            result.put("failureReason", null);

            activityLog.info("EXTERNAL WORKER: Completed task '{}' with output keys: {}",
                    taskRefName, output.keySet());

            return result;
        }
    }
}
