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
 * E2E tests for EVENT task type against real Temporal server.
 * EVENT tasks publish events to queues (using logging providers by default).
 */
class EventE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(EventE2ETest.class);

    @Test
    void testSimpleEventTask() throws Exception {
        String workflowName = "event_simple_e2e";

        // Create workflow with EVENT task
        WorkflowDef workflowDef = createWorkflowWithEventTask(workflowName, "conductor:my-event");
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with EVENT task: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify EVENT task completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("event_task_ref"),
                "EVENT task should be completed");

        // Verify EVENT task output contains event_produced
        WorkflowStatusResponse.TaskInfo eventTask = status.getTasks().stream()
                .filter(t -> "event_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(eventTask);
        assertNotNull(eventTask.getOutputData());
        assertNotNull(eventTask.getOutputData().get("event_produced"),
                "EVENT task output should contain 'event_produced' with queue name");
        log.info("Event published to: {}", eventTask.getOutputData().get("event_produced"));
    }

    @Test
    void testEventTaskWithSqsSink() throws Exception {
        String workflowName = "event_sqs_e2e";

        // Create workflow with EVENT task using SQS sink
        WorkflowDef workflowDef = createWorkflowWithEventTask(workflowName, "sqs:my-sqs-queue");
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with SQS EVENT task: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify EVENT task output
        WorkflowStatusResponse.TaskInfo eventTask = status.getTasks().stream()
                .filter(t -> "event_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(eventTask);
        assertEquals("sqs:my-sqs-queue", eventTask.getOutputData().get("event_produced"));
    }

    @Test
    void testEventTaskWithPayload() throws Exception {
        String workflowName = "event_payload_e2e";

        // Create workflow with EVENT task that includes custom payload
        WorkflowDef workflowDef = createWorkflowWithEventTaskAndPayload(workflowName);
        registerWorkflowDef(workflowDef);

        // Start workflow with input
        Map<String, Object> input = new HashMap<>();
        input.put("orderId", "ORD-12345");
        input.put("customerId", "CUST-789");

        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started workflow with EVENT payload: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify EVENT task output contains workflow metadata
        WorkflowStatusResponse.TaskInfo eventTask = status.getTasks().stream()
                .filter(t -> "event_task_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(eventTask);
        assertNotNull(eventTask.getOutputData().get("workflowInstanceId"),
                "Event payload should contain workflowInstanceId");
        assertNotNull(eventTask.getOutputData().get("workflowType"),
                "Event payload should contain workflowType");
        log.info("Event output: {}", eventTask.getOutputData());
    }

    @Test
    void testEventTaskFollowedBySimpleTask() throws Exception {
        String workflowName = "event_then_simple_e2e";
        String simpleTaskName = "after_event_task";

        // Register simple task
        registerTaskDefs(createTaskDefs(List.of(simpleTaskName)));

        // Create workflow with EVENT -> SIMPLE sequence
        WorkflowDef workflowDef = createWorkflowWithEventAndSimpleTask(workflowName, simpleTaskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with EVENT then SIMPLE: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify both tasks completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        assertTrue(completedTaskRefs.contains("event_task_ref"),
                "EVENT task should be completed");
        assertTrue(completedTaskRefs.contains(simpleTaskName + "_ref"),
                "Simple task after EVENT should be completed");
    }

    // ==================== Helper Methods ====================

    /**
     * Create a workflow with a single EVENT task.
     */
    private WorkflowDef createWorkflowWithEventTask(String workflowName, String sink) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask eventTask = new WorkflowTask();
        eventTask.setName("event_task");
        eventTask.setTaskReferenceName("event_task_ref");
        eventTask.setType(TaskType.EVENT.name());

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("sink", sink);
        eventTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(eventTask));
        return workflowDef;
    }

    /**
     * Create a workflow with EVENT task that includes custom payload from workflow input.
     */
    private WorkflowDef createWorkflowWithEventTaskAndPayload(String workflowName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask eventTask = new WorkflowTask();
        eventTask.setName("event_task");
        eventTask.setTaskReferenceName("event_task_ref");
        eventTask.setType(TaskType.EVENT.name());

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("sink", "conductor:order-events");
        inputParams.put("orderId", "${workflow.input.orderId}");
        inputParams.put("customerId", "${workflow.input.customerId}");
        eventTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(eventTask));
        return workflowDef;
    }

    /**
     * Create a workflow with EVENT task followed by a SIMPLE task.
     */
    private WorkflowDef createWorkflowWithEventAndSimpleTask(String workflowName, String simpleTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // EVENT task
        WorkflowTask eventTask = new WorkflowTask();
        eventTask.setName("event_task");
        eventTask.setTaskReferenceName("event_task_ref");
        eventTask.setType(TaskType.EVENT.name());
        Map<String, Object> eventParams = new HashMap<>();
        eventParams.put("sink", "kafka:order-notifications");
        eventTask.setInputParameters(eventParams);
        tasks.add(eventTask);

        // SIMPLE task after EVENT
        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName(simpleTaskName);
        simpleTask.setTaskReferenceName(simpleTaskName + "_ref");
        simpleTask.setType(TaskType.SIMPLE.name());
        tasks.add(simpleTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }
}
