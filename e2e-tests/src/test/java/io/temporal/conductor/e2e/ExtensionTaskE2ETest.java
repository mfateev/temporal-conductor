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

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for extension tasks (JSON_JQ_TRANSFORM, etc.) against real Temporal server.
 */
class ExtensionTaskE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ExtensionTaskE2ETest.class);

    @Test
    void testJsonJqTransformTask() throws Exception {
        String workflowName = "jq_transform_e2e";

        // Create workflow with JSON_JQ_TRANSFORM task
        // Expression accesses .data because workflow input is mapped to task input under 'data' key
        WorkflowDef workflowDef = createJqTransformWorkflow(workflowName,
                "{ name: .data.name, location: .data.city }");
        registerWorkflowDef(workflowDef);

        // Start workflow with input data
        Map<String, Object> input = new HashMap<>();
        input.put("name", "John");
        input.put("age", 30);
        input.put("city", "New York");

        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started workflow with JSON_JQ_TRANSFORM task: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus(),
                "Workflow should complete. Reason: " + status.getReasonForIncompletion());

        // Verify task completed
        WorkflowStatusResponse.TaskInfo jqTask = status.getTasks().stream()
                .filter(t -> "jq_transform_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(jqTask, "JSON_JQ_TRANSFORM task should exist");
        assertEquals("COMPLETED", jqTask.getStatus(), "Task should be completed");
        assertNotNull(jqTask.getOutputData(), "Task should have output data");
        log.info("JQ transform output: {}", jqTask.getOutputData());
    }

    @Test
    void testJsonJqTransformWithArrayInput() throws Exception {
        String workflowName = "jq_array_e2e";

        // Create workflow with JQ expression that processes array
        // Expression accesses .data because workflow input is mapped to task input under 'data' key
        WorkflowDef workflowDef = createJqTransformWorkflow(workflowName,
                "[.data.items[].value] | add");
        registerWorkflowDef(workflowDef);

        // Start workflow with array input
        Map<String, Object> input = new HashMap<>();
        input.put("items", List.of(
                Map.of("id", 1, "value", 10),
                Map.of("id", 2, "value", 20),
                Map.of("id", 3, "value", 30)
        ));

        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started workflow with JQ array transform: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus(),
                "Workflow should complete. Reason: " + status.getReasonForIncompletion());
    }

    @Test
    void testJsonJqTransformExtractsNestedField() throws Exception {
        String workflowName = "jq_nested_e2e";

        // Create workflow with JQ expression that extracts nested data
        // Expression accesses .data because workflow input is mapped to task input under 'data' key
        WorkflowDef workflowDef = createJqTransformWorkflow(workflowName,
                ".data.user.address.city");
        registerWorkflowDef(workflowDef);

        // Start workflow with nested input
        Map<String, Object> input = new HashMap<>();
        input.put("user", Map.of(
                "name", "Alice",
                "address", Map.of(
                        "city", "San Francisco",
                        "zip", "94102"
                )
        ));

        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started workflow with nested JQ extraction: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus(),
                "Workflow should complete. Reason: " + status.getReasonForIncompletion());

        // Verify task output
        WorkflowStatusResponse.TaskInfo jqTask = status.getTasks().stream()
                .filter(t -> "jq_transform_ref".equals(t.getReferenceTaskName()))
                .findFirst()
                .orElse(null);
        assertNotNull(jqTask, "JQ task should exist");
        assertEquals("COMPLETED", jqTask.getStatus());
        log.info("Nested extraction output: {}", jqTask.getOutputData());
    }

    @Test
    void testJsonJqTransformFollowedBySimpleTask() throws Exception {
        String workflowName = "jq_then_simple_e2e";
        String simpleTaskName = "after_jq_task";

        // Register simple task
        registerTaskDefs(createTaskDefs(List.of(simpleTaskName)));

        // Create workflow: JQ transform -> SIMPLE task
        WorkflowDef workflowDef = createJqTransformThenSimpleWorkflow(workflowName, simpleTaskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        input.put("value", 42);

        String workflowId = startWorkflow(workflowName, 1, input);
        log.info("Started workflow with JQ then SIMPLE: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus(),
                "Workflow should complete. Reason: " + status.getReasonForIncompletion());

        // Verify both tasks completed
        long completedCount = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .count();
        assertEquals(2, completedCount, "Both tasks should be completed");
    }

    // ==================== Helper Methods ====================

    private WorkflowDef createJqTransformWorkflow(String workflowName, String jqExpression) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        WorkflowTask jqTask = new WorkflowTask();
        jqTask.setName("jq_transform");
        jqTask.setTaskReferenceName("jq_transform_ref");
        jqTask.setType("JSON_JQ_TRANSFORM");

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("queryExpression", jqExpression);
        // Pass workflow input to task so JQ expression can access it
        // The JQ expression operates on the task's input data (excluding queryExpression)
        inputParams.put("data", "${workflow.input}");
        jqTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(jqTask));
        return workflowDef;
    }

    private WorkflowDef createJqTransformThenSimpleWorkflow(String workflowName, String simpleTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        // JSON_JQ_TRANSFORM task
        WorkflowTask jqTask = new WorkflowTask();
        jqTask.setName("jq_transform");
        jqTask.setTaskReferenceName("jq_transform_ref");
        jqTask.setType("JSON_JQ_TRANSFORM");
        Map<String, Object> jqParams = new HashMap<>();
        // Expression accesses .data because workflow input is mapped to task input under 'data' key
        jqParams.put("queryExpression", "{ doubled: (.data.value * 2) }");
        // Pass workflow input to task so JQ expression can access it
        jqParams.put("data", "${workflow.input}");
        jqTask.setInputParameters(jqParams);

        // SIMPLE task
        WorkflowTask simpleTask = new WorkflowTask();
        simpleTask.setName(simpleTaskName);
        simpleTask.setTaskReferenceName(simpleTaskName + "_ref");
        simpleTask.setType("SIMPLE");

        workflowDef.setTasks(List.of(jqTask, simpleTask));
        return workflowDef;
    }
}
