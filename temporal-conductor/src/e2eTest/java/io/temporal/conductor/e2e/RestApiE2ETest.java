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

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for Conductor REST API integration against real Temporal server.
 */
class RestApiE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(RestApiE2ETest.class);

    @Test
    void testWorkflowLifecycle() throws Exception {
        String workflowName = "lifecycle_test_workflow";
        String taskName = "lifecycle_task";

        // Register task and workflow
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Start workflow with input
        Map<String, Object> input = new HashMap<>();
        input.put("testKey", "testValue");
        input.put("number", 42);
        String workflowId = startWorkflow(workflowName, 1, input);

        assertNotNull(workflowId, "Workflow ID should not be null");
        log.info("Started workflow with ID: {}", workflowId);

        // Get workflow status
        WorkflowStatusResponse status = getWorkflowStatus(workflowId);
        assertNotNull(status, "Workflow status should not be null");
        assertEquals(workflowId, status.getWorkflowId());
        assertEquals(workflowName, status.getWorkflowType());

        // Wait for completion
        status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());
        assertNotNull(status.getStartTime());
        assertNotNull(status.getEndTime());
    }

    @Test
    void testWorkflowStatusReflectsProgress() throws Exception {
        String workflowName = "status_progress_workflow";
        List<String> taskNames = List.of("step1", "step2", "step3");

        // Register tasks and workflow
        registerTaskDefs(createTaskDefs(taskNames));
        registerWorkflowDef(createSequentialWorkflowDef(workflowName, taskNames));

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion and verify tasks
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));

        assertEquals("COMPLETED", status.getStatus());
        assertNotNull(status.getTasks());
        assertEquals(taskNames.size(), status.getTasks().size(),
                "Should have all tasks in response");

        // Verify each task completed
        for (WorkflowStatusResponse.TaskInfo task : status.getTasks()) {
            assertEquals("COMPLETED", task.getStatus(),
                    "Task " + task.getReferenceTaskName() + " should be completed");
        }
    }

    @Test
    void testWorkflowWithCorrelationId() throws Exception {
        String workflowName = "correlation_test_workflow";
        String taskName = "correlation_task";
        String correlationId = "test-correlation-" + System.currentTimeMillis();

        // Register task and workflow
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Start workflow with correlation ID
        Map<String, Object> request = new HashMap<>();
        request.put("name", workflowName);
        request.put("version", 1);
        request.put("input", Collections.emptyMap());
        request.put("correlationId", correlationId);

        String json = OBJECT_MAPPER.writeValueAsString(request);
        String workflowId = conductorClient.post()
                .uri("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(json)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        workflowId = workflowId != null ? workflowId.replace("\"", "") : null;

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));

        assertEquals("COMPLETED", status.getStatus());
        assertEquals(correlationId, status.getCorrelationId());
    }

    @Test
    void testGetMetadataWorkflowDefs() throws Exception {
        String workflowName = "metadata_test_workflow";
        String taskName = "metadata_task";

        // Register task and workflow
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Get workflow definition from metadata API
        String response = conductorClient.get()
                .uri("/api/metadata/workflow/{name}", workflowName)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));

        assertNotNull(response);
        JsonNode workflowDefJson = OBJECT_MAPPER.readTree(response);
        assertEquals(workflowName, workflowDefJson.get("name").asText());
    }

    @Test
    void testSearchWorkflows() throws Exception {
        String workflowName = "search_test_workflow";
        String taskName = "search_task";
        String correlationId = "search-correlation-" + System.currentTimeMillis();

        // Register task and workflow
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Start workflow with unique correlation ID
        Map<String, Object> request = new HashMap<>();
        request.put("name", workflowName);
        request.put("version", 1);
        request.put("input", Collections.emptyMap());
        request.put("correlationId", correlationId);

        String json = OBJECT_MAPPER.writeValueAsString(request);
        String workflowId = conductorClient.post()
                .uri("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(json)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        workflowId = workflowId != null ? workflowId.replace("\"", "") : null;

        // Wait for completion
        waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));

        // Search for workflow by workflow type
        String searchQuery = "workflowType=" + workflowName;
        String searchResponse = conductorClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/workflow/search")
                        .queryParam("query", searchQuery)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));

        assertNotNull(searchResponse);
        JsonNode searchResult = OBJECT_MAPPER.readTree(searchResponse);
        assertTrue(searchResult.has("results"), "Search response should have results");

        // Verify our workflow is in the results
        JsonNode results = searchResult.get("results");
        assertTrue(results.isArray(), "Results should be an array");

        boolean found = false;
        for (JsonNode result : results) {
            if (workflowId.equals(result.get("workflowId").asText())) {
                found = true;
                assertEquals(workflowName, result.get("workflowType").asText());
                break;
            }
        }
        assertTrue(found, "Should find our workflow in search results");
    }

    @Test
    void testWorkflowInputOutputPassthrough() throws Exception {
        String workflowName = "io_passthrough_workflow";
        String taskName = "io_task";

        // Register task and workflow
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Start workflow with complex input
        Map<String, Object> input = new HashMap<>();
        input.put("stringField", "hello");
        input.put("numberField", 123);
        input.put("boolField", true);
        Map<String, Object> nestedObject = new HashMap<>();
        nestedObject.put("nestedKey", "nestedValue");
        input.put("objectField", nestedObject);

        String workflowId = startWorkflow(workflowName, 1, input);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());

        // Verify input was stored correctly
        assertNotNull(status.getInput());
        assertEquals("hello", status.getInput().get("stringField"));
        assertEquals(123, status.getInput().get("numberField"));
        assertEquals(true, status.getInput().get("boolField"));

        // Verify output exists
        assertNotNull(status.getOutput());
    }
}
