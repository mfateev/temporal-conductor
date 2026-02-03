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

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for workflow termination functionality.
 *
 * These tests verify:
 * - Basic workflow termination via DELETE endpoint
 * - Termination with reason parameter
 * - Handling of termination on already completed workflows
 * - Search for terminated workflows using ConductorStatus
 */
class TerminateE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(TerminateE2ETest.class);

    // Slow task delay - long enough to allow termination before completion
    private static final long SLOW_TASK_DELAY_MS = 30000L;

    /**
     * Test basic workflow termination.
     * Starts a workflow with a slow task, terminates it, and verifies the status becomes TERMINATED.
     *
     * Note: When Temporal terminates a workflow externally, the workflow code doesn't run,
     * so endTime from the in-memory state may not be set. The status is determined from
     * Temporal's workflow execution status.
     */
    @Test
    void testBasicWorkflowTermination() throws Exception {
        String workflowName = "terminate_basic_workflow";
        String taskName = "slow_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with slow task (stays running for a while)
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, SLOW_TASK_DELAY_MS);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with ID: {}", workflowId);

        // Wait for workflow to be RUNNING
        WorkflowStatusResponse status = waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));
        assertEquals("RUNNING", status.getStatus());
        log.info("Workflow is RUNNING");

        // Terminate the workflow
        terminateWorkflow(workflowId, null);
        log.info("Terminated workflow: {}", workflowId);

        // Wait for status to become TERMINATED
        status = waitForWorkflowStatus(workflowId, "TERMINATED", Duration.ofSeconds(10));
        assertEquals("TERMINATED", status.getStatus());
        log.info("Workflow is TERMINATED");

        // Note: endTime may not be set when workflow is terminated externally
        // because the workflow code doesn't get to run to update in-memory state.
        // The important assertion is that the status is TERMINATED.
    }

    /**
     * Test workflow termination with a reason.
     * Verifies the workflow becomes TERMINATED.
     *
     * Note: When Temporal terminates a workflow externally, the reason is stored in
     * Temporal's workflow history but may not be accessible via the workflow query
     * since the workflow code doesn't run. The key assertion is that the workflow
     * reaches TERMINATED status.
     */
    @Test
    void testTerminateWithReason() throws Exception {
        String workflowName = "terminate_reason_workflow";
        String taskName = "slow_task";
        String terminationReason = "Terminated by E2E test for verification";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with slow task
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, SLOW_TASK_DELAY_MS);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with ID: {}", workflowId);

        // Wait for workflow to be RUNNING
        waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));

        // Terminate with reason
        terminateWorkflow(workflowId, terminationReason);
        log.info("Terminated workflow with reason: {}", terminationReason);

        // Wait for status to become TERMINATED
        WorkflowStatusResponse status = waitForWorkflowStatus(workflowId, "TERMINATED", Duration.ofSeconds(10));
        assertEquals("TERMINATED", status.getStatus());
        log.info("Workflow successfully terminated");
    }

    /**
     * Test terminating an already completed workflow.
     * The server should handle this gracefully (either no-op or appropriate error).
     */
    @Test
    void testTerminateAlreadyCompletedWorkflow() throws Exception {
        String workflowName = "terminate_completed_workflow";
        String taskName = "quick_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with a quick task (completes immediately)
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        registerWorkflowDef(workflowDef);

        // Start workflow and wait for completion
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());
        log.info("Workflow COMPLETED");

        // Try to terminate the completed workflow - should be handled gracefully
        try {
            terminateWorkflow(workflowId, "Attempting to terminate completed workflow");
            // If no exception, verify the workflow is still COMPLETED (terminate had no effect)
            status = getWorkflowStatus(workflowId);
            assertEquals("COMPLETED", status.getStatus(),
                    "Completed workflow should remain COMPLETED after terminate attempt");
            log.info("Terminate on completed workflow handled gracefully - status remains COMPLETED");
        } catch (Exception e) {
            // It's acceptable to get an error when terminating a completed workflow
            log.info("Terminate on completed workflow threw expected error: {}", e.getMessage());
        }
    }

    /**
     * Test searching for terminated workflows by workflow type.
     * Terminates a workflow and verifies it can be found via search.
     *
     * Note: When Temporal terminates a workflow externally, the ConductorStatus
     * search attribute is NOT updated (stays RUNNING) because the workflow code
     * doesn't run. However, the workflow can still be found by workflowType.
     */
    @Test
    void testSearchTerminatedWorkflows() throws Exception {
        String workflowName = "terminate_search_workflow";
        String taskName = "slow_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with slow task
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, SLOW_TASK_DELAY_MS);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with ID: {}", workflowId);

        // Wait for workflow to be RUNNING
        waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));

        // Terminate the workflow
        terminateWorkflow(workflowId, "Terminated for search test");

        // Wait for status to become TERMINATED (from Temporal's ExecutionStatus)
        WorkflowStatusResponse status = waitForWorkflowStatus(workflowId, "TERMINATED", Duration.ofSeconds(10));
        assertEquals("TERMINATED", status.getStatus());
        log.info("Workflow is TERMINATED");

        // Search for the workflow by type
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
                // Status in search results comes from Temporal's visibility
                log.info("Found workflow in search results with status: {}", result.get("status").asText());
                break;
            }
        }
        assertTrue(found, "Should find our workflow in search results");
    }

    /**
     * Test ConductorStatus search attribute behavior on termination.
     *
     * IMPORTANT: This test documents a known limitation - when a workflow is
     * terminated externally by Temporal, the ConductorStatus search attribute
     * is NOT updated because the workflow code doesn't get to run.
     *
     * The workflow status returned by getWorkflow is TERMINATED (mapped from
     * Temporal's ExecutionStatus), but the ConductorStatus search attribute
     * remains RUNNING.
     */
    @Test
    void testTerminatedWorkflowSearchAttributeLimitation() throws Exception {
        String workflowName = "terminate_search_attr_workflow";
        String taskName = "slow_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with slow task
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, SLOW_TASK_DELAY_MS);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for workflow to be RUNNING
        waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));

        // Verify initial ConductorStatus search attribute is RUNNING
        String conductorStatus = getSearchAttribute(workflowId, "ConductorStatus");
        log.info("Initial ConductorStatus search attribute: {}", conductorStatus);
        assertEquals("RUNNING", conductorStatus,
                "ConductorStatus search attribute should be RUNNING initially");

        // Terminate the workflow
        terminateWorkflow(workflowId, null);

        // Wait for status to become TERMINATED (from Temporal's ExecutionStatus)
        WorkflowStatusResponse status = waitForWorkflowStatus(workflowId, "TERMINATED", Duration.ofSeconds(10));
        assertEquals("TERMINATED", status.getStatus(),
                "Workflow status should be TERMINATED");

        // Document the limitation: ConductorStatus search attribute is NOT updated
        // when terminated externally because workflow code doesn't run
        conductorStatus = getSearchAttribute(workflowId, "ConductorStatus");
        log.info("ConductorStatus search attribute after termination: {}", conductorStatus);
        // The search attribute stays RUNNING because the workflow code didn't run
        assertEquals("RUNNING", conductorStatus,
                "ConductorStatus search attribute stays RUNNING when terminated externally " +
                "(this is a known limitation - workflow code doesn't run on external termination)");
    }

    // ==================== Helper Methods ====================

    /**
     * Terminate a workflow via Conductor REST API.
     *
     * @param workflowId the workflow ID to terminate
     * @param reason optional termination reason
     */
    protected void terminateWorkflow(String workflowId, String reason) {
        var uriSpec = conductorClient.delete()
                .uri(uriBuilder -> {
                    uriBuilder.path("/api/workflow/{workflowId}");
                    if (reason != null) {
                        uriBuilder.queryParam("reason", reason);
                    }
                    return uriBuilder.build(workflowId);
                });

        uriSpec.retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
        log.info("Terminated workflow: {} with reason: {}", workflowId, reason);
    }
}
