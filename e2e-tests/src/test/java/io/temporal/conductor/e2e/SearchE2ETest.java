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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for Conductor search/query functionality against real Temporal server.
 *
 * <p>Tests the ConductorQueryTranslator functionality by executing search queries
 * via the REST API and verifying results against a real Temporal server.
 *
 * <p>Covers:
 * <ul>
 *   <li>Search by status (equals syntax and legacy colon syntax)</li>
 *   <li>Search by workflowType</li>
 *   <li>Search with AND conditions</li>
 *   <li>Search with IN clause</li>
 *   <li>Search by correlationId</li>
 * </ul>
 */
class SearchE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(SearchE2ETest.class);

    // Unique test identifier to avoid interference from other tests
    private static final String TEST_RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    // Workflow names for this test class
    private static final String WORKFLOW_TYPE_A = "search_e2e_workflow_a_" + TEST_RUN_ID;
    private static final String WORKFLOW_TYPE_B = "search_e2e_workflow_b_" + TEST_RUN_ID;
    private static final String TASK_NAME = "search_e2e_task_" + TEST_RUN_ID;

    // Correlation IDs for testing
    private static final String CORRELATION_ID_1 = "corr-search-1-" + TEST_RUN_ID;
    private static final String CORRELATION_ID_2 = "corr-search-2-" + TEST_RUN_ID;

    // Store workflow IDs for assertions
    private static final List<String> workflowIdsTypeA = new ArrayList<>();
    private static final List<String> workflowIdsTypeB = new ArrayList<>();
    private static String workflowIdWithCorrelation1;
    private static String workflowIdWithCorrelation2;

    /**
     * Set up test data by creating workflows with different types, statuses, and correlation IDs.
     * This runs once before all tests in this class.
     */
    @BeforeAll
    static void setUpTestData() throws Exception {
        log.info("Setting up search E2E test data with test run ID: {}", TEST_RUN_ID);

        // Register task definitions
        var taskDef = new com.netflix.conductor.common.metadata.tasks.TaskDef();
        taskDef.setName(TASK_NAME);
        String taskJson = OBJECT_MAPPER.writeValueAsString(List.of(taskDef));
        conductorClient.post()
                .uri("/api/metadata/taskdefs")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(taskJson)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));

        // Register workflow type A
        var workflowDefA = createSimpleWorkflowDefStatic(WORKFLOW_TYPE_A, TASK_NAME);
        String workflowJsonA = OBJECT_MAPPER.writeValueAsString(workflowDefA);
        conductorClient.post()
                .uri("/api/metadata/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(workflowJsonA)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
        log.info("Registered workflow definition: {}", WORKFLOW_TYPE_A);

        // Register workflow type B
        var workflowDefB = createSimpleWorkflowDefStatic(WORKFLOW_TYPE_B, TASK_NAME);
        String workflowJsonB = OBJECT_MAPPER.writeValueAsString(workflowDefB);
        conductorClient.post()
                .uri("/api/metadata/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(workflowJsonB)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
        log.info("Registered workflow definition: {}", WORKFLOW_TYPE_B);

        // Start 2 workflows of type A (will complete)
        for (int i = 0; i < 2; i++) {
            String workflowId = startWorkflowStatic(WORKFLOW_TYPE_A, 1, Collections.emptyMap(), null);
            workflowIdsTypeA.add(workflowId);
            log.info("Started workflow type A: {}", workflowId);
        }

        // Start 2 workflows of type B (will complete)
        for (int i = 0; i < 2; i++) {
            String workflowId = startWorkflowStatic(WORKFLOW_TYPE_B, 1, Collections.emptyMap(), null);
            workflowIdsTypeB.add(workflowId);
            log.info("Started workflow type B: {}", workflowId);
        }

        // Start workflow with correlation ID 1
        workflowIdWithCorrelation1 = startWorkflowStatic(WORKFLOW_TYPE_A, 1, Collections.emptyMap(), CORRELATION_ID_1);
        workflowIdsTypeA.add(workflowIdWithCorrelation1);
        log.info("Started workflow with correlation ID 1: {}", workflowIdWithCorrelation1);

        // Start workflow with correlation ID 2
        workflowIdWithCorrelation2 = startWorkflowStatic(WORKFLOW_TYPE_B, 1, Collections.emptyMap(), CORRELATION_ID_2);
        workflowIdsTypeB.add(workflowIdWithCorrelation2);
        log.info("Started workflow with correlation ID 2: {}", workflowIdWithCorrelation2);

        // Wait for all workflows to complete
        for (String workflowId : workflowIdsTypeA) {
            waitForWorkflowCompletionStatic(workflowId, Duration.ofSeconds(30));
        }
        for (String workflowId : workflowIdsTypeB) {
            waitForWorkflowCompletionStatic(workflowId, Duration.ofSeconds(30));
        }
        log.info("All {} workflows completed", workflowIdsTypeA.size() + workflowIdsTypeB.size());

        // Wait a bit for search index to update (Temporal eventual consistency)
        Thread.sleep(2000);
    }

    // ==================== Search by Status Tests ====================

    @Test
    void testSearchByStatusCompleted() throws Exception {
        // Test: status = 'COMPLETED'
        String query = "status = 'COMPLETED'";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"), "Search response should have results");
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        // Should find our completed workflows
        Set<String> foundIds = extractWorkflowIds(resultList);

        // All our test workflows should be COMPLETED and found
        for (String workflowId : workflowIdsTypeA) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found in COMPLETED search");
        }
        for (String workflowId : workflowIdsTypeB) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found in COMPLETED search");
        }

        log.info("Search by status=COMPLETED found {} results, verified {} test workflows",
                resultList.size(), workflowIdsTypeA.size() + workflowIdsTypeB.size());
    }

    @Test
    void testSearchByStatusLegacySyntax() throws Exception {
        // Test: status:COMPLETED (legacy colon syntax)
        String query = "status:COMPLETED";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        // Should find our completed workflows
        Set<String> foundIds = extractWorkflowIds(resultList);

        // Verify at least one of our test workflows is found
        boolean foundAtLeastOne = false;
        for (String workflowId : workflowIdsTypeA) {
            if (foundIds.contains(workflowId)) {
                foundAtLeastOne = true;
                break;
            }
        }
        assertTrue(foundAtLeastOne, "Should find at least one test workflow with legacy syntax");

        log.info("Search by status:COMPLETED (legacy syntax) found {} results", resultList.size());
    }

    // ==================== Search by WorkflowType Tests ====================

    @Test
    void testSearchByWorkflowType() throws Exception {
        // Test: workflowType = 'workflow_name'
        String query = "workflowType = '" + WORKFLOW_TYPE_A + "'";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find all type A workflows
        for (String workflowId : workflowIdsTypeA) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found in workflowType search");
        }

        // Should NOT find type B workflows
        for (String workflowId : workflowIdsTypeB) {
            assertFalse(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " (type B) should NOT be found in type A search");
        }

        log.info("Search by workflowType={} found {} workflows (expected {})",
                WORKFLOW_TYPE_A, foundIds.size(), workflowIdsTypeA.size());
    }

    @Test
    void testSearchByWorkflowTypeLegacySyntax() throws Exception {
        // Test: workflowType:workflow_name (legacy colon syntax)
        String query = "workflowType:" + WORKFLOW_TYPE_B;
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find type B workflows
        for (String workflowId : workflowIdsTypeB) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found in workflowType search (legacy syntax)");
        }

        // Should NOT find type A workflows
        for (String workflowId : workflowIdsTypeA) {
            assertFalse(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " (type A) should NOT be found in type B search");
        }

        log.info("Search by workflowType:{} (legacy syntax) found {} workflows",
                WORKFLOW_TYPE_B, foundIds.size());
    }

    // ==================== Search with AND Conditions ====================

    @Test
    void testSearchWithAndCondition() throws Exception {
        // Test: workflowType = 'name' AND status = 'COMPLETED'
        String query = "workflowType = '" + WORKFLOW_TYPE_A + "' AND status = 'COMPLETED'";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find all type A workflows (they're all completed)
        for (String workflowId : workflowIdsTypeA) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found with AND condition");
        }

        // Should NOT find type B workflows
        for (String workflowId : workflowIdsTypeB) {
            assertFalse(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " (type B) should NOT be found in type A AND COMPLETED search");
        }

        // Verify all results have correct type and status
        for (JsonNode result : resultList) {
            String workflowType = result.get("workflowType").asText();
            String status = result.get("status").asText();
            assertEquals(WORKFLOW_TYPE_A, workflowType, "All results should have workflowType=" + WORKFLOW_TYPE_A);
            assertEquals("COMPLETED", status, "All results should have status=COMPLETED");
        }

        log.info("Search with AND condition found {} workflows", foundIds.size());
    }

    // ==================== Search with IN Clause ====================

    @Test
    void testSearchWithInClause() throws Exception {
        // Test: status IN (COMPLETED, FAILED)
        // All our test workflows are COMPLETED, so they should all be found
        String query = "status IN (COMPLETED, FAILED)";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // All test workflows should be found (they're all COMPLETED)
        for (String workflowId : workflowIdsTypeA) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found with IN clause");
        }
        for (String workflowId : workflowIdsTypeB) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found with IN clause");
        }

        log.info("Search with IN clause found {} results", resultList.size());
    }

    @Test
    void testSearchWithInClauseQuotedValues() throws Exception {
        // Test: status IN ('COMPLETED', 'RUNNING')
        String query = "status IN ('COMPLETED', 'RUNNING')";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find COMPLETED workflows
        for (String workflowId : workflowIdsTypeA) {
            assertTrue(foundIds.contains(workflowId),
                    "Workflow " + workflowId + " should be found with IN clause (quoted values)");
        }

        log.info("Search with IN clause (quoted values) found {} results", resultList.size());
    }

    // ==================== Search by CorrelationId ====================

    @Test
    void testSearchByCorrelationId() throws Exception {
        // Test: correlationId = 'some-id'
        String query = "correlationId = '" + CORRELATION_ID_1 + "'";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find exactly the workflow with correlation ID 1
        assertTrue(foundIds.contains(workflowIdWithCorrelation1),
                "Should find workflow with correlation ID 1");

        // Should NOT find workflow with different correlation ID
        assertFalse(foundIds.contains(workflowIdWithCorrelation2),
                "Should NOT find workflow with different correlation ID");

        // Note: WorkflowSummary may not include correlationId in all cases
        // The important verification is that the search query correctly filters by correlationId
        // which is proven by finding the expected workflow and not finding others

        log.info("Search by correlationId={} found {} workflow(s)", CORRELATION_ID_1, foundIds.size());
    }

    @Test
    void testSearchByCorrelationIdWithQuotedLegacyValue() throws Exception {
        // Note: Legacy colon syntax does NOT support hyphens in values
        // For values with hyphens, use quoted equality syntax instead
        // This test verifies that the quoted equality syntax works correctly
        String query = "correlationId = '" + CORRELATION_ID_2 + "'";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find exactly the workflow with correlation ID 2
        assertTrue(foundIds.contains(workflowIdWithCorrelation2),
                "Should find workflow with correlation ID 2 using quoted equality syntax");

        // Should NOT find workflow with different correlation ID
        assertFalse(foundIds.contains(workflowIdWithCorrelation1),
                "Should NOT find workflow with different correlation ID");

        log.info("Search by correlationId='{}' found {} workflow(s)",
                CORRELATION_ID_2, foundIds.size());
    }

    // ==================== Combined Complex Queries ====================

    @Test
    void testSearchComplexQuery() throws Exception {
        // Test: workflowType = 'name' AND correlationId = 'id'
        String query = "workflowType = '" + WORKFLOW_TYPE_A + "' AND correlationId = '" + CORRELATION_ID_1 + "'";
        JsonNode results = executeSearch(query);

        assertNotNull(results);
        assertTrue(results.has("results"));
        JsonNode resultList = results.get("results");
        assertTrue(resultList.isArray());

        Set<String> foundIds = extractWorkflowIds(resultList);

        // Should find exactly one workflow (type A with correlation ID 1)
        assertEquals(1, foundIds.size(), "Should find exactly one workflow matching complex query");
        assertTrue(foundIds.contains(workflowIdWithCorrelation1),
                "Should find the specific workflow matching both conditions");

        log.info("Complex query found {} workflow(s)", foundIds.size());
    }

    // ==================== Helper Methods ====================

    /**
     * Execute a search query via the REST API.
     */
    private JsonNode executeSearch(String query) throws Exception {
        log.info("Executing search query: {}", query);

        String response = conductorClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/workflow/search")
                        .queryParam("query", query)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(30));

        assertNotNull(response, "Search response should not be null");
        return OBJECT_MAPPER.readTree(response);
    }

    /**
     * Extract workflow IDs from search results.
     */
    private Set<String> extractWorkflowIds(JsonNode resultList) {
        Set<String> ids = new HashSet<>();
        for (JsonNode result : resultList) {
            if (result.has("workflowId")) {
                ids.add(result.get("workflowId").asText());
            }
        }
        return ids;
    }

    /**
     * Create a simple workflow definition (static version for BeforeAll).
     */
    private static com.netflix.conductor.common.metadata.workflow.WorkflowDef createSimpleWorkflowDefStatic(
            String workflowName, String taskName) {
        var workflowDef = new com.netflix.conductor.common.metadata.workflow.WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        var task = new com.netflix.conductor.common.metadata.workflow.WorkflowTask();
        task.setName(taskName);
        task.setTaskReferenceName(taskName + "_ref");
        task.setType(com.netflix.conductor.common.metadata.tasks.TaskType.SIMPLE.name());

        workflowDef.setTasks(Collections.singletonList(task));
        return workflowDef;
    }

    /**
     * Start a workflow (static version for BeforeAll).
     */
    private static String startWorkflowStatic(String workflowName, int version,
            Map<String, Object> input, String correlationId) throws Exception {
        Map<String, Object> request = new HashMap<>();
        request.put("name", workflowName);
        request.put("version", version);
        request.put("input", input != null ? input : Collections.emptyMap());
        if (correlationId != null) {
            request.put("correlationId", correlationId);
        }

        String json = OBJECT_MAPPER.writeValueAsString(request);
        String workflowId = conductorClient.post()
                .uri("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(json)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));

        // Remove quotes if present
        return workflowId != null ? workflowId.replace("\"", "") : null;
    }

    /**
     * Wait for workflow completion (static version for BeforeAll).
     */
    private static void waitForWorkflowCompletionStatic(String workflowId, Duration timeout) {
        await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    String json = conductorClient.get()
                            .uri("/api/workflow/{workflowId}", workflowId)
                            .retrieve()
                            .bodyToMono(String.class)
                            .block(Duration.ofSeconds(10));
                    JsonNode status = OBJECT_MAPPER.readTree(json);
                    String statusValue = status.get("status").asText();
                    return "COMPLETED".equals(statusValue)
                            || "FAILED".equals(statusValue)
                            || "TERMINATED".equals(statusValue);
                });
    }
}
