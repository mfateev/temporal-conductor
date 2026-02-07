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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for Metadata CRUD operations (workflow and task definitions).
 * Tests the /api/metadata REST endpoints against real Temporal server.
 */
class MetadataCrudE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(MetadataCrudE2ETest.class);

    // ==================== Workflow Definition CRUD Tests ====================

    @Test
    void testWorkflowDefinitionCreate() throws Exception {
        String workflowName = "metadata_crud_create_wf_" + System.currentTimeMillis();
        String taskName = "metadata_crud_create_task";

        // Create task definition first
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Create workflow definition via POST
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        workflowDef.setDescription("Test workflow for CRUD create operation");
        registerWorkflowDef(workflowDef);
        log.info("Created workflow definition: {}", workflowName);

        // Verify by retrieving it
        WorkflowDef retrieved = getWorkflowDef(workflowName, 1);
        assertNotNull(retrieved);
        assertEquals(workflowName, retrieved.getName());
        assertEquals(1, retrieved.getVersion());
        assertEquals("Test workflow for CRUD create operation", retrieved.getDescription());
        assertEquals(1, retrieved.getTasks().size());
        assertEquals(taskName, retrieved.getTasks().get(0).getName());
    }

    @Test
    void testWorkflowDefinitionGet() throws Exception {
        String workflowName = "metadata_crud_get_wf_" + System.currentTimeMillis();
        String taskName = "metadata_crud_get_task";

        // Setup
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Get specific version
        WorkflowDef retrieved = getWorkflowDef(workflowName, 1);
        assertNotNull(retrieved);
        assertEquals(workflowName, retrieved.getName());
        assertEquals(1, retrieved.getVersion());
    }

    @Test
    void testWorkflowDefinitionGetLatest() throws Exception {
        String workflowName = "metadata_crud_latest_wf_" + System.currentTimeMillis();
        String taskName = "metadata_crud_latest_task";

        // Setup task
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Create version 1
        WorkflowDef v1 = createSimpleWorkflowDef(workflowName, taskName);
        v1.setDescription("Version 1");
        registerWorkflowDef(v1);
        log.info("Registered version 1");

        // Create version 2 - explicitly set version before registration
        WorkflowDef v2 = createSimpleWorkflowDef(workflowName, taskName);
        v2.setVersion(2);
        v2.setDescription("Version 2 - Latest");
        registerWorkflowDef(v2);
        log.info("Registered version 2");

        // Get without version - should return latest version registered
        WorkflowDef latest = getWorkflowDefLatest(workflowName);
        assertNotNull(latest);
        assertEquals(workflowName, latest.getName());
        // The latest version should be the highest version we registered
        assertTrue(latest.getVersion() >= 1, "Should have a valid version");
        log.info("Got latest version: {} with description: {}", latest.getVersion(), latest.getDescription());
        // Since we registered v2 last, it should be the latest
        assertEquals(2, latest.getVersion(), "Latest version should be 2");
    }

    @Test
    void testWorkflowDefinitionList() throws Exception {
        String workflowName1 = "metadata_crud_list_wf1_" + System.currentTimeMillis();
        String workflowName2 = "metadata_crud_list_wf2_" + System.currentTimeMillis();
        String taskName = "metadata_crud_list_task";

        // Setup
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName1, taskName));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName2, taskName));

        // Wait for both workflows to be visible in the list (eventual consistency)
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    List<WorkflowDef> allDefs = getAllWorkflowDefs();
                    assertNotNull(allDefs);
                    assertFalse(allDefs.isEmpty());

                    boolean foundWf1 = allDefs.stream().anyMatch(d -> workflowName1.equals(d.getName()));
                    boolean foundWf2 = allDefs.stream().anyMatch(d -> workflowName2.equals(d.getName()));
                    assertTrue(foundWf1, "Should find first workflow in list");
                    assertTrue(foundWf2, "Should find second workflow in list");
                    log.info("Found {} workflow definitions in total", allDefs.size());
                });
    }

    @Test
    void testWorkflowDefinitionUpdate() throws Exception {
        String workflowName = "metadata_crud_update_wf_" + System.currentTimeMillis();
        String taskName1 = "metadata_crud_update_task1";
        String taskName2 = "metadata_crud_update_task2";

        // Setup
        registerTaskDefs(createTaskDefs(List.of(taskName1, taskName2)));

        // Create initial workflow
        WorkflowDef initial = createSimpleWorkflowDef(workflowName, taskName1);
        initial.setDescription("Initial description");
        registerWorkflowDef(initial);

        // Verify initial state
        WorkflowDef retrieved = getWorkflowDef(workflowName, 1);
        assertEquals("Initial description", retrieved.getDescription());
        assertEquals(taskName1, retrieved.getTasks().get(0).getName());

        // Update workflow via PUT
        WorkflowDef updated = createSimpleWorkflowDef(workflowName, taskName2);
        updated.setDescription("Updated description");
        updateWorkflowDefs(List.of(updated));
        log.info("Updated workflow definition: {}", workflowName);

        // Verify update
        WorkflowDef afterUpdate = getWorkflowDef(workflowName, 1);
        assertEquals("Updated description", afterUpdate.getDescription());
        assertEquals(taskName2, afterUpdate.getTasks().get(0).getName());
    }

    @Test
    void testWorkflowDefinitionDelete() throws Exception {
        String workflowName = "metadata_crud_delete_wf_" + System.currentTimeMillis();
        String taskName = "metadata_crud_delete_task";

        // Setup
        registerTaskDefs(createTaskDefs(List.of(taskName)));
        registerWorkflowDef(createSimpleWorkflowDef(workflowName, taskName));

        // Verify it exists
        WorkflowDef retrieved = getWorkflowDef(workflowName, 1);
        assertNotNull(retrieved);
        assertEquals(workflowName, retrieved.getName());

        // Delete
        deleteWorkflowDef(workflowName, 1);
        log.info("Deleted workflow definition: {} version 1", workflowName);

        // Verify deletion - should get error or null when trying to fetch
        try {
            WorkflowDef deleted = getWorkflowDef(workflowName, 1);
            // If we get here, the response should be null (depends on API behavior)
            assertNull(deleted, "Deleted workflow should not be retrievable");
        } catch (Exception e) {
            // Expected - either WebClientResponseException (404) or other error
            log.info("Expected error getting deleted workflow: {}", e.getClass().getSimpleName());
        }
    }

    @Test
    void testWorkflowDefinitionVersioning() throws Exception {
        String workflowName = "metadata_crud_versioning_wf_" + System.currentTimeMillis();
        String taskName1 = "metadata_crud_v1_task";
        String taskName2 = "metadata_crud_v2_task";

        // Setup tasks
        registerTaskDefs(createTaskDefs(List.of(taskName1, taskName2)));

        // Create version 1
        WorkflowDef v1 = createSimpleWorkflowDef(workflowName, taskName1);
        v1.setVersion(1);
        v1.setDescription("First version");
        registerWorkflowDef(v1);
        log.info("Created workflow version 1: {}", workflowName);

        // Create version 2
        WorkflowDef v2 = createSimpleWorkflowDef(workflowName, taskName2);
        v2.setVersion(2);
        v2.setDescription("Second version");
        registerWorkflowDef(v2);
        log.info("Created workflow version 2: {}", workflowName);

        // Verify both versions retrievable
        WorkflowDef retrievedV1 = getWorkflowDef(workflowName, 1);
        WorkflowDef retrievedV2 = getWorkflowDef(workflowName, 2);

        assertNotNull(retrievedV1);
        assertNotNull(retrievedV2);
        assertEquals(1, retrievedV1.getVersion());
        assertEquals(2, retrievedV2.getVersion());
        assertEquals("First version", retrievedV1.getDescription());
        assertEquals("Second version", retrievedV2.getDescription());
        assertEquals(taskName1, retrievedV1.getTasks().get(0).getName());
        assertEquals(taskName2, retrievedV2.getTasks().get(0).getName());
    }

    @Test
    void testStartWorkflowWithSpecificVersion() throws Exception {
        String workflowName = "metadata_crud_start_version_wf_" + System.currentTimeMillis();
        String taskNameV1 = "metadata_crud_start_v1_task";
        String taskNameV2 = "metadata_crud_start_v2_task";

        // Setup tasks
        registerTaskDefs(createTaskDefs(List.of(taskNameV1, taskNameV2)));

        // Create version 1
        WorkflowDef v1 = createSimpleWorkflowDef(workflowName, taskNameV1);
        v1.setVersion(1);
        registerWorkflowDef(v1);

        // Create version 2
        WorkflowDef v2 = createSimpleWorkflowDef(workflowName, taskNameV2);
        v2.setVersion(2);
        registerWorkflowDef(v2);

        // Start workflow with version 1
        String workflowIdV1 = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow version 1: {}", workflowIdV1);
        WorkflowStatusResponse statusV1 = waitForWorkflowCompletion(workflowIdV1, Duration.ofSeconds(30));
        assertEquals("COMPLETED", statusV1.getStatus());
        assertEquals(1, statusV1.getVersion());

        // Verify task from version 1 was executed
        assertNotNull(statusV1.getTasks());
        assertEquals(1, statusV1.getTasks().size());
        assertEquals(taskNameV1 + "_ref", statusV1.getTasks().get(0).getReferenceTaskName());

        // Start workflow with version 2
        String workflowIdV2 = startWorkflow(workflowName, 2, Collections.emptyMap());
        log.info("Started workflow version 2: {}", workflowIdV2);
        WorkflowStatusResponse statusV2 = waitForWorkflowCompletion(workflowIdV2, Duration.ofSeconds(30));
        assertEquals("COMPLETED", statusV2.getStatus());
        assertEquals(2, statusV2.getVersion());

        // Verify task from version 2 was executed
        assertNotNull(statusV2.getTasks());
        assertEquals(1, statusV2.getTasks().size());
        assertEquals(taskNameV2 + "_ref", statusV2.getTasks().get(0).getReferenceTaskName());
    }

    // ==================== Task Definition CRUD Tests ====================

    @Test
    void testTaskDefinitionCreate() throws Exception {
        String taskType = "metadata_crud_create_taskdef_" + System.currentTimeMillis();

        // Create task definition
        TaskDef taskDef = new TaskDef();
        taskDef.setName(taskType);
        taskDef.setDescription("Test task definition for CRUD create");
        taskDef.setRetryCount(3);
        taskDef.setTimeoutSeconds(120);
        taskDef.setResponseTimeoutSeconds(60);

        registerTaskDefs(List.of(taskDef));
        log.info("Created task definition: {}", taskType);

        // Verify by retrieving it
        TaskDef retrieved = getTaskDef(taskType);
        assertNotNull(retrieved);
        assertEquals(taskType, retrieved.getName());
        assertEquals("Test task definition for CRUD create", retrieved.getDescription());
        assertEquals(3, retrieved.getRetryCount());
        assertEquals(120, retrieved.getTimeoutSeconds());
        assertEquals(60, retrieved.getResponseTimeoutSeconds());
    }

    @Test
    void testTaskDefinitionGet() throws Exception {
        String taskType = "metadata_crud_get_taskdef_" + System.currentTimeMillis();

        // Setup
        TaskDef taskDef = new TaskDef();
        taskDef.setName(taskType);
        taskDef.setDescription("Get test");
        registerTaskDefs(List.of(taskDef));

        // Get task definition
        TaskDef retrieved = getTaskDef(taskType);
        assertNotNull(retrieved);
        assertEquals(taskType, retrieved.getName());
        assertEquals("Get test", retrieved.getDescription());
    }

    @Test
    void testTaskDefinitionList() throws Exception {
        String taskType1 = "metadata_crud_list_taskdef1_" + System.currentTimeMillis();
        String taskType2 = "metadata_crud_list_taskdef2_" + System.currentTimeMillis();

        // Setup
        registerTaskDefs(createTaskDefs(List.of(taskType1, taskType2)));

        // Get all task definitions
        List<TaskDef> allDefs = getAllTaskDefs();
        assertNotNull(allDefs);
        assertFalse(allDefs.isEmpty());

        // Verify our task definitions are in the list
        boolean foundTask1 = allDefs.stream().anyMatch(d -> taskType1.equals(d.getName()));
        boolean foundTask2 = allDefs.stream().anyMatch(d -> taskType2.equals(d.getName()));
        assertTrue(foundTask1, "Should find first task definition in list");
        assertTrue(foundTask2, "Should find second task definition in list");
        log.info("Found {} task definitions in total", allDefs.size());
    }

    @Test
    void testTaskDefinitionUpdate() throws Exception {
        String taskType = "metadata_crud_update_taskdef_" + System.currentTimeMillis();

        // Create initial task definition
        TaskDef initial = new TaskDef();
        initial.setName(taskType);
        initial.setDescription("Initial description");
        initial.setRetryCount(1);
        registerTaskDefs(List.of(initial));

        // Verify initial state
        TaskDef retrieved = getTaskDef(taskType);
        assertEquals("Initial description", retrieved.getDescription());
        assertEquals(1, retrieved.getRetryCount());

        // Update via POST (the API uses POST for both create and update for task defs)
        TaskDef updated = new TaskDef();
        updated.setName(taskType);
        updated.setDescription("Updated description");
        updated.setRetryCount(5);
        registerTaskDefs(List.of(updated));
        log.info("Updated task definition: {}", taskType);

        // Verify update
        TaskDef afterUpdate = getTaskDef(taskType);
        assertEquals("Updated description", afterUpdate.getDescription());
        assertEquals(5, afterUpdate.getRetryCount());
    }

    @Test
    void testTaskDefinitionDelete() throws Exception {
        String taskType = "metadata_crud_delete_taskdef_" + System.currentTimeMillis();

        // Setup
        registerTaskDefs(createTaskDefs(List.of(taskType)));

        // Verify it exists
        TaskDef retrieved = getTaskDef(taskType);
        assertNotNull(retrieved);
        assertEquals(taskType, retrieved.getName());

        // Delete
        deleteTaskDef(taskType);
        log.info("Deleted task definition: {}", taskType);

        // Verify deletion - should get error or null when trying to fetch
        try {
            TaskDef deleted = getTaskDef(taskType);
            // If we get here, the response should be null (depends on API behavior)
            assertNull(deleted, "Deleted task definition should not be retrievable");
        } catch (Exception e) {
            // Expected - either WebClientResponseException (404) or other error
            log.info("Expected error getting deleted task definition: {}", e.getClass().getSimpleName());
        }
    }

    @Test
    void testTaskDefinitionBatchCreate() throws Exception {
        String taskType1 = "metadata_crud_batch1_" + System.currentTimeMillis();
        String taskType2 = "metadata_crud_batch2_" + System.currentTimeMillis();
        String taskType3 = "metadata_crud_batch3_" + System.currentTimeMillis();

        // Create multiple task definitions in batch
        List<TaskDef> taskDefs = new ArrayList<>();
        for (String taskType : List.of(taskType1, taskType2, taskType3)) {
            TaskDef taskDef = new TaskDef();
            taskDef.setName(taskType);
            taskDef.setDescription("Batch created: " + taskType);
            taskDefs.add(taskDef);
        }
        registerTaskDefs(taskDefs);
        log.info("Batch created {} task definitions", taskDefs.size());

        // Verify all were created
        for (String taskType : List.of(taskType1, taskType2, taskType3)) {
            TaskDef retrieved = getTaskDef(taskType);
            assertNotNull(retrieved);
            assertEquals(taskType, retrieved.getName());
            assertEquals("Batch created: " + taskType, retrieved.getDescription());
        }
    }

    // ==================== Helper Methods for Metadata API ====================

    /**
     * Get workflow definition by name and version.
     */
    private WorkflowDef getWorkflowDef(String name, int version) {
        String response = conductorClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/metadata/workflow/{name}")
                        .queryParam("version", version)
                        .build(name))
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        try {
            return OBJECT_MAPPER.readValue(response, WorkflowDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse workflow definition", e);
        }
    }

    /**
     * Get latest workflow definition by name (without version).
     */
    private WorkflowDef getWorkflowDefLatest(String name) {
        String response = conductorClient.get()
                .uri("/api/metadata/workflow/{name}", name)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        try {
            return OBJECT_MAPPER.readValue(response, WorkflowDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse workflow definition", e);
        }
    }

    /**
     * Get all workflow definitions.
     */
    private List<WorkflowDef> getAllWorkflowDefs() {
        String response = conductorClient.get()
                .uri("/api/metadata/workflow")
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        try {
            return OBJECT_MAPPER.readValue(response, new TypeReference<List<WorkflowDef>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse workflow definitions list", e);
        }
    }

    /**
     * Update workflow definitions via PUT.
     */
    private void updateWorkflowDefs(List<WorkflowDef> workflowDefs) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(workflowDefs);
            conductorClient.put()
                    .uri("/api/metadata/workflow")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(json)
                    .retrieve()
                    .toBodilessEntity()
                    .block(Duration.ofSeconds(10));
            log.info("Updated {} workflow definitions", workflowDefs.size());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize workflow definitions", e);
        }
    }

    /**
     * Delete workflow definition by name and version.
     */
    private void deleteWorkflowDef(String name, int version) {
        conductorClient.delete()
                .uri("/api/metadata/workflow/{name}/{version}", name, version)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
    }

    /**
     * Get task definition by type.
     */
    private TaskDef getTaskDef(String taskType) {
        String response = conductorClient.get()
                .uri("/api/metadata/taskdefs/{taskType}", taskType)
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        try {
            return OBJECT_MAPPER.readValue(response, TaskDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse task definition", e);
        }
    }

    /**
     * Get all task definitions.
     */
    private List<TaskDef> getAllTaskDefs() {
        String response = conductorClient.get()
                .uri("/api/metadata/taskdefs")
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        try {
            return OBJECT_MAPPER.readValue(response, new TypeReference<List<TaskDef>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse task definitions list", e);
        }
    }

    /**
     * Delete task definition by type.
     */
    private void deleteTaskDef(String taskType) {
        conductorClient.delete()
                .uri("/api/metadata/taskdefs/{taskType}", taskType)
                .retrieve()
                .toBodilessEntity()
                .block(Duration.ofSeconds(10));
    }
}
