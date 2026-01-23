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

package io.temporal.conductor.service.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.conductor.workflow.DefinitionWorkflow;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for TemporalMetadataService.
 *
 * <p>These tests verify the metadata service correctly interacts with Temporal workflows for
 * storing definitions. The tests use mocks to avoid needing a running Temporal server.
 */
@ExtendWith(MockitoExtension.class)
class TemporalMetadataServiceTest {

    @Mock
    private WorkflowClient workflowClient;

    @Mock
    private WorkflowServiceStubs workflowServiceStubs;

    @Mock
    private WorkflowStub workflowStub;

    @Mock
    private DefinitionWorkflow definitionWorkflow;

    private ObjectMapper objectMapper;
    private TemporalMetadataService metadataService;

    private WorkflowNotFoundException createWorkflowNotFoundException(String workflowId) {
        WorkflowExecution execution = WorkflowExecution.newBuilder()
                .setWorkflowId(workflowId)
                .setRunId("test-run-id")
                .build();
        return new WorkflowNotFoundException(execution, "DefinitionWorkflow", null);
    }

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        lenient().when(workflowClient.getWorkflowServiceStubs()).thenReturn(workflowServiceStubs);
        metadataService = new TemporalMetadataService(workflowClient, objectMapper,
                "conductor-workflows", "conductor");
    }

    // Workflow Definition Tests

    @Test
    void testRegisterWorkflowDef() throws Exception {
        WorkflowDef workflowDef = createWorkflowDef("test-workflow", 1);

        when(workflowClient.newWorkflowStub(eq(DefinitionWorkflow.class), any(WorkflowOptions.class)))
                .thenReturn(definitionWorkflow);
        doNothing().when(definitionWorkflow).updateDefinition(anyString());

        metadataService.registerWorkflowDef(workflowDef);

        verify(workflowClient).newWorkflowStub(eq(DefinitionWorkflow.class),
                any(WorkflowOptions.class));
    }

    @Test
    void testGetWorkflowDefNotFoundReturnsNull() {
        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        when(workflowStub.query("getDefinitionJson", String.class))
                .thenThrow(createWorkflowNotFoundException("test-workflow-id"));

        WorkflowDef result = metadataService.getWorkflowDef("non-existent", 1);
        assertNull(result);
    }

    @Test
    void testGetWorkflowDefFromCache() throws Exception {
        WorkflowDef workflowDef = createWorkflowDef("cached-workflow", 1);
        String json = objectMapper.writeValueAsString(workflowDef);

        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        when(workflowStub.query("getDefinitionJson", String.class)).thenReturn(json);

        // First call - should query Temporal
        WorkflowDef result1 = metadataService.getWorkflowDef("cached-workflow", 1);
        assertNotNull(result1);
        assertEquals("cached-workflow", result1.getName());

        // Second call - should return from cache
        WorkflowDef result2 = metadataService.getWorkflowDef("cached-workflow", 1);
        assertNotNull(result2);
        assertEquals("cached-workflow", result2.getName());
    }

    @Test
    void testDeleteWorkflowDef() {
        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        doNothing().when(workflowStub).terminate(anyString());

        metadataService.deleteWorkflowDef("delete-test", 1);

        verify(workflowStub).terminate("Definition deleted");
    }

    @Test
    void testDeleteNonExistentWorkflowDef() {
        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        doThrow(createWorkflowNotFoundException("non-existent-id"))
                .when(workflowStub).terminate(anyString());

        // Should not throw
        metadataService.deleteWorkflowDef("non-existent", 1);
    }

    // Task Definition Tests

    @Test
    void testRegisterTaskDef() throws Exception {
        TaskDef taskDef = createTaskDef("simple-task");

        when(workflowClient.newWorkflowStub(eq(DefinitionWorkflow.class), any(WorkflowOptions.class)))
                .thenReturn(definitionWorkflow);
        doNothing().when(definitionWorkflow).updateDefinition(anyString());

        metadataService.registerTaskDefs(List.of(taskDef));

        verify(workflowClient).newWorkflowStub(eq(DefinitionWorkflow.class),
                any(WorkflowOptions.class));
    }

    @Test
    void testGetTaskDefNotFoundReturnsNull() {
        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        when(workflowStub.query("getDefinitionJson", String.class))
                .thenThrow(createWorkflowNotFoundException("task-def-id"));

        TaskDef result = metadataService.getTaskDef("non-existent");
        assertNull(result);
    }

    @Test
    void testGetTaskDefFromCache() throws Exception {
        TaskDef taskDef = createTaskDef("cached-task");
        String json = objectMapper.writeValueAsString(taskDef);

        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        when(workflowStub.query("getDefinitionJson", String.class)).thenReturn(json);

        // First call - should query Temporal
        TaskDef result1 = metadataService.getTaskDef("cached-task");
        assertNotNull(result1);
        assertEquals("cached-task", result1.getName());

        // Second call - should return from cache
        TaskDef result2 = metadataService.getTaskDef("cached-task");
        assertNotNull(result2);
        assertEquals("cached-task", result2.getName());
    }

    @Test
    void testDeleteTaskDef() {
        when(workflowClient.newUntypedWorkflowStub(anyString())).thenReturn(workflowStub);
        doNothing().when(workflowStub).terminate(anyString());

        metadataService.deleteTaskDef("delete-task");

        verify(workflowStub).terminate("Definition deleted");
    }

    // Helper methods

    private WorkflowDef createWorkflowDef(String name, int version) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(version);
        def.setDescription("Test workflow: " + name + " v" + version);
        return def;
    }

    private TaskDef createTaskDef(String name) {
        TaskDef def = new TaskDef();
        def.setName(name);
        def.setDescription("Test task: " + name);
        def.setRetryCount(3);
        def.setTimeoutSeconds(60);
        return def;
    }
}
