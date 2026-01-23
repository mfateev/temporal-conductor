package io.temporal.conductor.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Tests for MetadataResource REST endpoints.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("stub")
class MetadataResourceTest {

    @Autowired
    private MockMvc mockMvc;

    // Workflow Definition Tests

    @Test
    void getAllWorkflowDefs() throws Exception {
        mockMvc.perform(get("/api/metadata/workflow"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].name").exists());
    }

    @Test
    void getWorkflowDefByName() throws Exception {
        mockMvc.perform(get("/api/metadata/workflow/order-processing"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("order-processing"))
                .andExpect(jsonPath("$.version").exists());
    }

    @Test
    void getWorkflowDefByNameAndVersion() throws Exception {
        mockMvc.perform(get("/api/metadata/workflow/order-processing")
                        .param("version", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("order-processing"))
                .andExpect(jsonPath("$.version").value(1));
    }

    @Test
    void createWorkflowDef() throws Exception {
        String workflowDef = """
                {
                    "name": "new-workflow",
                    "version": 1,
                    "description": "A new test workflow",
                    "ownerEmail": "test@example.com",
                    "timeoutSeconds": 3600,
                    "tasks": [
                        {
                            "name": "task1",
                            "taskReferenceName": "task1_ref",
                            "type": "SIMPLE"
                        }
                    ]
                }
                """;

        mockMvc.perform(post("/api/metadata/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(workflowDef))
                .andExpect(status().isNoContent());
    }

    @Test
    void updateWorkflowDefs() throws Exception {
        String workflowDefs = """
                [
                    {
                        "name": "updated-workflow",
                        "version": 2,
                        "description": "An updated workflow",
                        "ownerEmail": "test@example.com",
                        "timeoutSeconds": 7200,
                        "tasks": [
                            {
                                "name": "task1",
                                "taskReferenceName": "task1_ref",
                                "type": "SIMPLE"
                            }
                        ]
                    }
                ]
                """;

        mockMvc.perform(put("/api/metadata/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(workflowDefs))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteWorkflowDef() throws Exception {
        // Delete a workflow that was pre-initialized in the stub service
        mockMvc.perform(delete("/api/metadata/workflow/order-processing/1"))
                .andExpect(status().isNoContent());
    }

    // Task Definition Tests

    @Test
    void getAllTaskDefs() throws Exception {
        mockMvc.perform(get("/api/metadata/taskdefs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].name").exists());
    }

    @Test
    void getTaskDefByType() throws Exception {
        mockMvc.perform(get("/api/metadata/taskdefs/simple_task_1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("simple_task_1"));
    }

    @Test
    void registerTaskDefs() throws Exception {
        String taskDefs = """
                [
                    {
                        "name": "new_task_1",
                        "description": "A new task",
                        "retryCount": 3,
                        "timeoutSeconds": 60
                    },
                    {
                        "name": "new_task_2",
                        "description": "Another new task",
                        "retryCount": 2,
                        "timeoutSeconds": 120
                    }
                ]
                """;

        mockMvc.perform(post("/api/metadata/taskdefs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(taskDefs))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteTaskDef() throws Exception {
        // Delete a task that was pre-initialized in the stub service
        mockMvc.perform(delete("/api/metadata/taskdefs/simple_task_1"))
                .andExpect(status().isNoContent());
    }
}
