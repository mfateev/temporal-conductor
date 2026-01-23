package io.temporal.conductor.api;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
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
 * Tests for TaskResource REST endpoints.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("stub")
class TaskResourceTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void getTask() throws Exception {
        String taskId = "test-task-123";

        mockMvc.perform(get("/api/tasks/" + taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.taskId").value(taskId))
                .andExpect(jsonPath("$.taskType").exists())
                .andExpect(jsonPath("$.status").exists());
    }

    @Test
    void searchTasks() throws Exception {
        mockMvc.perform(get("/api/tasks/search")
                        .param("start", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results").isArray())
                .andExpect(jsonPath("$.totalHits").isNumber());
    }

    @Test
    void searchTasksWithQuery() throws Exception {
        mockMvc.perform(get("/api/tasks/search")
                        .param("start", "0")
                        .param("size", "5")
                        .param("freeText", "sample")
                        .param("query", "status=COMPLETED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results").isArray())
                .andExpect(jsonPath("$.totalHits").isNumber());
    }

    @Test
    void addAndGetTaskLogs() throws Exception {
        String taskId = "test-task-logs-456";

        // Add a log entry
        mockMvc.perform(post("/api/tasks/" + taskId + "/log")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("\"Test log entry from unit test\""))
                .andExpect(status().isOk());

        // Get task logs
        mockMvc.perform(get("/api/tasks/" + taskId + "/log"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$", hasSize(greaterThan(0))))
                .andExpect(jsonPath("$[0].taskId").value(taskId));
    }

    @Test
    void pollTask() throws Exception {
        String taskType = "test_task_type";

        mockMvc.perform(post("/api/tasks/queue/poll/" + taskType)
                        .param("workerId", "worker-1")
                        .param("domain", "test-domain"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.taskId").exists())
                .andExpect(jsonPath("$.taskType").value(taskType))
                .andExpect(jsonPath("$.status").value("IN_PROGRESS"));
    }

    @Test
    void updateTask() throws Exception {
        String taskId = "test-update-task-789";

        // Update the task as completed using TaskResult structure
        // TaskResult from Conductor uses different field names
        String taskResult = String.format(
                "{\"taskId\": \"%s\", \"workflowInstanceId\": \"wf-123\", "
                        + "\"status\": \"COMPLETED\", \"outputData\": {\"result\": \"success\"}}",
                taskId);

        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(taskResult))
                .andExpect(status().isOk())
                .andExpect(content().string(taskId));
    }
}
