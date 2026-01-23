package io.temporal.conductor.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Tests for EventResource REST endpoints.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("stub")
class EventResourceTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void getWorkflowEvents() throws Exception {
        String workflowId = "test-workflow-123";

        mockMvc.perform(get("/api/events/workflow/" + workflowId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].workflowId").value(workflowId))
                .andExpect(jsonPath("$[0].eventType").exists())
                .andExpect(jsonPath("$[0].timestamp").exists())
                .andExpect(jsonPath("$[0].eventId").exists());
    }

    @Test
    void getWorkflowEventsWithPagination() throws Exception {
        String workflowId = "test-workflow-456";

        // Get first 2 events
        mockMvc.perform(get("/api/events/workflow/" + workflowId)
                        .param("start", "0")
                        .param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2));

        // Get next 2 events
        mockMvc.perform(get("/api/events/workflow/" + workflowId)
                        .param("start", "2")
                        .param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void getWorkflowEventsEmptyPage() throws Exception {
        String workflowId = "test-workflow-789";

        // Request a page beyond the available events
        mockMvc.perform(get("/api/events/workflow/" + workflowId)
                        .param("start", "1000")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getTaskEvents() throws Exception {
        String taskId = "test-workflow-abc-task-1";

        mockMvc.perform(get("/api/events/task/" + taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].taskId").value(taskId))
                .andExpect(jsonPath("$[0].eventType").exists())
                .andExpect(jsonPath("$[0].timestamp").exists())
                .andExpect(jsonPath("$[0].eventId").exists());
    }

    @Test
    void getWorkflowEventsContainsExpectedEventTypes() throws Exception {
        String workflowId = "test-workflow-event-types";

        mockMvc.perform(get("/api/events/workflow/" + workflowId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                // First event should be STARTED
                .andExpect(jsonPath("$[0].eventType").value("STARTED"))
                // Should have task events
                .andExpect(jsonPath("$[1].eventType").value("TASK_STARTED"));
    }

    @Test
    void getWorkflowEventsHasPayload() throws Exception {
        String workflowId = "test-workflow-payload";

        mockMvc.perform(get("/api/events/workflow/" + workflowId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].payload").exists())
                .andExpect(jsonPath("$[0].payload.workflowType").exists());
    }
}
