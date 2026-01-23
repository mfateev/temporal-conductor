package io.temporal.conductor.api;

import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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
 * Tests for WorkflowResource REST endpoints.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("stub")
class WorkflowResourceTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void startWorkflow() throws Exception {
        mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\", \"input\": {\"key\": \"value\"}}"))
                .andExpect(status().isOk())
                .andExpect(content().string(notNullValue()));
    }

    @Test
    void startWorkflowByName() throws Exception {
        mockMvc.perform(post("/api/workflow/test-workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\": \"value\"}"))
                .andExpect(status().isOk())
                .andExpect(content().string(notNullValue()));
    }

    @Test
    void getWorkflow() throws Exception {
        // First start a workflow
        String workflowId = mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andReturn().getResponse().getContentAsString();

        // Then get it
        mockMvc.perform(get("/api/workflow/" + workflowId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.workflowId").value(workflowId))
                .andExpect(jsonPath("$.status").exists());
    }

    @Test
    void getWorkflowStatus() throws Exception {
        // First start a workflow
        String workflowId = mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andReturn().getResponse().getContentAsString();

        // Then get its status
        mockMvc.perform(get("/api/workflow/" + workflowId + "/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.workflowId").value(workflowId))
                .andExpect(jsonPath("$.status").exists());
    }

    @Test
    void searchWorkflows() throws Exception {
        mockMvc.perform(get("/api/workflow/search")
                        .param("start", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results").isArray())
                .andExpect(jsonPath("$.totalHits").isNumber());
    }

    @Test
    void searchWorkflowsV2() throws Exception {
        mockMvc.perform(get("/api/workflow/search-v2")
                        .param("start", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results").isArray())
                .andExpect(jsonPath("$.totalHits").isNumber());
    }

    @Test
    void pauseAndResumeWorkflow() throws Exception {
        String workflowId = mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andReturn().getResponse().getContentAsString();

        mockMvc.perform(put("/api/workflow/" + workflowId + "/pause"))
                .andExpect(status().isOk());

        mockMvc.perform(put("/api/workflow/" + workflowId + "/resume"))
                .andExpect(status().isOk());
    }

    @Test
    void terminateWorkflow() throws Exception {
        String workflowId = mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andReturn().getResponse().getContentAsString();

        mockMvc.perform(delete("/api/workflow/" + workflowId)
                        .param("reason", "Test termination"))
                .andExpect(status().isNoContent());
    }

    @Test
    void restartWorkflow() throws Exception {
        String workflowId = mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andReturn().getResponse().getContentAsString();

        mockMvc.perform(post("/api/workflow/" + workflowId + "/restart"))
                .andExpect(status().isOk())
                .andExpect(content().string(notNullValue()));
    }

    @Test
    void retryWorkflow() throws Exception {
        String workflowId = mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andReturn().getResponse().getContentAsString();

        mockMvc.perform(post("/api/workflow/" + workflowId + "/retry"))
                .andExpect(status().isOk());
    }

    @Test
    void getRunningWorkflows() throws Exception {
        // First start a workflow
        mockMvc.perform(post("/api/workflow")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\": \"test-workflow\"}"))
                .andExpect(status().isOk());

        // Then get running workflows by name
        mockMvc.perform(get("/api/workflow/running/test-workflow"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }
}
