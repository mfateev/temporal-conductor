package io.temporal.conductor.api;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Integration tests for OpenAPI/Swagger UI endpoints.
 * Verifies that API documentation is properly generated and accessible.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("stub")
class OpenApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void apiDocsEndpointReturnsValidJson() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    void apiDocsContainsCorrectApiInfo() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.openapi").exists())
                .andExpect(jsonPath("$.info.title").value("Conductor Server (Temporal Backend)"))
                .andExpect(jsonPath("$.info.version").value("1.0.0"))
                .andExpect(jsonPath("$.info.description")
                        .value("Netflix Conductor-compatible API powered by Temporal"));
    }

    @Test
    void apiDocsContainsWorkflowPaths() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/workflow']").exists())
                .andExpect(jsonPath("$.paths['/api/workflow/{workflowId}']").exists())
                .andExpect(jsonPath("$.paths['/api/workflow/{workflowId}/status']").exists())
                .andExpect(jsonPath("$.paths['/api/workflow/search']").exists());
    }

    @Test
    void apiDocsContainsTaskPaths() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/tasks/{taskId}']").exists())
                .andExpect(jsonPath("$.paths['/api/tasks/search']").exists())
                .andExpect(jsonPath("$.paths['/api/tasks/queue/poll/{taskType}']").exists());
    }

    @Test
    void apiDocsContainsMetadataPaths() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/metadata/workflow']").exists())
                .andExpect(jsonPath("$.paths['/api/metadata/workflow/{name}']").exists())
                .andExpect(jsonPath("$.paths['/api/metadata/taskdefs']").exists())
                .andExpect(jsonPath("$.paths['/api/metadata/taskdefs/{taskType}']").exists());
    }

    @Test
    void apiDocsContainsEventPaths() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/events/workflow/{workflowId}']").exists())
                .andExpect(jsonPath("$.paths['/api/events/task/{taskId}']").exists());
    }

    @Test
    void apiDocsContainsAllTags() throws Exception {
        mockMvc.perform(get("/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tags[*].name",
                        hasItems("Workflow", "Task", "Metadata", "Events")));
    }

    @Test
    void swaggerUiHtmlRedirects() throws Exception {
        mockMvc.perform(get("/swagger-ui.html"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/swagger-ui/index.html"));
    }

    @Test
    void swaggerUiIndexIsAccessible() throws Exception {
        mockMvc.perform(get("/swagger-ui/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("text/html"))
                .andExpect(content().string(containsString("swagger-ui")));
    }
}
