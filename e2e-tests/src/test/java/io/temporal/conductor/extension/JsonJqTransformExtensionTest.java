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

package io.temporal.conductor.extension;

import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import io.temporal.conductor.activity.ExtensionTaskExecutionActivityImpl;
import io.temporal.conductor.config.ExtensionTaskDiscoveryConfig;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the JSON_JQ_TRANSFORM extension task is auto-discovered
 * and can be executed via the extension activity.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = JsonJqTransformExtensionTest.TestConfig.class)
class JsonJqTransformExtensionTest {

    @Configuration
    @ComponentScan(basePackages = {
            "io.temporal.conductor.config",
            "com.netflix.conductor.tasks.json"  // JSON JQ task package
    })
    static class TestConfig {
    }

    @Autowired
    private Map<String, WorkflowSystemTask> extensionTasks;

    @Autowired
    private ExtensionTaskExecutionActivityImpl extensionTaskActivity;

    @Test
    void jsonJqTransformTaskIsDiscovered() {
        // Verify the JSON_JQ_TRANSFORM task is discovered
        assertTrue(extensionTasks.containsKey("JSON_JQ_TRANSFORM"),
                "JSON_JQ_TRANSFORM should be discovered. Found tasks: " + extensionTasks.keySet());
    }

    @Test
    void jsonJqTransformTaskCanExecute() {
        // Prepare input data with a simple JQ transformation
        Map<String, Object> inputData = new HashMap<>();

        // Input JSON to transform
        Map<String, Object> inputJson = new HashMap<>();
        inputJson.put("name", "John");
        inputJson.put("age", 30);
        inputJson.put("city", "New York");

        // JQ expression to extract specific fields
        String jqExpression = "{ name: .name, location: .city }";

        inputData.put("queryExpression", jqExpression);
        inputData.put("name", inputJson.get("name"));
        inputData.put("age", inputJson.get("age"));
        inputData.put("city", inputJson.get("city"));

        // Execute the extension task
        TaskExecutionResult result = extensionTaskActivity.execute(
                "JSON_JQ_TRANSFORM",
                "jq_transform_ref",
                inputData
        );

        // Verify execution completed
        assertEquals("COMPLETED", result.getStatus(),
                "Task should complete. Failure reason: " + result.getFailureReason());

        // Verify output contains transformed data
        assertNotNull(result.getOutput(), "Output should not be null");
    }

    @Test
    void jsonJqTransformWithArrayInput() {
        // Test with array input
        Map<String, Object> inputData = new HashMap<>();

        // Input array
        List<Map<String, Object>> items = List.of(
                Map.of("id", 1, "value", 10),
                Map.of("id", 2, "value", 20),
                Map.of("id", 3, "value", 30)
        );

        // JQ expression to sum values
        String jqExpression = "[.items[].value] | add";

        inputData.put("queryExpression", jqExpression);
        inputData.put("items", items);

        // Execute the extension task
        TaskExecutionResult result = extensionTaskActivity.execute(
                "JSON_JQ_TRANSFORM",
                "jq_array_ref",
                inputData
        );

        // Verify execution completed
        assertEquals("COMPLETED", result.getStatus(),
                "Task should complete. Failure reason: " + result.getFailureReason());
    }

    @Test
    void extensionActivityReportsErrorForInvalidJq() {
        // Test with invalid JQ expression
        Map<String, Object> inputData = new HashMap<>();
        inputData.put("queryExpression", "invalid[[[jq");
        inputData.put("data", "test");

        // Execute the extension task
        TaskExecutionResult result = extensionTaskActivity.execute(
                "JSON_JQ_TRANSFORM",
                "jq_invalid_ref",
                inputData
        );

        // Invalid JQ should fail
        assertEquals("FAILED", result.getStatus(),
                "Task should fail with invalid JQ expression");
        assertNotNull(result.getFailureReason(),
                "Failure reason should be provided");
    }
}
