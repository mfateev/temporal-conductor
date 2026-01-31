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

package io.temporal.conductor.activity;

import io.temporal.activity.Activity;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic Activity implementation for executing Conductor tasks.
 *
 * <p>This implements DynamicActivity so that the Temporal activity type
 * equals the Conductor task name (e.g., "send_email", "process_order").
 * This provides better visibility in Temporal UI and enables per-task
 * metrics and configuration.
 *
 * <p>For POC purposes, this provides mock implementations of task execution.
 * In production, this would integrate with:
 * <ul>
 *   <li>Conductor worker polling (for SIMPLE tasks)</li>
 *   <li>HTTP client (for HTTP tasks)</li>
 *   <li>Other task-specific implementations</li>
 * </ul>
 */
public class TaskExecutionActivitiesImpl implements DynamicActivity {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionActivitiesImpl.class);

    @Override
    public Object execute(EncodedValues args) {
        String taskName = Activity.getExecutionContext().getInfo().getActivityType();
        String taskRefName = args.get(0, String.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> input = args.get(1, Map.class);

        logger.info("Executing task: {} (ref: {})", taskName, taskRefName);
        logger.debug("Task input: {}", input);

        if (taskName.toLowerCase().contains("http")) {
            return executeHttpTask(taskName, taskRefName, input);
        } else if (taskName.toLowerCase().contains("transform")) {
            return executeTransformTask(taskName, taskRefName, input);
        } else if (taskName.toLowerCase().contains("fail")) {
            return executeFailingTask(taskName, taskRefName, input);
        } else if (taskName.toLowerCase().contains("slow")) {
            return executeSlowTask(taskName, taskRefName, input);
        } else {
            return executeSimpleTask(taskName, taskRefName, input);
        }
    }

    private TaskExecutionResult executeSimpleTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input) {

        Map<String, Object> output = new HashMap<>();
        output.put("taskName", taskName);
        output.put("taskRefName", taskRefName);
        output.put("result", "completed");
        output.put("processedAt", System.currentTimeMillis());

        logger.info("Simple task completed: {}", taskRefName);
        return TaskExecutionResult.builder().output(output).build();
    }

    @SuppressWarnings("unchecked")
    private TaskExecutionResult executeHttpTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input) {
        String url = "http://example.com";
        if (input != null) {
            if (input.containsKey("url")) {
                url = (String) input.get("url");
            } else if (input.containsKey("http_request")) {
                Object httpRequest = input.get("http_request");
                if (httpRequest instanceof Map) {
                    Object uri = ((Map<String, Object>) httpRequest).get("uri");
                    if (uri instanceof String) {
                        url = (String) uri;
                    }
                }
            }
        }

        String method = input != null && input.containsKey("method")
                ? (String) input.get("method")
                : "GET";

        logger.info("Mock HTTP {} request to: {}", method, url);

        Map<String, Object> body = new HashMap<>();
        body.put("message", "Mock response from " + url);
        body.put("method", method);
        body.put("timestamp", System.currentTimeMillis());

        Map<String, Object> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        Map<String, Object> output = new HashMap<>();
        output.put("statusCode", 200);
        output.put("headers", headers);
        output.put("body", body);

        return TaskExecutionResult.builder().output(output).build();
    }

    private TaskExecutionResult executeTransformTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input) {
        Map<String, Object> transformed = new HashMap<>();
        transformed.put("original", input != null ? input : Collections.emptyMap());
        transformed.put("transformed", true);
        transformed.put("transformedAt", System.currentTimeMillis());

        logger.info("Transform task completed");
        return TaskExecutionResult.builder().output(transformed).build();
    }

    private TaskExecutionResult executeFailingTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input) {
        boolean shouldFail = true;
        if (input != null && input.containsKey("shouldFail")) {
            Object shouldFailValue = input.get("shouldFail");
            if (shouldFailValue instanceof Boolean) {
                shouldFail = (Boolean) shouldFailValue;
            }
        }

        if (shouldFail) {
            logger.warn("Task intentionally failing");
            return TaskExecutionResult.builder()
                    .output(Collections.emptyMap())
                    .status("FAILED")
                    .failureReason("Intentional failure for testing")
                    .build();
        } else {
            Map<String, Object> output = new HashMap<>();
            output.put("result", "recovered");
            return TaskExecutionResult.builder().output(output).build();
        }
    }

    private TaskExecutionResult executeSlowTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input) {
        long delayMs = 1000L;
        if (input != null && input.containsKey("delayMs")) {
            Object delayValue = input.get("delayMs");
            if (delayValue instanceof Number) {
                delayMs = ((Number) delayValue).longValue();
            }
        }

        logger.info("Slow task sleeping for {}ms", delayMs);
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskExecutionResult.builder()
                    .output(Collections.emptyMap())
                    .status("FAILED")
                    .failureReason("Task interrupted")
                    .build();
        }

        Map<String, Object> output = new HashMap<>();
        output.put("sleptFor", delayMs);
        output.put("completedAt", System.currentTimeMillis());

        return TaskExecutionResult.builder().output(output).build();
    }
}
