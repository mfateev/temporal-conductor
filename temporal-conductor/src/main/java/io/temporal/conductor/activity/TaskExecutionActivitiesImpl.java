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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Dynamic Activity implementation for executing SIMPLE Conductor tasks.
 *
 * <p>This implements DynamicActivity so that the Temporal activity type
 * equals the Conductor task name (e.g., "send_email", "process_order").
 * This provides better visibility in Temporal UI and enables per-task
 * metrics and configuration.
 *
 * <p>IMPORTANT: This activity only handles SIMPLE worker tasks. Extension
 * task types (JSON_JQ_TRANSFORM, HTTP, KAFKA_PUBLISH, etc.) must be handled
 * by the appropriate Conductor extension. If an extension task type reaches
 * this activity, it means the extension is not configured and will fail.
 *
 * <p>For POC purposes, this provides stub implementations for SIMPLE tasks.
 * In production, this would integrate with Conductor worker polling.
 */
@Component
public class TaskExecutionActivitiesImpl implements DynamicActivity {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionActivitiesImpl.class);

    /**
     * Known Conductor extension task types that require extension dependencies.
     * If these types reach this activity, the extension is not configured.
     */
    private static final Set<String> EXTENSION_TASK_TYPES = Set.of(
            "JSON_JQ_TRANSFORM",
            "HTTP",
            "KAFKA_PUBLISH",
            "AMQP_PUBLISH",
            "SQS",
            "LAMBDA",
            "INLINE"
    );

    @Override
    public Object execute(EncodedValues args) {
        String activityType = Activity.getExecutionContext().getInfo().getActivityType();
        String taskRefName = args.get(0, String.class);
        String conductorTaskType = args.get(1, String.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> input = args.get(2, Map.class);

        logger.info("Executing task: {} (ref: {}, conductorType: {})", activityType, taskRefName, conductorTaskType);
        logger.debug("Task input: {}", input);

        // Check for extension task types - these should be handled by extensions
        if (EXTENSION_TASK_TYPES.contains(conductorTaskType)) {
            logger.error("Extension task type '{}' is not configured", conductorTaskType);
            return TaskExecutionResult.builder()
                    .output(Collections.emptyMap())
                    .status("FAILED")
                    .failureReason("Extension task type '" + conductorTaskType + "' is not configured. " +
                            "Add the appropriate Conductor extension dependency (e.g., conductor-json-jq-task).")
                    .build();
        }

        // Test helpers for specific behaviors (based on activity type / task name)
        if (activityType.toLowerCase().contains("fail")) {
            return executeFailingTask(activityType, taskRefName, input);
        } else if (activityType.toLowerCase().contains("slow")) {
            return executeSlowTask(activityType, taskRefName, input);
        } else {
            // Default: execute as simple worker task stub
            return executeSimpleTask(activityType, taskRefName, input);
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
