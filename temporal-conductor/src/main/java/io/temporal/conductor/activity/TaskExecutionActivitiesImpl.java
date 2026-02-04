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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Dynamic Activity implementation for executing Conductor tasks.
 *
 * <p>This implements DynamicActivity so that the Temporal activity type
 * equals the Conductor task name (e.g., "send_email", "process_order").
 * This provides better visibility in Temporal UI and enables per-task
 * metrics and configuration.
 *
 * <p>For extension task types (JSON_JQ_TRANSFORM, KAFKA_PUBLISH, etc.),
 * this activity delegates to the {@link ExtensionTaskExecutionActivityImpl}
 * which has the registered WorkflowSystemTask implementations.
 *
 * <p>For SIMPLE tasks, this provides stub implementations for POC purposes.
 * In production, this would integrate with Conductor worker polling.
 */
@Component
public class TaskExecutionActivitiesImpl implements DynamicActivity {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionActivitiesImpl.class);

    /**
     * Known Conductor extension task types that should be delegated to extension activity.
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

    private final ExtensionTaskExecutionActivityImpl extensionActivity;

    /**
     * Constructor for Spring dependency injection.
     *
     * @param extensionActivity the extension task execution activity
     */
    @Autowired
    public TaskExecutionActivitiesImpl(ExtensionTaskExecutionActivityImpl extensionActivity) {
        this.extensionActivity = extensionActivity;
        logger.info("TaskExecutionActivitiesImpl initialized with extension activity supporting: {}",
                extensionActivity.getRegisteredTaskTypes());
    }

    /**
     * No-arg constructor for testing without extensions.
     */
    public TaskExecutionActivitiesImpl() {
        this.extensionActivity = new ExtensionTaskExecutionActivityImpl();
        logger.info("TaskExecutionActivitiesImpl initialized without extension activity");
    }

    @Override
    public Object execute(EncodedValues args) {
        String activityType = Activity.getExecutionContext().getInfo().getActivityType();
        String taskRefName = args.get(0, String.class);
        String conductorTaskType = args.get(1, String.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> input = args.get(2, Map.class);

        logger.info("Executing task: {} (ref: {}, conductorType: {})", activityType, taskRefName, conductorTaskType);
        logger.debug("Task input: {}", input);

        // Check for extension task types - delegate to extension activity
        if (EXTENSION_TASK_TYPES.contains(conductorTaskType)) {
            if (extensionActivity.getRegisteredTaskTypes().contains(conductorTaskType)) {
                logger.info("Delegating extension task type '{}' to extension activity", conductorTaskType);
                return extensionActivity.execute(conductorTaskType, taskRefName, input);
            } else {
                logger.error("Extension task type '{}' is not configured", conductorTaskType);
                return TaskExecutionResult.builder()
                        .output(Collections.emptyMap())
                        .status("FAILED")
                        .failureReason("Extension task type '" + conductorTaskType + "' is not configured. " +
                                "Add the appropriate Conductor extension dependency (e.g., conductor-json-jq-task).")
                        .build();
            }
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

    /**
     * Execute a SIMPLE task with real processing logic.
     *
     * <p>This implementation does actual work based on the input:
     * <ul>
     *   <li>Copies all input fields to output (data passthrough)</li>
     *   <li>Computes a "result" field based on input for downstream task references</li>
     *   <li>Supports specific behaviors based on task name patterns</li>
     * </ul>
     *
     * <p>Task name patterns:
     * <ul>
     *   <li>"compute_*" - performs arithmetic on numeric inputs</li>
     *   <li>"transform_*" - transforms string inputs to uppercase</li>
     *   <li>"validate_*" - validates required fields are present</li>
     *   <li>Default - echoes input with metadata</li>
     * </ul>
     */
    private TaskExecutionResult executeSimpleTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input) {

        Map<String, Object> output = new HashMap<>();

        // Copy all input to output (real data passthrough)
        if (input != null) {
            output.putAll(input);
        }

        // Add task metadata
        output.put("taskName", taskName);
        output.put("taskRefName", taskRefName);
        output.put("processedAt", System.currentTimeMillis());

        // Compute result based on task type
        String lowerTaskName = taskName.toLowerCase();

        if (lowerTaskName.startsWith("compute")) {
            // Arithmetic computation task
            Object result = computeResult(input);
            output.put("result", result);
            logger.debug("Compute task '{}' completed with result: {}", taskRefName, result);

        } else if (lowerTaskName.startsWith("transform")) {
            // String transformation task
            Object result = transformResult(input);
            output.put("result", result);
            logger.debug("Transform task '{}' completed with result: {}", taskRefName, result);

        } else if (lowerTaskName.startsWith("validate")) {
            // Validation task
            boolean valid = validateInput(input);
            output.put("result", valid);
            output.put("valid", valid);
            if (!valid) {
                logger.warn("Validation task '{}' failed validation", taskRefName);
                return TaskExecutionResult.builder()
                        .output(output)
                        .status("FAILED")
                        .failureReason("Validation failed: missing required fields")
                        .build();
            }
            logger.debug("Validation task '{}' passed", taskRefName);

        } else {
            // Default: input already copied to output, just add a result marker
            // If input has explicit "value", use that as result; otherwise mark completed
            Object result = (input != null && input.containsKey("value"))
                    ? input.get("value")
                    : "completed";
            output.put("result", result);
        }

        return TaskExecutionResult.builder().output(output).build();
    }

    /**
     * Compute arithmetic result from numeric inputs.
     * Supports: sum, multiply, or returns the first numeric value found.
     */
    private Object computeResult(Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return 0;
        }

        // Check for explicit operation
        String operation = (String) input.getOrDefault("operation", "sum");

        // Collect numeric values
        List<Number> numbers = input.values().stream()
                .filter(v -> v instanceof Number)
                .map(v -> (Number) v)
                .collect(Collectors.toList());

        if (numbers.isEmpty()) {
            // If no numbers, return input value or 0
            return input.getOrDefault("value", 0);
        }

        switch (operation.toLowerCase()) {
            case "multiply":
                return numbers.stream()
                        .mapToDouble(Number::doubleValue)
                        .reduce(1, (a, b) -> a * b);
            case "sum":
            default:
                return numbers.stream()
                        .mapToDouble(Number::doubleValue)
                        .sum();
        }
    }

    /**
     * Transform string inputs to uppercase.
     */
    private Object transformResult(Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return "";
        }

        // If there's a "value" field, transform it
        if (input.containsKey("value")) {
            Object value = input.get("value");
            if (value instanceof String) {
                return ((String) value).toUpperCase();
            }
            return value;
        }

        // Otherwise, concatenate all string values
        return input.values().stream()
                .filter(v -> v instanceof String)
                .map(v -> ((String) v).toUpperCase())
                .collect(Collectors.joining(" "));
    }

    /**
     * Validate that required fields are present in input.
     * Only fails if explicit requiredFields are specified and missing.
     * Empty input with no requirements passes validation (valid by default).
     */
    private boolean validateInput(Map<String, Object> input) {
        if (input == null) {
            // Null input with no requirements is considered valid
            return true;
        }

        // Check for explicit required fields
        @SuppressWarnings("unchecked")
        List<String> requiredFields = (List<String>) input.get("requiredFields");
        if (requiredFields != null && !requiredFields.isEmpty()) {
            for (String field : requiredFields) {
                if (!input.containsKey(field) || input.get(field) == null) {
                    return false;
                }
            }
        }

        // Default: valid (no explicit requirements means always pass)
        return true;
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
