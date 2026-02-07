/*
 * Copyright 2024 Temporal Technologies, Inc.
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
package io.temporal.conductor.demo;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInfo;
import io.temporal.activity.DynamicActivity;
import io.temporal.common.converter.EncodedValues;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic activity implementation for the external worker.
 *
 * <p>This activity handles any task type routed to the external worker's task queue.
 * It processes the task and returns a result in the format expected by the conductor workflow.
 *
 * <p>The activity receives:
 * <ul>
 *   <li>taskRefName - the task reference name in the workflow</li>
 *   <li>conductorTaskType - the Conductor task type (e.g., SIMPLE)</li>
 *   <li>inputData - the task input data map</li>
 * </ul>
 */
public class ExternalTaskActivities implements DynamicActivity {

    private static final Logger logger = LoggerFactory.getLogger(ExternalTaskActivities.class);

    @Override
    @SuppressWarnings("unchecked")
    public Object execute(EncodedValues args) {
        ActivityInfo info = Activity.getExecutionContext().getInfo();
        String activityType = info.getActivityType();

        // Extract arguments: taskRefName, conductorTaskType, inputData
        String taskRefName = args.get(0, String.class);
        String conductorTaskType = args.get(1, String.class);
        Map<String, Object> inputData = args.get(2, Map.class);

        logger.info("=================================================");
        logger.info("EXTERNAL WORKER: Processing task");
        logger.info("  Activity Type: {}", activityType);
        logger.info("  Task Ref Name: {}", taskRefName);
        logger.info("  Conductor Task Type: {}", conductorTaskType);
        logger.info("  Activity ID: {}", info.getActivityId());
        logger.info("  Input Data: {}", inputData);
        logger.info("=================================================");

        // Process the task - for demo purposes, we echo the input with metadata
        Map<String, Object> output = new HashMap<>();

        // Copy input data to output
        if (inputData != null) {
            output.putAll(inputData);
        }

        // Add metadata showing this was processed by external worker
        output.put("_processedBy", "external-worker");
        output.put("_activityId", info.getActivityId());
        output.put("_activityType", activityType);
        output.put("_taskRefName", taskRefName);

        logger.info("EXTERNAL WORKER: Task completed successfully");
        logger.info("  Output: {}", output);

        // Return result in TaskExecutionResult format
        Map<String, Object> result = new HashMap<>();
        result.put("output", output);
        result.put("status", "COMPLETED");
        result.put("failureReason", null);

        return result;
    }
}
