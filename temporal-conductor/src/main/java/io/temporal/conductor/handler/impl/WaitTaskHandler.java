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

package io.temporal.conductor.handler.impl;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for WAIT tasks using Temporal timers.
 *
 * <p>WAIT tasks can wait for either:
 * <ul>
 *   <li>A duration (e.g., "100ms", "5s", "10m") - uses Temporal timer</li>
 *   <li>An absolute time ("until" parameter) - uses Temporal timer</li>
 *   <li>Indefinitely - waits for external signal to complete</li>
 * </ul>
 *
 * <p>The execution mode varies based on task configuration:
 * <ul>
 *   <li>With duration/until: TIMER mode</li>
 *   <li>Without duration: WAIT_FOR_SIGNAL mode</li>
 * </ul>
 */
public class WaitTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(WaitTaskHandler.class);

    /**
     * Pattern for parsing duration strings like "100ms", "5s", "10m", "1h".
     */
    private static final Pattern DURATION_PATTERN = Pattern.compile("^(\\d+)(ms|s|m|h|d)$");

    @Override
    public String getTaskType() {
        return TaskType.WAIT.name();
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        // Check if task has a duration or until time
        Map<String, Object> inputData = task.getInputData();
        if (inputData != null) {
            String durationStr = inputData.get("duration") != null
                    ? inputData.get("duration").toString() : null;
            String untilStr = inputData.get("until") != null
                    ? inputData.get("until").toString() : null;

            if ((durationStr != null && !durationStr.isEmpty())
                    || (untilStr != null && !untilStr.isEmpty())) {
                return ExecutionMode.TIMER;
            }
        }
        return ExecutionMode.WAIT_FOR_SIGNAL;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
        }

        if (task.getStatus().isTerminal()) {
            if (task.getEndTime() == 0L) {
                task.setEndTime(context.currentTimeMillis());
            }
            return;
        }

        // If timer already started, nothing more to do - callback will complete it
        if (context.isTimerPending(taskRefName)) {
            return;
        }

        Map<String, Object> inputData = task.getInputData();
        if (inputData != null) {
            String durationStr = inputData.get("duration") != null
                    ? inputData.get("duration").toString() : null;
            String untilStr = inputData.get("until") != null
                    ? inputData.get("until").toString() : null;

            if (durationStr != null && !durationStr.isEmpty()) {
                // Parse duration string (e.g., "100ms", "1s", "5m")
                Duration duration = parseDurationString(durationStr);
                if (duration != null && !duration.isZero() && !duration.isNegative()) {
                    startWaitTimer(taskRefName, duration, task, context);
                    return;
                }
            } else if (untilStr != null && !untilStr.isEmpty()) {
                long waitTimeout = task.getWaitTimeout();
                if (waitTimeout > 0) {
                    long currentTime = context.currentTimeMillis();
                    long delayMs = waitTimeout - currentTime;
                    if (delayMs > 0) {
                        startWaitTimer(taskRefName, Duration.ofMillis(delayMs), task, context);
                    } else {
                        task.setStatus(TaskModel.Status.COMPLETED);
                        task.setEndTime(context.currentTimeMillis());
                    }
                    return;
                }
            }
        }
        logger.debug("WAIT task {} waiting indefinitely for signal", taskRefName);
    }

    /**
     * Start a timer for the WAIT task.
     */
    private void startWaitTimer(String taskRefName, Duration duration, TaskModel task, TaskExecutionContext context) {
        context.startTimer(taskRefName, duration, () -> {
            // Timer completed - mark task as completed
            task.setStatus(TaskModel.Status.COMPLETED);
            task.setEndTime(context.currentTimeMillis());
            context.markStateUpdated();
            logger.debug("WAIT timer completed for task {}", taskRefName);
        });
        logger.debug("Started WAIT timer for task {} with duration {}", taskRefName, duration);
    }

    /**
     * Parse a duration string into a Duration object.
     *
     * @param durationStr duration string (e.g., "100ms", "5s", "10m", "1h", "1d")
     * @return the parsed duration, or null if invalid
     */
    static Duration parseDurationString(String durationStr) {
        if (durationStr == null || durationStr.isEmpty()) {
            return null;
        }

        Matcher matcher = DURATION_PATTERN.matcher(durationStr.trim().toLowerCase());
        if (!matcher.matches()) {
            return null;
        }

        long value = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);

        return switch (unit) {
            case "ms" -> Duration.ofMillis(value);
            case "s" -> Duration.ofSeconds(value);
            case "m" -> Duration.ofMinutes(value);
            case "h" -> Duration.ofHours(value);
            case "d" -> Duration.ofDays(value);
            default -> null;
        };
    }
}
