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
package io.temporal.conductor.util;

/**
 * Utility for parsing task names that may contain a task queue suffix.
 *
 * <p>Task names can optionally include a task queue using the format: {@code taskName@taskQueue}
 * This allows routing SIMPLE tasks to external workers listening on different task queues.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code process_order} - runs on default task queue</li>
 *   <li>{@code process_order@external-workers} - runs on "external-workers" task queue</li>
 * </ul>
 */
public final class TaskNameParser {

    private TaskNameParser() {
        // Utility class
    }

    /**
     * Result of parsing a task name with optional task queue suffix.
     *
     * @param baseName the activity type name (without the @taskQueue suffix)
     * @param taskQueue the target task queue, or null if using the workflow's default queue
     */
    public record ParsedTaskName(String baseName, String taskQueue) {}

    /**
     * Parse a task definition name that may contain a task queue suffix.
     *
     * <p>The format is: {@code baseName@taskQueue}
     *
     * @param taskDefName the task definition name (may be null)
     * @return parsed result with base name and optional task queue
     */
    public static ParsedTaskName parse(String taskDefName) {
        if (taskDefName == null || !taskDefName.contains("@")) {
            return new ParsedTaskName(taskDefName, null);
        }

        int atIndex = taskDefName.lastIndexOf('@');
        String baseName = taskDefName.substring(0, atIndex);
        String taskQueue = taskDefName.substring(atIndex + 1);

        // Treat empty task queue as null
        if (taskQueue.isEmpty()) {
            return new ParsedTaskName(baseName, null);
        }

        return new ParsedTaskName(baseName, taskQueue);
    }
}
