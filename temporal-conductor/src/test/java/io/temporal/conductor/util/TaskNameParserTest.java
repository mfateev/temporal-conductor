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

import static org.junit.jupiter.api.Assertions.*;

import io.temporal.conductor.util.TaskNameParser.ParsedTaskName;
import org.junit.jupiter.api.Test;

class TaskNameParserTest {

    @Test
    void testSimpleTaskName() {
        ParsedTaskName result = TaskNameParser.parse("process_order");
        assertEquals("process_order", result.baseName());
        assertNull(result.taskQueue());
    }

    @Test
    void testTaskNameWithTaskQueue() {
        ParsedTaskName result = TaskNameParser.parse("process_order@external-workers");
        assertEquals("process_order", result.baseName());
        assertEquals("external-workers", result.taskQueue());
    }

    @Test
    void testTaskNameWithMultipleAtSymbols() {
        // Use last @ as separator
        ParsedTaskName result = TaskNameParser.parse("email@example.com@external-workers");
        assertEquals("email@example.com", result.baseName());
        assertEquals("external-workers", result.taskQueue());
    }

    @Test
    void testTaskNameWithEmptyTaskQueue() {
        ParsedTaskName result = TaskNameParser.parse("process_order@");
        assertEquals("process_order", result.baseName());
        assertNull(result.taskQueue());
    }

    @Test
    void testNullTaskName() {
        ParsedTaskName result = TaskNameParser.parse(null);
        assertNull(result.baseName());
        assertNull(result.taskQueue());
    }

    @Test
    void testEmptyTaskName() {
        ParsedTaskName result = TaskNameParser.parse("");
        assertEquals("", result.baseName());
        assertNull(result.taskQueue());
    }

    @Test
    void testTaskNameWithOnlyAtSymbol() {
        ParsedTaskName result = TaskNameParser.parse("@");
        assertEquals("", result.baseName());
        assertNull(result.taskQueue());
    }

    @Test
    void testTaskNameWithDashes() {
        ParsedTaskName result = TaskNameParser.parse("my-complex-task@my-task-queue");
        assertEquals("my-complex-task", result.baseName());
        assertEquals("my-task-queue", result.taskQueue());
    }

    @Test
    void testTaskNameWithUnderscores() {
        ParsedTaskName result = TaskNameParser.parse("my_task_name@my_queue_name");
        assertEquals("my_task_name", result.baseName());
        assertEquals("my_queue_name", result.taskQueue());
    }

    @Test
    void testTaskNameWithNumbers() {
        ParsedTaskName result = TaskNameParser.parse("task123@queue456");
        assertEquals("task123", result.baseName());
        assertEquals("queue456", result.taskQueue());
    }
}
