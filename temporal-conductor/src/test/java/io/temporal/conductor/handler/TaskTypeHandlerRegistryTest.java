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

package io.temporal.conductor.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.model.TaskModel;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for TaskTypeHandlerRegistry and TaskTypeHandlerFactory.
 *
 * <p>These tests verify that all system task types have handlers registered
 * in the registry created by TaskTypeHandlerFactory.
 *
 * <p>Missing handlers cause tasks to fall through to the default (worker) handler,
 * which may not correctly handle system tasks.
 */
class TaskTypeHandlerRegistryTest {

    private TaskTypeHandlerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = TaskTypeHandlerFactory.createRegistry();
    }

    /**
     * All system task types that should have explicit handlers registered.
     * These are task types with special execution semantics (not worker tasks).
     */
    private static final Set<String> SYSTEM_TASK_TYPES = Set.of(
            // Sync system tasks
            TaskType.FORK_JOIN.name(),
            "FORK", // TaskType.TASK_TYPE_FORK
            TaskType.SWITCH.name(),
            TaskType.DECISION.name(),
            TaskType.SET_VARIABLE.name(),
            TaskType.TERMINATE.name(),
            "NOOP",
            // Async system tasks (need re-evaluation)
            TaskType.DO_WHILE.name(),
            TaskType.JOIN.name(),
            TaskType.EXCLUSIVE_JOIN.name(),
            // Special task handlers
            TaskType.WAIT.name(),
            TaskType.HUMAN.name(),
            TaskType.SUB_WORKFLOW.name(),
            "START_WORKFLOW",
            TaskType.EVENT.name()
    );

    @Test
    void testAllSystemTaskTypesHaveHandlers() {
        for (String taskType : SYSTEM_TASK_TYPES) {
            assertTrue(registry.hasHandler(taskType),
                    "Handler for " + taskType + " should be registered. "
                    + "If this test fails, add the handler in TaskTypeHandlerFactory.createRegistry()");
        }
    }

    @Test
    void testEventTaskHandlerReturnsActivityMode() {
        TaskTypeHandler handler = registry.getHandler(TaskType.EVENT.name());

        assertNotNull(handler, "EVENT handler should be registered");
        assertEquals(TaskType.EVENT.name(), handler.getTaskType(),
                "Handler task type should be EVENT");

        // EVENT tasks should use ACTIVITY mode (run via Temporal activity)
        TaskModel task = new TaskModel();
        task.setTaskType(TaskType.EVENT.name());
        assertEquals(ExecutionMode.ACTIVITY, handler.getExecutionMode(task),
                "EVENT tasks should use ACTIVITY execution mode");
    }

    @Test
    void testForkJoinTaskHandlerReturnsSyncMode() {
        TaskTypeHandler handler = registry.getHandler(TaskType.FORK_JOIN.name());

        assertNotNull(handler, "FORK_JOIN handler should be registered");

        TaskModel task = new TaskModel();
        task.setTaskType(TaskType.FORK_JOIN.name());
        assertEquals(ExecutionMode.SYNC, handler.getExecutionMode(task),
                "FORK_JOIN tasks should use SYNC execution mode");
    }

    @Test
    void testDoWhileTaskHandlerReturnsAsyncReevalMode() {
        TaskTypeHandler handler = registry.getHandler(TaskType.DO_WHILE.name());

        assertNotNull(handler, "DO_WHILE handler should be registered");

        TaskModel task = new TaskModel();
        task.setTaskType(TaskType.DO_WHILE.name());
        assertEquals(ExecutionMode.ASYNC_REEVAL, handler.getExecutionMode(task),
                "DO_WHILE tasks should use ASYNC_REEVAL execution mode");
    }

    @Test
    void testWaitTaskHandlerRegistered() {
        TaskTypeHandler handler = registry.getHandler(TaskType.WAIT.name());

        assertNotNull(handler, "WAIT handler should be registered");
        assertEquals(TaskType.WAIT.name(), handler.getTaskType(),
                "Handler task type should be WAIT");
    }

    @Test
    void testHumanTaskHandlerRegistered() {
        TaskTypeHandler handler = registry.getHandler(TaskType.HUMAN.name());

        assertNotNull(handler, "HUMAN handler should be registered");
        assertEquals(TaskType.HUMAN.name(), handler.getTaskType(),
                "Handler task type should be HUMAN");
    }

    @Test
    void testSubWorkflowTaskHandlerRegistered() {
        TaskTypeHandler handler = registry.getHandler(TaskType.SUB_WORKFLOW.name());

        assertNotNull(handler, "SUB_WORKFLOW handler should be registered");
        assertEquals(TaskType.SUB_WORKFLOW.name(), handler.getTaskType(),
                "Handler task type should be SUB_WORKFLOW");

        TaskModel task = new TaskModel();
        task.setTaskType(TaskType.SUB_WORKFLOW.name());
        assertEquals(ExecutionMode.CHILD_WORKFLOW, handler.getExecutionMode(task),
                "SUB_WORKFLOW tasks should use CHILD_WORKFLOW execution mode");
    }

    @Test
    void testWorkerTaskTypesUseDefaultHandler() {
        // SIMPLE and HTTP should have explicit worker handlers registered
        assertTrue(registry.hasHandler(TaskType.SIMPLE.name()),
                "SIMPLE handler should be registered");
        assertTrue(registry.hasHandler(TaskType.HTTP.name()),
                "HTTP handler should be registered");

        // Verify they use ACTIVITY mode
        TaskTypeHandler simpleHandler = registry.getHandler(TaskType.SIMPLE.name());
        TaskModel simpleTask = new TaskModel();
        simpleTask.setTaskType(TaskType.SIMPLE.name());
        assertEquals(ExecutionMode.ACTIVITY, simpleHandler.getExecutionMode(simpleTask),
                "SIMPLE tasks should use ACTIVITY execution mode");
    }

    @Test
    void testUnknownTaskTypeReturnsDefaultHandler() {
        // Unknown task types should return the default handler (WorkerTaskHandler)
        TaskTypeHandler handler = registry.getHandler("UNKNOWN_TASK_TYPE");

        assertNotNull(handler, "Default handler should be returned for unknown types");

        // Default handler should use ACTIVITY mode
        TaskModel task = new TaskModel();
        task.setTaskType("UNKNOWN_TASK_TYPE");
        assertEquals(ExecutionMode.ACTIVITY, handler.getExecutionMode(task),
                "Unknown tasks should use ACTIVITY execution mode (worker task behavior)");
    }

    @Test
    void testRegistryHasDefaultHandler() {
        TaskTypeHandler defaultHandler = registry.getDefaultHandler();

        assertNotNull(defaultHandler, "Registry should have a default handler");
    }

    @Test
    void testRegisteredTaskTypesIncludesAllSystemTasks() {
        Set<String> registeredTypes = registry.getRegisteredTaskTypes();

        // All system task types should be in the registered set
        for (String taskType : SYSTEM_TASK_TYPES) {
            assertTrue(registeredTypes.contains(taskType),
                    taskType + " should be in registered task types");
        }
    }
}
