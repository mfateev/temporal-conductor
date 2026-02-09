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
package io.temporal.conductor.handler.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class WorkerTaskHandlerTest {

    @Mock
    private TaskExecutionContext context;

    @Mock
    private WorkflowModel workflow;

    private WorkerTaskHandler handler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new WorkerTaskHandler();
        when(context.currentTimeMillis()).thenReturn(System.currentTimeMillis());
        when(context.isActivityPending(anyString())).thenReturn(false);
    }

    @Test
    void testExecuteWithExternalTaskQueue() {
        // Given: a task with @taskQueue suffix
        TaskModel task = new TaskModel();
        task.setTaskId("task-123");
        task.setReferenceTaskName("process_external_ref");
        task.setTaskDefName("process_order@external-workers");
        task.setTaskType("SIMPLE");
        task.setStatus(TaskModel.Status.SCHEDULED);

        // Capture the activity arguments
        ArgumentCaptor<String> activityTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> taskQueueCaptor = ArgumentCaptor.forClass(String.class);

        // When: the handler executes
        handler.execute(task, workflow, context);

        // Then: verify executeActivityAsync was called with parsed values
        verify(context).executeActivityAsync(
                activityTypeCaptor.capture(),  // activityType
                eq("task-123"),                // taskId
                eq("process_external_ref"),    // taskRefName
                eq("SIMPLE"),                  // conductorTaskType
                eq(Collections.emptyMap()),    // inputData
                taskQueueCaptor.capture(),     // taskQueue
                any()                          // callback
        );

        // Activity type should be the base name without @taskQueue suffix
        assertEquals("process_order", activityTypeCaptor.getValue(),
                "Activity type should be base name without @taskQueue suffix");

        // Task queue should be extracted from the suffix
        assertEquals("external-workers", taskQueueCaptor.getValue(),
                "Task queue should be extracted from the suffix");
    }

    @Test
    void testExecuteWithoutTaskQueueSuffix() {
        // Given: a task WITHOUT @taskQueue suffix
        TaskModel task = new TaskModel();
        task.setTaskId("task-456");
        task.setReferenceTaskName("normal_task_ref");
        task.setTaskDefName("process_order");
        task.setTaskType("SIMPLE");
        task.setStatus(TaskModel.Status.SCHEDULED);

        // Capture the activity arguments
        ArgumentCaptor<String> activityTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> taskQueueCaptor = ArgumentCaptor.forClass(String.class);

        // When: the handler executes
        handler.execute(task, workflow, context);

        // Then: verify executeActivityAsync was called
        verify(context).executeActivityAsync(
                activityTypeCaptor.capture(),
                eq("task-456"),
                eq("normal_task_ref"),
                eq("SIMPLE"),
                eq(Collections.emptyMap()),
                taskQueueCaptor.capture(),
                any()
        );

        // Activity type should be the full task name
        assertEquals("process_order", activityTypeCaptor.getValue(),
                "Activity type should equal task def name when no suffix");

        // Task queue should be null (use default)
        assertNull(taskQueueCaptor.getValue(),
                "Task queue should be null when no suffix present");
    }

    @Test
    void testExecuteWithMultipleAtSymbols() {
        // Given: a task with multiple @ symbols (edge case)
        TaskModel task = new TaskModel();
        task.setTaskId("task-789");
        task.setReferenceTaskName("email_task_ref");
        task.setTaskDefName("send_email@example.com@external-workers");
        task.setTaskType("SIMPLE");
        task.setStatus(TaskModel.Status.SCHEDULED);

        // Capture the activity arguments
        ArgumentCaptor<String> activityTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> taskQueueCaptor = ArgumentCaptor.forClass(String.class);

        // When: the handler executes
        handler.execute(task, workflow, context);

        // Then: verify executeActivityAsync was called
        verify(context).executeActivityAsync(
                activityTypeCaptor.capture(),
                eq("task-789"),
                eq("email_task_ref"),
                eq("SIMPLE"),
                eq(Collections.emptyMap()),
                taskQueueCaptor.capture(),
                any()
        );

        // Activity type should use the last @ as separator
        assertEquals("send_email@example.com", activityTypeCaptor.getValue(),
                "Activity type should include all content before last @");

        // Task queue should be extracted from after the last @
        assertEquals("external-workers", taskQueueCaptor.getValue(),
                "Task queue should be after last @");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testActivityCompletionHandling() {
        // Given: a task that will complete successfully
        TaskModel task = new TaskModel();
        task.setTaskId("task-complete");
        task.setReferenceTaskName("complete_ref");
        task.setTaskDefName("complete_task");
        task.setTaskType("SIMPLE");
        task.setStatus(TaskModel.Status.SCHEDULED);

        // Capture the callback
        ArgumentCaptor<BiConsumer<TaskExecutionResult, Throwable>> callbackCaptor =
                ArgumentCaptor.forClass(BiConsumer.class);

        // When: the handler executes
        handler.execute(task, workflow, context);

        // Capture the completion callback
        verify(context).executeActivityAsync(
                anyString(), anyString(), anyString(), anyString(), anyMap(),
                any(), callbackCaptor.capture()
        );

        // Simulate successful completion
        TaskExecutionResult result = new TaskExecutionResult(
                Map.of("result", "success"),
                "COMPLETED",
                null
        );

        // Invoke the callback
        callbackCaptor.getValue().accept(result, null);

        // Verify task is marked completed
        assertEquals(TaskModel.Status.COMPLETED, task.getStatus());
        assertEquals(Map.of("result", "success"), task.getOutputData());
        verify(context).markStateUpdated();
    }
}
