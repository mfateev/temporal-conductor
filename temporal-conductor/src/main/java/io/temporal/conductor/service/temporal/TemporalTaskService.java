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

package io.temporal.conductor.service.temporal;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.TaskSummary;
import io.temporal.conductor.service.TaskService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Temporal-backed implementation of TaskService.
 *
 * <p>This service provides task operations that interact with Temporal workflows.
 * Task information is queried from the running workflows, and task completion
 * is signaled to the workflow.
 */
@Service
@Profile("temporal")
public class TemporalTaskService implements TaskService {

    private static final Logger logger = LoggerFactory.getLogger(TemporalTaskService.class);

    private final WorkflowClient workflowClient;

    // In-memory task logs storage
    private final Map<String, List<TaskExecLog>> taskLogs = new ConcurrentHashMap<>();

    // Task ID to Workflow ID mapping (populated when tasks are queried)
    private final Map<String, String> taskToWorkflowMapping = new ConcurrentHashMap<>();

    /**
     * Creates a new TemporalTaskService.
     *
     * @param workflowClient the Temporal workflow client
     */
    public TemporalTaskService(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    @Override
    public Task getTask(String taskId) {
        // TODO: This API requires workflowId to function properly.
        // Refactor to include workflowId parameter or use workflow-specific task queries.
        throw new UnsupportedOperationException(
                "getTask(taskId) is not implemented. Use workflow query to get tasks instead.");
    }

    @Override
    public SearchResult<TaskSummary> searchTasks(
            int start, int size, String sort, String freeText, String query) {
        logger.debug("Searching tasks: start={}, size={}, query={}", start, size, query);
        // Task search is complex and would require visibility API integration
        // For now, return empty results
        return new SearchResult<>(0, Collections.emptyList());
    }

    @Override
    public void addTaskLog(String taskId, String log) {
        logger.debug("Adding log to task {}: {}", taskId, log);

        TaskExecLog execLog = new TaskExecLog();
        execLog.setTaskId(taskId);
        execLog.setLog(log);
        execLog.setCreatedTime(System.currentTimeMillis());

        taskLogs.computeIfAbsent(taskId, k -> new ArrayList<>()).add(execLog);
    }

    @Override
    public List<TaskExecLog> getTaskLogs(String taskId) {
        logger.debug("Getting logs for task: {}", taskId);
        List<TaskExecLog> logs = taskLogs.get(taskId);
        return logs != null ? new ArrayList<>(logs) : Collections.emptyList();
    }

    @Override
    public Task poll(String taskType, String workerId, String domain) {
        logger.debug("Polling for task: taskType={}, workerId={}", taskType, workerId);
        // External polling is not supported in this POC implementation
        // Tasks are executed internally via Temporal activities
        return null;
    }

    @Override
    public String updateTask(TaskResult taskResult) {
        logger.info("Updating task: taskId={}, status={}",
                taskResult.getTaskId(), taskResult.getStatus());

        String workflowId = taskResult.getWorkflowInstanceId();
        if (workflowId == null) {
            workflowId = taskToWorkflowMapping.get(taskResult.getTaskId());
        }

        if (workflowId == null) {
            logger.warn("Cannot update task - workflow ID not found: {}", taskResult.getTaskId());
            return taskResult.getTaskId();
        }

        try {
            // Signal the workflow to complete the task by ID
            // The workflow does the taskId -> taskRefName lookup internally
            WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
            workflowStub.signal("completeTaskById", taskResult.getTaskId(),
                    taskResult.getOutputData() != null
                            ? taskResult.getOutputData() : Collections.emptyMap());
            logger.info("Task completion signaled: taskId={}", taskResult.getTaskId());
        } catch (Exception e) {
            logger.warn("Failed to signal task completion: {}", e.getMessage());
        }

        return taskResult.getTaskId();
    }

    /**
     * Registers a task to workflow mapping.
     * This is called when tasks are discovered during workflow queries.
     *
     * @param taskId the task ID
     * @param workflowId the workflow ID containing the task
     */
    public void registerTaskMapping(String taskId, String workflowId) {
        taskToWorkflowMapping.put(taskId, workflowId);
    }
}
