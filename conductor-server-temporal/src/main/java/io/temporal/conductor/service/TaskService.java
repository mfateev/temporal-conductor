package io.temporal.conductor.service;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.TaskSummary;
import java.util.List;

/**
 * Service interface for task operations.
 */
public interface TaskService {

    /**
     * Get a task by ID.
     *
     * @param taskId the task ID
     * @return the task
     */
    Task getTask(String taskId);

    /**
     * Search for tasks.
     *
     * @param start the start index
     * @param size the page size
     * @param sort the sort field
     * @param freeText free text search
     * @param query the search query
     * @return search results with task summaries
     */
    SearchResult<TaskSummary> searchTasks(
            int start, int size, String sort, String freeText, String query);

    /**
     * Add a log entry to a task.
     *
     * @param taskId the task ID
     * @param log the log message
     */
    void addTaskLog(String taskId, String log);

    /**
     * Get task execution logs.
     *
     * @param taskId the task ID
     * @return list of task execution logs
     */
    List<TaskExecLog> getTaskLogs(String taskId);

    /**
     * Poll for a task of the specified type.
     *
     * @param taskType the task type
     * @param workerId the worker ID
     * @param domain the task domain
     * @return the task, or null if none available
     */
    Task poll(String taskType, String workerId, String domain);

    /**
     * Update the status of a task.
     *
     * @param taskResult the task result
     * @return the task ID
     */
    String updateTask(TaskResult taskResult);
}
