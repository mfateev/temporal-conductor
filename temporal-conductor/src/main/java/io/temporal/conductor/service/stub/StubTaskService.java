package io.temporal.conductor.service.stub;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.TaskSummary;
import io.temporal.conductor.service.TaskService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Stub implementation of TaskService for testing and UI validation.
 */
@Service
@Profile("stub")
public class StubTaskService implements TaskService {

    private final Map<String, Task> tasks = new ConcurrentHashMap<>();
    private final Map<String, List<TaskExecLog>> taskLogs = new ConcurrentHashMap<>();

    @Override
    public Task getTask(String taskId) {
        Task task = tasks.get(taskId);
        if (task == null) {
            // Return a sample task
            task = new Task();
            task.setTaskId(taskId);
            task.setTaskType("SIMPLE");
            task.setTaskDefName("sample_task");
            task.setReferenceTaskName("sample_task_ref");
            task.setStatus(Task.Status.COMPLETED);
            task.setScheduledTime(System.currentTimeMillis() - 60000);
            task.setStartTime(System.currentTimeMillis() - 55000);
            task.setEndTime(System.currentTimeMillis() - 50000);
            task.setInputData(Map.of("input1", "value1"));
            task.setOutputData(Map.of("output1", "result1"));
            task.setWorkflowInstanceId(UUID.randomUUID().toString());
        }
        return task;
    }

    @Override
    public SearchResult<TaskSummary> searchTasks(
            int start, int size, String sort, String freeText, String query) {
        List<TaskSummary> results = StubDataGenerator.createTaskSummaries(size);
        return new SearchResult<>(results.size() + 100, results);
    }

    @Override
    public void addTaskLog(String taskId, String log) {
        TaskExecLog execLog = new TaskExecLog();
        execLog.setTaskId(taskId);
        execLog.setLog(log);
        execLog.setCreatedTime(System.currentTimeMillis());

        taskLogs.computeIfAbsent(taskId, k -> new ArrayList<>()).add(execLog);
    }

    @Override
    public List<TaskExecLog> getTaskLogs(String taskId) {
        List<TaskExecLog> logs = taskLogs.get(taskId);
        if (logs == null || logs.isEmpty()) {
            // Return sample logs
            logs = new ArrayList<>();

            TaskExecLog log1 = new TaskExecLog();
            log1.setTaskId(taskId);
            log1.setLog("Task execution started");
            log1.setCreatedTime(System.currentTimeMillis() - 60000);
            logs.add(log1);

            TaskExecLog log2 = new TaskExecLog();
            log2.setTaskId(taskId);
            log2.setLog("Processing input data");
            log2.setCreatedTime(System.currentTimeMillis() - 55000);
            logs.add(log2);

            TaskExecLog log3 = new TaskExecLog();
            log3.setTaskId(taskId);
            log3.setLog("Task completed successfully");
            log3.setCreatedTime(System.currentTimeMillis() - 50000);
            logs.add(log3);
        }
        return logs;
    }

    @Override
    public Task poll(String taskType, String workerId, String domain) {
        // Return a sample task for polling (simulates a queued task)
        Task task = new Task();
        task.setTaskId(UUID.randomUUID().toString());
        task.setTaskType(taskType);
        task.setTaskDefName(taskType);
        task.setReferenceTaskName(taskType + "_ref");
        task.setStatus(Task.Status.IN_PROGRESS);
        task.setScheduledTime(System.currentTimeMillis() - 5000);
        task.setStartTime(System.currentTimeMillis());
        task.setInputData(Map.of("input1", "value1", "workerId", workerId != null ? workerId : ""));
        task.setWorkflowInstanceId(UUID.randomUUID().toString());

        tasks.put(task.getTaskId(), task);
        return task;
    }

    @Override
    public String updateTask(TaskResult taskResult) {
        String taskId = taskResult.getTaskId();
        Task task = tasks.get(taskId);
        if (task == null) {
            task = new Task();
            task.setTaskId(taskId);
        }

        // Update task based on result
        switch (taskResult.getStatus()) {
            case COMPLETED:
                task.setStatus(Task.Status.COMPLETED);
                break;
            case FAILED:
                task.setStatus(Task.Status.FAILED);
                break;
            case FAILED_WITH_TERMINAL_ERROR:
                task.setStatus(Task.Status.FAILED_WITH_TERMINAL_ERROR);
                break;
            case IN_PROGRESS:
            default:
                task.setStatus(Task.Status.IN_PROGRESS);
                break;
        }

        task.setOutputData(taskResult.getOutputData());
        task.setEndTime(System.currentTimeMillis());
        tasks.put(taskId, task);

        return taskId;
    }
}
