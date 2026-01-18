package io.temporal.conductor.poc;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.PollDataDAO;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of ExecutionDAO for POC testing. Stores workflow and task execution
 * state in HashMap structures. Designed for single-threaded use within a Temporal workflow.
 */
public class InMemoryExecutionDAO implements ExecutionDAO, PollDataDAO {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryExecutionDAO.class);

  private final Map<String, Workflow> workflows = new ConcurrentHashMap<>();
  private final Map<String, Task> tasks = new ConcurrentHashMap<>();
  private final Map<String, List<Task>> workflowToTasks = new ConcurrentHashMap<>();

  @Override
  public List<Task> createTasks(List<Task> tasks) {
    for (Task task : tasks) {
      this.tasks.put(task.getTaskId(), task);
      workflowToTasks.computeIfAbsent(task.getWorkflowInstanceId(), k -> new ArrayList<>()).add(task);
      logger.debug(
          "Created task: {} ({}) for workflow {}",
          task.getTaskDefName(),
          task.getTaskId(),
          task.getWorkflowInstanceId());
    }
    return tasks;
  }

  @Override
  public void updateTask(Task task) {
    tasks.put(task.getTaskId(), task);
    logger.debug(
        "Updated task: {} ({}) status={}",
        task.getTaskDefName(),
        task.getTaskId(),
        task.getStatus());
  }

  @Override
  public boolean exceedsInProgressLimit(Task task) {
    return false; // No limits for POC
  }

  @Override
  public boolean exceedsRateLimitPerFrequency(Task task) {
    return false; // No limits for POC
  }

  @Override
  public Task getTask(String taskId) {
    return tasks.get(taskId);
  }

  @Override
  public List<Task> getTasks(List<String> taskIds) {
    return taskIds.stream().map(tasks::get).filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public List<Task> getTasksForWorkflow(String workflowId) {
    return workflowToTasks.getOrDefault(workflowId, Collections.emptyList());
  }

  @Override
  public String createWorkflow(Workflow workflow) {
    workflows.put(workflow.getWorkflowId(), workflow);
    logger.info("Created workflow: {} ({})", workflow.getWorkflowName(), workflow.getWorkflowId());
    return workflow.getWorkflowId();
  }

  @Override
  public String updateWorkflow(Workflow workflow) {
    workflows.put(workflow.getWorkflowId(), workflow);
    logger.debug(
        "Updated workflow: {} ({}) status={}",
        workflow.getWorkflowName(),
        workflow.getWorkflowId(),
        workflow.getStatus());
    return workflow.getWorkflowId();
  }

  @Override
  public boolean removeWorkflow(String workflowId) {
    workflows.remove(workflowId);
    workflowToTasks.remove(workflowId);
    return true;
  }

  @Override
  public Workflow getWorkflow(String workflowId) {
    return workflows.get(workflowId);
  }

  @Override
  public Workflow getWorkflow(String workflowId, boolean includeTasks) {
    Workflow workflow = workflows.get(workflowId);
    if (workflow != null && includeTasks) {
      List<Task> workflowTasks = workflowToTasks.getOrDefault(workflowId, Collections.emptyList());
      workflow.setTasks(new ArrayList<>(workflowTasks));
    }
    return workflow;
  }

  @Override
  public boolean removeTask(String taskId) {
    Task task = tasks.remove(taskId);
    if (task != null) {
      List<Task> workflowTasks = workflowToTasks.get(task.getWorkflowInstanceId());
      if (workflowTasks != null) {
        workflowTasks.removeIf(t -> t.getTaskId().equals(taskId));
      }
      return true;
    }
    return false;
  }

  // Unsupported operations for POC - minimal implementations

  @Override
  public List<Task> getPendingTasksForTaskType(String taskType) {
    return Collections.emptyList();
  }

  @Override
  public long getInProgressTaskCount(String taskDefName) {
    return 0;
  }

  @Override
  public List<Workflow> getPendingWorkflowsByType(String workflowName, int version) {
    return Collections.emptyList();
  }

  @Override
  public long getPendingWorkflowCount(String workflowName) {
    return 0;
  }

  @Override
  public long getRunningWorkflowCount(String workflowName, int version) {
    return workflows.values().stream()
        .filter(
            w ->
                w.getWorkflowName().equals(workflowName)
                    && w.getStatus().equals(Workflow.WorkflowStatus.RUNNING))
        .count();
  }

  @Override
  public List<String> getRunningWorkflowIds(String workflowName, int version) {
    return workflows.values().stream()
        .filter(
            w ->
                w.getWorkflowName().equals(workflowName)
                    && w.getStatus().equals(Workflow.WorkflowStatus.RUNNING))
        .map(Workflow::getWorkflowId)
        .collect(Collectors.toList());
  }

  @Override
  public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
    return Collections.emptyList();
  }

  @Override
  public List<Workflow> getWorkflowsByCorrelationId(
      String workflowName, String correlationId, boolean includeTasks) {
    return Collections.emptyList();
  }

  @Override
  public boolean canSearchAcrossWorkflows() {
    return false;
  }

  @Override
  public void updateTaskDomain(String taskId, String domain) {
    // No-op for POC
  }

  @Override
  public void addTaskExecLog(List<TaskExecLog> logs) {
    // No-op for POC
  }

  @Override
  public List<TaskExecLog> getTaskExecLog(String taskId) {
    return Collections.emptyList();
  }

  @Override
  public void addMessage(String queue, Message message) {
    // No-op for POC
  }

  @Override
  public void addEventExecution(com.netflix.conductor.common.metadata.events.EventExecution ee) {
    // No-op for POC
  }

  @Override
  public void updateEventExecution(com.netflix.conductor.common.metadata.events.EventExecution ee) {
    // No-op for POC
  }

  @Override
  public void removeEventExecution(
      com.netflix.conductor.common.metadata.events.EventExecution ee) {
    // No-op for POC
  }

  // PollDataDAO implementation (required by DeciderService)

  @Override
  public void updateLastPollData(String taskDefName, String domain, String workerId) {
    // No-op for POC
  }

  @Override
  public PollData getPollData(String taskDefName, String domain) {
    return null; // No polling needed in POC
  }

  @Override
  public List<PollData> getPollData(String taskDefName) {
    return Collections.emptyList();
  }
}
