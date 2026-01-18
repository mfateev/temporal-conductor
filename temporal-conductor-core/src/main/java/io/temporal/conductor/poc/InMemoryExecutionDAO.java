package io.temporal.conductor.poc;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of ExecutionDAO for POC testing. Stores workflow and task execution
 * state in HashMap structures. Designed for single-threaded use within a Temporal workflow.
 *
 * <p>Uses domain models (WorkflowModel/TaskModel) as required by ExecutionDAO interface.
 */
public class InMemoryExecutionDAO implements ExecutionDAO, PollDataDAO {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryExecutionDAO.class);

  private final Map<String, WorkflowModel> workflows = new ConcurrentHashMap<>();
  private final Map<String, TaskModel> tasks = new ConcurrentHashMap<>();
  private final Map<String, List<TaskModel>> workflowToTasks = new ConcurrentHashMap<>();

  @Override
  public List<TaskModel> createTasks(List<TaskModel> tasks) {
    for (TaskModel task : tasks) {
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
  public void updateTask(TaskModel task) {
    tasks.put(task.getTaskId(), task);
    logger.debug(
        "Updated task: {} ({}) status={}",
        task.getTaskDefName(),
        task.getTaskId(),
        task.getStatus());
  }

  @Override
  public boolean exceedsInProgressLimit(TaskModel task) {
    return false; // No limits for POC
  }

  @Override
  public boolean exceedsRateLimitPerFrequency(TaskModel task) {
    return false; // No limits for POC
  }

  @Override
  public TaskModel getTask(String taskId) {
    return tasks.get(taskId);
  }

  @Override
  public List<TaskModel> getTasks(List<String> taskIds) {
    return taskIds.stream().map(tasks::get).filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public List<TaskModel> getTasksForWorkflow(String workflowId) {
    return workflowToTasks.getOrDefault(workflowId, Collections.emptyList());
  }

  @Override
  public String createWorkflow(WorkflowModel workflow) {
    workflows.put(workflow.getWorkflowId(), workflow);
    logger.info("Created workflow: {} ({})", workflow.getWorkflowName(), workflow.getWorkflowId());
    return workflow.getWorkflowId();
  }

  @Override
  public String updateWorkflow(WorkflowModel workflow) {
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
  public void removeFromPendingWorkflow(String workflowType, String workflowId) {
    // No-op for POC (we don't track pending workflows separately)
  }

  @Override
  public WorkflowModel getWorkflow(String workflowId) {
    return workflows.get(workflowId);
  }

  @Override
  public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
    WorkflowModel workflow = workflows.get(workflowId);
    if (workflow != null && includeTasks) {
      List<TaskModel> workflowTasks = workflowToTasks.getOrDefault(workflowId, Collections.emptyList());
      workflow.setTasks(new ArrayList<>(workflowTasks));
    }
    return workflow;
  }

  @Override
  public boolean removeTask(String taskId) {
    TaskModel task = tasks.remove(taskId);
    if (task != null) {
      List<TaskModel> workflowTasks = workflowToTasks.get(task.getWorkflowInstanceId());
      if (workflowTasks != null) {
        workflowTasks.removeIf(t -> t.getTaskId().equals(taskId));
      }
      return true;
    }
    return false;
  }

  // Unsupported operations for POC - minimal implementations

  @Override
  public List<TaskModel> getPendingTasksForTaskType(String taskType) {
    return Collections.emptyList();
  }

  @Override
  public long getInProgressTaskCount(String taskDefName) {
    return 0;
  }

  @Override
  public List<WorkflowModel> getPendingWorkflowsByType(String workflowName, int version) {
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
                    && w.getStatus().equals(WorkflowModel.Status.RUNNING))
        .count();
  }

  @Override
  public List<String> getRunningWorkflowIds(String workflowName, int version) {
    return workflows.values().stream()
        .filter(
            w ->
                w.getWorkflowName().equals(workflowName)
                    && w.getStatus().equals(WorkflowModel.Status.RUNNING))
        .map(WorkflowModel::getWorkflowId)
        .collect(Collectors.toList());
  }

  @Override
  public List<WorkflowModel> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
    return Collections.emptyList();
  }

  @Override
  public List<WorkflowModel> getWorkflowsByCorrelationId(
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
  public boolean addEventExecution(com.netflix.conductor.common.metadata.events.EventExecution ee) {
    // No-op for POC
    return true;
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
