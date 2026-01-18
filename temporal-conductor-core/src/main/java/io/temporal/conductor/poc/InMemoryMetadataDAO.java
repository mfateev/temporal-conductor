package io.temporal.conductor.poc;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.dao.MetadataDAO;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of MetadataDAO for POC testing. Stores workflow and task definitions in
 * HashMap structures.
 */
public class InMemoryMetadataDAO implements MetadataDAO {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryMetadataDAO.class);

  private final Map<String, WorkflowDef> workflowDefs = new ConcurrentHashMap<>();
  private final Map<String, TaskDef> taskDefs = new ConcurrentHashMap<>();

  @Override
  public TaskDef getTaskDef(String name) {
    return taskDefs.get(name);
  }

  @Override
  public List<TaskDef> getAllTaskDefs() {
    return new ArrayList<>(taskDefs.values());
  }

  @Override
  public void createWorkflowDef(WorkflowDef def) {
    workflowDefs.put(def.getName(), def);
    logger.debug("Created workflow def: {}", def.getName());
  }

  @Override
  public void update(WorkflowDef def) {
    workflowDefs.put(def.getName(), def);
    logger.debug("Updated workflow def: {}", def.getName());
  }

  @Override
  public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
    return Optional.ofNullable(workflowDefs.get(name));
  }

  @Override
  public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
    // For POC, ignore version and return latest
    return getLatestWorkflowDef(name);
  }

  @Override
  public void removeWorkflowDef(String name, Integer version) {
    workflowDefs.remove(name);
  }

  @Override
  public List<WorkflowDef> getAllWorkflowDefs() {
    return new ArrayList<>(workflowDefs.values());
  }

  @Override
  public List<WorkflowDef> getAllWorkflowDefsLatestVersions() {
    return new ArrayList<>(workflowDefs.values());
  }

  @Override
  public List<String> findAllWorkflowDefNames() {
    return new ArrayList<>(workflowDefs.keySet());
  }

  @Override
  public void createTaskDef(TaskDef taskDef) {
    taskDefs.put(taskDef.getName(), taskDef);
    logger.debug("Created task def: {}", taskDef.getName());
  }

  @Override
  public void updateTaskDef(TaskDef taskDef) {
    taskDefs.put(taskDef.getName(), taskDef);
    logger.debug("Updated task def: {}", taskDef.getName());
  }

  @Override
  public void removeTaskDef(String name) {
    taskDefs.remove(name);
  }

  // Helper method for POC testing
  public void registerSimpleTask(String name) {
    TaskDef taskDef = new TaskDef();
    taskDef.setName(name);
    taskDef.setRetryCount(3);
    taskDef.setTimeoutSeconds(60);
    taskDef.setResponseTimeoutSeconds(30);
    createTaskDef(taskDef);
  }

  // Helper method to create a simple workflow definition for testing
  public void registerSimpleWorkflow(String name, String... taskNames) {
    WorkflowDef workflowDef = new WorkflowDef();
    workflowDef.setName(name);
    workflowDef.setVersion(1);

    List<WorkflowTask> tasks = new ArrayList<>();
    for (String taskName : taskNames) {
      WorkflowTask task = new WorkflowTask();
      task.setName(taskName);
      task.setTaskReferenceName(taskName + "_ref");
      task.setType(TaskType.SIMPLE.name());
      tasks.add(task);
    }
    workflowDef.setTasks(tasks);
    createWorkflowDef(workflowDef);
  }
}
