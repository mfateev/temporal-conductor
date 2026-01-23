package io.temporal.conductor.service.stub;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.conductor.service.MetadataService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Stub implementation of MetadataService for testing and UI validation.
 */
@Service
@Profile("stub")
public class StubMetadataService implements MetadataService {

    private final Map<String, Map<Integer, WorkflowDef>> workflowDefs = new ConcurrentHashMap<>();
    private final Map<String, TaskDef> taskDefs = new ConcurrentHashMap<>();

    /**
     * Constructor that initializes with sample data.
     */
    public StubMetadataService() {
        // Initialize with sample workflow definitions
        for (WorkflowDef def : StubDataGenerator.createWorkflowDefs()) {
            registerWorkflowDef(def);
        }

        // Initialize with sample task definitions
        for (TaskDef def : StubDataGenerator.createTaskDefs()) {
            taskDefs.put(def.getName(), def);
        }
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefs() {
        List<WorkflowDef> allDefs = new ArrayList<>();
        for (Map<Integer, WorkflowDef> versions : workflowDefs.values()) {
            allDefs.addAll(versions.values());
        }
        return allDefs;
    }

    @Override
    public WorkflowDef getWorkflowDef(String name, int version) {
        Map<Integer, WorkflowDef> versions = workflowDefs.get(name);
        if (versions == null) {
            return StubDataGenerator.createWorkflowDef(name);
        }
        WorkflowDef def = versions.get(version);
        return def != null ? def : StubDataGenerator.createWorkflowDef(name);
    }

    @Override
    public WorkflowDef getLatestWorkflowDef(String name) {
        Map<Integer, WorkflowDef> versions = workflowDefs.get(name);
        if (versions == null || versions.isEmpty()) {
            return StubDataGenerator.createWorkflowDef(name);
        }
        int latestVersion = versions.keySet().stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElse(1);
        return versions.get(latestVersion);
    }

    @Override
    public void registerWorkflowDef(WorkflowDef workflowDef) {
        workflowDefs.computeIfAbsent(workflowDef.getName(), k -> new ConcurrentHashMap<>())
                .put(workflowDef.getVersion(), workflowDef);
    }

    @Override
    public void updateWorkflowDefs(List<WorkflowDef> workflowDefsList) {
        for (WorkflowDef def : workflowDefsList) {
            registerWorkflowDef(def);
        }
    }

    @Override
    public void deleteWorkflowDef(String name, int version) {
        Map<Integer, WorkflowDef> versions = workflowDefs.get(name);
        if (versions != null) {
            versions.remove(version);
            if (versions.isEmpty()) {
                workflowDefs.remove(name);
            }
        }
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        return new ArrayList<>(taskDefs.values());
    }

    @Override
    public TaskDef getTaskDef(String taskType) {
        TaskDef def = taskDefs.get(taskType);
        if (def == null) {
            // Return a default task definition
            def = new TaskDef();
            def.setName(taskType);
            def.setDescription("Task definition for " + taskType);
            def.setRetryCount(3);
            def.setTimeoutSeconds(60);
        }
        return def;
    }

    @Override
    public void registerTaskDefs(List<TaskDef> taskDefsList) {
        for (TaskDef def : taskDefsList) {
            taskDefs.put(def.getName(), def);
        }
    }

    @Override
    public void deleteTaskDef(String taskType) {
        taskDefs.remove(taskType);
    }
}
