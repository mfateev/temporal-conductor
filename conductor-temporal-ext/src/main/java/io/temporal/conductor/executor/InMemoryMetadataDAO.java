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

package io.temporal.conductor.executor;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.dao.MetadataDAO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of MetadataDAO for Temporal workflow execution.
 *
 * <p>This provides a lightweight, thread-safe metadata store that doesn't require
 * external databases. Suitable for:
 * <ul>
 *   <li>Running Conductor workflows in Temporal</li>
 *   <li>Testing and development</li>
 *   <li>Embedded use cases</li>
 * </ul>
 *
 * <p>Note: Data is not persisted across process restarts. For production use,
 * consider using the Temporal-only mode where workflow/task definitions
 * are stored in Temporal's state.
 */
public class InMemoryMetadataDAO implements MetadataDAO {

    private final Map<String, TaskDef> taskDefs = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, WorkflowDef>> workflowDefs = new ConcurrentHashMap<>();

    @Override
    public TaskDef createTaskDef(TaskDef taskDef) {
        taskDefs.put(taskDef.getName(), taskDef);
        return taskDef;
    }

    @Override
    public TaskDef updateTaskDef(TaskDef taskDef) {
        taskDefs.put(taskDef.getName(), taskDef);
        return taskDef;
    }

    @Override
    public TaskDef getTaskDef(String name) {
        return taskDefs.get(name);
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        return new ArrayList<>(taskDefs.values());
    }

    @Override
    public void removeTaskDef(String name) {
        taskDefs.remove(name);
    }

    @Override
    public void createWorkflowDef(WorkflowDef def) {
        workflowDefs.computeIfAbsent(def.getName(), k -> new ConcurrentHashMap<>())
                .put(def.getVersion(), def);
    }

    @Override
    public void updateWorkflowDef(WorkflowDef def) {
        createWorkflowDef(def);
    }

    @Override
    public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
        Map<Integer, WorkflowDef> versions = workflowDefs.get(name);
        if (versions == null || versions.isEmpty()) {
            return Optional.empty();
        }
        int maxVersion = versions.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        return Optional.ofNullable(versions.get(maxVersion));
    }

    @Override
    public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
        Map<Integer, WorkflowDef> versions = workflowDefs.get(name);
        if (versions == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(versions.get(version));
    }

    @Override
    public void removeWorkflowDef(String name, Integer version) {
        if (version != null) {
            Map<Integer, WorkflowDef> versions = workflowDefs.get(name);
            if (versions != null) {
                versions.remove(version);
            }
        } else {
            workflowDefs.remove(name);
        }
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefs() {
        List<WorkflowDef> result = new ArrayList<>();
        for (Map<Integer, WorkflowDef> versions : workflowDefs.values()) {
            result.addAll(versions.values());
        }
        return result;
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefsLatestVersions() {
        List<WorkflowDef> result = new ArrayList<>();
        for (Map<Integer, WorkflowDef> versions : workflowDefs.values()) {
            if (!versions.isEmpty()) {
                int maxVersion = versions.keySet().stream()
                        .mapToInt(Integer::intValue)
                        .max()
                        .orElse(0);
                WorkflowDef def = versions.get(maxVersion);
                if (def != null) {
                    result.add(def);
                }
            }
        }
        return result;
    }

    /**
     * Clear all stored definitions.
     */
    public void clear() {
        taskDefs.clear();
        workflowDefs.clear();
    }

    /**
     * Get count of task definitions.
     *
     * @return the number of task definitions
     */
    public int getTaskDefCount() {
        return taskDefs.size();
    }

    /**
     * Get count of workflow definitions (all versions).
     *
     * @return the total number of workflow definitions across all versions
     */
    public int getWorkflowDefCount() {
        return workflowDefs.values().stream().mapToInt(Map::size).sum();
    }
}
