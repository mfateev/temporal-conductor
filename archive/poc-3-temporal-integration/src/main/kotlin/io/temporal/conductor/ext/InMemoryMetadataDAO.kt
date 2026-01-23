package io.temporal.conductor.ext

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.dao.MetadataDAO
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/**
 * In-memory implementation of MetadataDAO for Temporal workflow execution.
 *
 * This provides a lightweight, thread-safe metadata store that doesn't require
 * external databases. Suitable for:
 * - Running Conductor workflows in Temporal
 * - Testing and development
 * - Embedded use cases
 *
 * Note: Data is not persisted across process restarts. For production use,
 * consider using the Temporal-only mode where workflow/task definitions
 * are stored in Temporal's state.
 */
class InMemoryMetadataDAO : MetadataDAO {

    private val taskDefs = ConcurrentHashMap<String, TaskDef>()
    private val workflowDefs = ConcurrentHashMap<String, MutableMap<Int, WorkflowDef>>()

    override fun createTaskDef(taskDef: TaskDef): TaskDef {
        taskDefs[taskDef.name] = taskDef
        return taskDef
    }

    override fun updateTaskDef(taskDef: TaskDef): TaskDef {
        taskDefs[taskDef.name] = taskDef
        return taskDef
    }

    override fun getTaskDef(name: String): TaskDef? {
        return taskDefs[name]
    }

    override fun getAllTaskDefs(): List<TaskDef> {
        return taskDefs.values.toList()
    }

    override fun removeTaskDef(name: String) {
        taskDefs.remove(name)
    }

    override fun createWorkflowDef(def: WorkflowDef) {
        val versions = workflowDefs.getOrPut(def.name) { ConcurrentHashMap() }
        versions[def.version] = def
    }

    override fun updateWorkflowDef(def: WorkflowDef) {
        createWorkflowDef(def)
    }

    override fun getLatestWorkflowDef(name: String): Optional<WorkflowDef> {
        val versions = workflowDefs[name] ?: return Optional.empty()
        val maxVersion = versions.keys.maxOrNull() ?: return Optional.empty()
        return Optional.ofNullable(versions[maxVersion])
    }

    override fun getWorkflowDef(name: String, version: Int): Optional<WorkflowDef> {
        return Optional.ofNullable(workflowDefs[name]?.get(version))
    }

    override fun removeWorkflowDef(name: String, version: Int?) {
        if (version != null) {
            workflowDefs[name]?.remove(version)
        } else {
            workflowDefs.remove(name)
        }
    }

    override fun getAllWorkflowDefs(): List<WorkflowDef> {
        return workflowDefs.values.flatMap { it.values }
    }

    override fun getAllWorkflowDefsLatestVersions(): List<WorkflowDef> {
        return workflowDefs.values.mapNotNull { versions ->
            versions.keys.maxOrNull()?.let { versions[it] }
        }
    }

    /**
     * Clear all stored definitions
     */
    fun clear() {
        taskDefs.clear()
        workflowDefs.clear()
    }

    /**
     * Get count of task definitions
     */
    val taskDefCount: Int get() = taskDefs.size

    /**
     * Get count of workflow definitions (all versions)
     */
    val workflowDefCount: Int get() = workflowDefs.values.sumOf { it.size }
}
