package io.temporal.conductor.poc2

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.dao.MetadataDAO
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/**
 * Minimal in-memory implementation of MetadataDAO for POC.
 * Only implements methods required for DeciderService.decide() to work.
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
}
