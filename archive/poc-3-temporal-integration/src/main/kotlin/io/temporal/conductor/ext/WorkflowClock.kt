package io.temporal.conductor.ext

/**
 * Extension point for time in Temporal workflows.
 *
 * DeciderService uses System.currentTimeMillis() for timeout checking:
 * - checkWorkflowTimeout() - line 664
 * - checkTaskTimeout() - line 713
 * - checkTaskPollTimeout() - line 749
 * - isTaskPending() - line 799
 *
 * These calls break Temporal workflow replay determinism because the current
 * time changes between original execution and replay.
 *
 * MITIGATION STRATEGIES:
 *
 * 1. **Disable timeout checks in Temporal mode** (Recommended for v1)
 *    - Temporal has its own timeout mechanisms (workflow/activity timeouts)
 *    - Set workflow.timeoutSeconds = 0 to skip checkWorkflowTimeout()
 *    - Set task.timeoutSeconds = 0 to skip checkTaskTimeout()
 *    - This is safe because Temporal handles timeouts at activity level
 *
 * 2. **Fork DeciderService** (If Conductor timeout semantics required)
 *    - Create TemporalDeciderService that overrides timeout methods
 *    - Inject WorkflowClock that returns Temporal's Workflow.currentTimeMillis()
 *    - More invasive but preserves Conductor timeout behavior
 *
 * 3. **Wrap in sideEffect** (Not recommended)
 *    - Time would be recorded but still non-deterministic behavior
 *    - Timeout decisions would differ between execution and replay
 */
interface WorkflowClock {
    /**
     * Get current time in milliseconds.
     *
     * In Temporal workflows, this should return Workflow.currentTimeMillis()
     * which is deterministic across replays.
     */
    fun currentTimeMillis(): Long
}

/**
 * Default clock using System.currentTimeMillis().
 * NOT safe for Temporal workflows - use only for testing.
 */
class SystemClock : WorkflowClock {
    override fun currentTimeMillis(): Long = System.currentTimeMillis()
}

/**
 * Fixed clock for testing deterministic behavior.
 */
class FixedClock(private var time: Long) : WorkflowClock {
    override fun currentTimeMillis(): Long = time

    fun advance(millis: Long) {
        time += millis
    }

    fun set(time: Long) {
        this.time = time
    }
}

/**
 * Configuration for timeout behavior in Temporal mode.
 *
 * Since we can't easily inject a clock into DeciderService without forking,
 * the recommended approach is to disable Conductor's timeout checks and
 * rely on Temporal's timeout mechanisms instead.
 */
object TemporalTimeoutConfig {

    /**
     * Recommended timeout settings for Temporal mode:
     *
     * WorkflowDef:
     *   timeoutSeconds = 0  // Disables checkWorkflowTimeout()
     *
     * TaskDef:
     *   timeoutSeconds = 0       // Disables checkTaskTimeout()
     *   pollTimeoutSeconds = 0   // Disables checkTaskPollTimeout()
     *   responseTimeoutSeconds = 0
     *
     * Instead, configure timeouts in Temporal:
     *   - Workflow execution timeout
     *   - Activity start-to-close timeout
     *   - Activity schedule-to-close timeout
     */
    const val RECOMMENDATION = """
        For Temporal workflows, disable Conductor timeout checks:

        WorkflowDef:
          timeoutSeconds = 0

        TaskDef:
          timeoutSeconds = 0
          pollTimeoutSeconds = 0
          responseTimeoutSeconds = 0

        Configure timeouts in Temporal instead:
          - WorkflowOptions.setWorkflowExecutionTimeout()
          - ActivityOptions.setStartToCloseTimeout()
    """

    /**
     * Check if a workflow definition has timeout checks disabled.
     */
    fun isTimeoutDisabled(workflowDef: com.netflix.conductor.common.metadata.workflow.WorkflowDef): Boolean {
        return workflowDef.timeoutSeconds <= 0
    }

    /**
     * Check if a task definition has timeout checks disabled.
     */
    fun isTimeoutDisabled(taskDef: com.netflix.conductor.common.metadata.tasks.TaskDef): Boolean {
        return taskDef.timeoutSeconds <= 0 &&
               taskDef.pollTimeoutSeconds <= 0 &&
               taskDef.responseTimeoutSeconds <= 0
    }
}
