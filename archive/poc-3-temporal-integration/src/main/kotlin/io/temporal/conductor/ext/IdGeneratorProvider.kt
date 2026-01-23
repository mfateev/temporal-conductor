package io.temporal.conductor.ext

import com.netflix.conductor.core.utils.IDGenerator

/**
 * Extension point for ID generation in Temporal workflows.
 *
 * The default Conductor IDGenerator uses UUID.randomUUID() which is non-deterministic
 * and breaks Temporal workflow replay. This interface allows injecting a deterministic
 * ID generator that uses workflow-scoped counters.
 *
 * Usage in Temporal workflows:
 * ```kotlin
 * val idGenerator = DeterministicIdGenerator(workflowRunId)
 * val factory = DeciderServiceFactory(metadataDAO, idGenerator)
 * val deciderService = factory.create()
 * ```
 */
interface IdGeneratorProvider {
    /**
     * Generate a unique ID for a task or workflow.
     *
     * For Temporal workflows, this MUST be deterministic - the same sequence
     * of calls must produce the same IDs on replay.
     */
    fun generate(): String

    /**
     * Reset the generator state (optional).
     * Called when starting a new workflow instance.
     */
    fun reset() {}
}

/**
 * Default implementation that wraps Conductor's IDGenerator.
 * Uses UUID.randomUUID() - NOT deterministic, NOT safe for Temporal workflows.
 */
class DefaultIdGeneratorProvider : IdGeneratorProvider {
    private val delegate = IDGenerator()

    override fun generate(): String = delegate.generate()
}

/**
 * Deterministic ID generator for Temporal workflows.
 *
 * Generates IDs in the format: {prefix}-{sequence}
 * where prefix is typically the workflow run ID and sequence is an incrementing counter.
 *
 * Example IDs:
 * - "abc123-1" (first task)
 * - "abc123-2" (second task)
 * - "abc123-3" (third task)
 *
 * This ensures:
 * 1. IDs are unique within a workflow execution
 * 2. IDs are deterministic on replay (same sequence produces same IDs)
 * 3. IDs are still globally unique (workflow run IDs are unique)
 */
class DeterministicIdGenerator(
    private val prefix: String
) : IdGeneratorProvider {

    private var sequence = 0L

    override fun generate(): String {
        sequence++
        return "$prefix-$sequence"
    }

    override fun reset() {
        sequence = 0
    }

    /**
     * Get current sequence number (for debugging/logging)
     */
    val currentSequence: Long get() = sequence
}

/**
 * ID generator that can switch between deterministic and non-deterministic modes.
 *
 * Useful for testing or hybrid scenarios where some code paths need
 * deterministic IDs and others don't.
 */
class SwitchableIdGenerator(
    private var deterministic: IdGeneratorProvider? = null,
    private val fallback: IdGeneratorProvider = DefaultIdGeneratorProvider()
) : IdGeneratorProvider {

    override fun generate(): String {
        return deterministic?.generate() ?: fallback.generate()
    }

    override fun reset() {
        deterministic?.reset()
    }

    fun setDeterministicMode(prefix: String) {
        deterministic = DeterministicIdGenerator(prefix)
    }

    fun setNonDeterministicMode() {
        deterministic = null
    }
}
