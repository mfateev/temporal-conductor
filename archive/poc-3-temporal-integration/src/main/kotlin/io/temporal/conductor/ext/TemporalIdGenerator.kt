package io.temporal.conductor.ext

import com.netflix.conductor.core.utils.IDGenerator

/**
 * IDGenerator adapter that delegates to an IdGeneratorProvider.
 *
 * This class extends Conductor's IDGenerator to allow injection into DeciderService
 * while delegating actual ID generation to our custom provider.
 *
 * Usage:
 * ```kotlin
 * val provider = DeterministicIdGenerator(workflowRunId)
 * val idGenerator = TemporalIdGenerator(provider)
 * // Use idGenerator in DeciderService constructor
 * ```
 */
class TemporalIdGenerator(
    private val provider: IdGeneratorProvider
) : IDGenerator() {

    override fun generate(): String {
        return provider.generate()
    }

    /**
     * Reset the underlying provider (if supported)
     */
    fun reset() {
        provider.reset()
    }

    companion object {
        /**
         * Create a deterministic ID generator for Temporal workflows
         */
        fun deterministic(prefix: String): TemporalIdGenerator {
            return TemporalIdGenerator(DeterministicIdGenerator(prefix))
        }

        /**
         * Create a non-deterministic ID generator (default UUID behavior)
         */
        fun default(): TemporalIdGenerator {
            return TemporalIdGenerator(DefaultIdGeneratorProvider())
        }
    }
}
