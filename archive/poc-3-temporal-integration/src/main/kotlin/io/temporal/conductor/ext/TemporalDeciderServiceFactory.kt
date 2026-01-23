package io.temporal.conductor.ext

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.core.config.ConductorProperties
import com.netflix.conductor.core.execution.DeciderService
import com.netflix.conductor.core.execution.evaluators.JavascriptEvaluator
import com.netflix.conductor.core.execution.evaluators.ValueParamEvaluator
import com.netflix.conductor.core.execution.mapper.*
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry
import com.netflix.conductor.core.storage.DummyPayloadStorage
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils
import com.netflix.conductor.core.utils.IDGenerator
import com.netflix.conductor.core.utils.ParametersUtils
import com.netflix.conductor.dao.MetadataDAO
import java.time.Duration
import java.util.Optional

/**
 * Factory for creating DeciderService with Temporal-compatible components.
 *
 * This factory allows injection of a custom ID generator for deterministic
 * workflow replay in Temporal.
 *
 * Usage:
 * ```kotlin
 * // For Temporal workflows (deterministic)
 * val factory = TemporalDeciderServiceFactory(
 *     metadataDAO = InMemoryMetadataDAO(),
 *     idGeneratorProvider = DeterministicIdGenerator(workflowRunId)
 * )
 *
 * // For non-Temporal use (default UUID behavior)
 * val factory = TemporalDeciderServiceFactory(
 *     metadataDAO = InMemoryMetadataDAO()
 * )
 * ```
 */
class TemporalDeciderServiceFactory(
    private val metadataDAO: MetadataDAO,
    private val idGeneratorProvider: IdGeneratorProvider = DefaultIdGeneratorProvider(),
    private val objectMapper: ObjectMapper = jacksonObjectMapper(),
    private val taskPendingTimeThreshold: Duration = Duration.ofMinutes(60)
) {
    /**
     * Create a DeciderService configured for Temporal workflow execution
     */
    fun create(): DeciderService {
        // Wrap the provider in our adapter
        val idGenerator = TemporalIdGenerator(idGeneratorProvider)

        // Create ParametersUtils for expression evaluation
        val parametersUtils = ParametersUtils(objectMapper)

        // Create ExternalPayloadStorageUtils (dummy implementation)
        val externalPayloadStorage = DummyPayloadStorage()
        val conductorProperties = ConductorProperties()
        val externalPayloadStorageUtils = ExternalPayloadStorageUtils(
            externalPayloadStorage,
            conductorProperties,
            objectMapper
        )

        // Create SystemTaskRegistry (empty for now)
        val systemTaskRegistry = SystemTaskRegistry(emptySet())

        // Create TaskMappers
        val taskMappers = createTaskMappers(parametersUtils)

        return DeciderService(
            idGenerator,
            parametersUtils,
            metadataDAO,
            externalPayloadStorageUtils,
            systemTaskRegistry,
            taskMappers,
            taskPendingTimeThreshold
        )
    }

    private fun createTaskMappers(parametersUtils: ParametersUtils): Map<String, TaskMapper> {
        val mappers = mutableMapOf<String, TaskMapper>()

        // SimpleTaskMapper for SIMPLE task type
        val simpleMapper = SimpleTaskMapper(parametersUtils)
        mappers[TaskType.SIMPLE.name] = simpleMapper

        // UserDefinedTaskMapper as fallback for custom task types
        val userDefinedMapper = UserDefinedTaskMapper(parametersUtils, metadataDAO)
        mappers[TaskType.USER_DEFINED.name] = userDefinedMapper

        // Create evaluators for SWITCH task
        val evaluators = mapOf(
            "value-param" to ValueParamEvaluator(),
            "javascript" to JavascriptEvaluator()
        )

        // SWITCH/DECISION task mapper
        val switchMapper = SwitchTaskMapper(evaluators)
        mappers[TaskType.SWITCH.name] = switchMapper
        mappers[TaskType.DECISION.name] = DecisionTaskMapper()  // Deprecated, uses ScriptEvaluator internally

        // FORK_JOIN task mappers
        mappers[TaskType.FORK_JOIN.name] = ForkJoinTaskMapper()
        mappers[TaskType.JOIN.name] = JoinTaskMapper()

        // DO_WHILE task mapper
        mappers[TaskType.DO_WHILE.name] = DoWhileTaskMapper(metadataDAO, parametersUtils)

        // SET_VARIABLE task mapper
        mappers[TaskType.SET_VARIABLE.name] = SetVariableTaskMapper()

        // TERMINATE task mapper
        mappers[TaskType.TERMINATE.name] = TerminateTaskMapper(parametersUtils)

        return mappers
    }

    companion object {
        /**
         * Create a factory configured for deterministic Temporal workflow execution
         */
        fun forTemporalWorkflow(
            metadataDAO: MetadataDAO,
            workflowRunId: String
        ): TemporalDeciderServiceFactory {
            return TemporalDeciderServiceFactory(
                metadataDAO = metadataDAO,
                idGeneratorProvider = DeterministicIdGenerator(workflowRunId)
            )
        }

        /**
         * Create a factory with default (non-deterministic) ID generation
         */
        fun withDefaults(metadataDAO: MetadataDAO): TemporalDeciderServiceFactory {
            return TemporalDeciderServiceFactory(
                metadataDAO = metadataDAO,
                idGeneratorProvider = DefaultIdGeneratorProvider()
            )
        }
    }
}
