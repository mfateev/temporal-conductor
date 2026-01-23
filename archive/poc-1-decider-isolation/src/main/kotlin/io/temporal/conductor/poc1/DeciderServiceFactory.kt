package io.temporal.conductor.poc1

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.core.config.ConductorProperties
import com.netflix.conductor.core.execution.DeciderService
import com.netflix.conductor.core.execution.mapper.SimpleTaskMapper
import com.netflix.conductor.core.execution.mapper.TaskMapper
import com.netflix.conductor.core.execution.mapper.UserDefinedTaskMapper
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask
import com.netflix.conductor.core.storage.DummyPayloadStorage
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils
import com.netflix.conductor.core.utils.IDGenerator
import com.netflix.conductor.core.utils.ParametersUtils
import com.netflix.conductor.dao.MetadataDAO
import java.time.Duration

/**
 * Factory for creating DeciderService without Spring context.
 * This validates POC 1: DeciderService can work standalone.
 */
class DeciderServiceFactory(
    private val metadataDAO: MetadataDAO = InMemoryMetadataDAO()
) {
    private val objectMapper: ObjectMapper = jacksonObjectMapper()

    fun create(): DeciderService {
        // 1. IDGenerator - simple UUID generator
        val idGenerator = IDGenerator()

        // 2. ParametersUtils - handles expression evaluation
        val parametersUtils = ParametersUtils(objectMapper)

        // 3. ExternalPayloadStorageUtils - for large payloads (dummy impl)
        val externalPayloadStorage = DummyPayloadStorage()
        val conductorProperties = ConductorProperties()
        val externalPayloadStorageUtils = ExternalPayloadStorageUtils(
            externalPayloadStorage,
            conductorProperties,
            objectMapper
        )

        // 4. SystemTaskRegistry - empty for now (no system tasks needed for SIMPLE tasks)
        val systemTaskRegistry = SystemTaskRegistry(emptySet())

        // 5. TaskMappers - map workflow task types to task models
        val taskMappers = createTaskMappers(parametersUtils)

        // 6. Task pending time threshold
        val taskPendingTimeThreshold = Duration.ofMinutes(60)

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

        return mappers
    }
}
