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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.execution.mapper.DoWhileTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.JoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.SetVariableTaskMapper;
import com.netflix.conductor.core.execution.mapper.SimpleTaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.TerminateTaskMapper;
import com.netflix.conductor.core.execution.mapper.UserDefinedTaskMapper;
import com.netflix.conductor.core.execution.mapper.WaitTaskMapper;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.storage.DummyPayloadStorage;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating DeciderService with Temporal-compatible components.
 *
 * <p>This factory allows injection of a custom ID generator for deterministic
 * workflow replay in Temporal.
 *
 * <p>Usage:
 * <pre>
 * // For Temporal workflows (deterministic)
 * TemporalDeciderServiceFactory factory = TemporalDeciderServiceFactory.forTemporalWorkflow(
 *     metadataDAO,
 *     workflowRunId
 * );
 *
 * // For non-Temporal use (default UUID behavior)
 * TemporalDeciderServiceFactory factory = TemporalDeciderServiceFactory.withDefaults(metadataDAO);
 * </pre>
 */
public class TemporalDeciderServiceFactory {

    private final MetadataDAO metadataDao;
    private final IdGeneratorProvider idGeneratorProvider;
    private final ObjectMapper objectMapper;
    private final Duration taskPendingTimeThreshold;

    /**
     * Creates a new factory with the specified configuration.
     *
     * @param metadataDao the MetadataDAO to use
     * @param idGeneratorProvider the ID generator provider
     * @param objectMapper the ObjectMapper for JSON processing
     * @param taskPendingTimeThreshold the task pending time threshold
     */
    public TemporalDeciderServiceFactory(
            MetadataDAO metadataDao,
            IdGeneratorProvider idGeneratorProvider,
            ObjectMapper objectMapper,
            Duration taskPendingTimeThreshold) {
        this.metadataDao = metadataDao;
        this.idGeneratorProvider = idGeneratorProvider;
        this.objectMapper = objectMapper;
        this.taskPendingTimeThreshold = taskPendingTimeThreshold;
    }

    /**
     * Creates a new factory with default ObjectMapper and task pending threshold.
     *
     * @param metadataDao the MetadataDAO to use
     * @param idGeneratorProvider the ID generator provider
     */
    public TemporalDeciderServiceFactory(MetadataDAO metadataDao,
                                          IdGeneratorProvider idGeneratorProvider) {
        this(metadataDao, idGeneratorProvider, new ObjectMapper(), Duration.ofMinutes(60));
    }

    /**
     * Create a DeciderService configured for Temporal workflow execution.
     *
     * @return a new DeciderService instance
     */
    public DeciderService create() {
        // Wrap the provider in our adapter
        TemporalIdGenerator idGenerator = new TemporalIdGenerator(idGeneratorProvider);

        // Create ParametersUtils for expression evaluation
        ParametersUtils parametersUtils = new ParametersUtils(objectMapper);

        // Create ExternalPayloadStorageUtils (dummy implementation)
        DummyPayloadStorage externalPayloadStorage = new DummyPayloadStorage();
        ConductorProperties conductorProperties = new ConductorProperties();
        ExternalPayloadStorageUtils externalPayloadStorageUtils = new ExternalPayloadStorageUtils(
                externalPayloadStorage,
                conductorProperties,
                objectMapper
        );

        // Create SystemTaskRegistry (empty for now)
        SystemTaskRegistry systemTaskRegistry = new SystemTaskRegistry(Collections.emptySet());

        // Create TaskMappers
        Map<String, TaskMapper> taskMappers = createTaskMappers(parametersUtils);

        return new DeciderService(
                idGenerator,
                parametersUtils,
                metadataDao,
                externalPayloadStorageUtils,
                systemTaskRegistry,
                taskMappers,
                taskPendingTimeThreshold
        );
    }

    private Map<String, TaskMapper> createTaskMappers(ParametersUtils parametersUtils) {
        Map<String, TaskMapper> mappers = new HashMap<>();

        // SimpleTaskMapper for SIMPLE task type
        SimpleTaskMapper simpleMapper = new SimpleTaskMapper(parametersUtils);
        mappers.put(TaskType.SIMPLE.name(), simpleMapper);

        // UserDefinedTaskMapper as fallback for custom task types
        UserDefinedTaskMapper userDefinedMapper =
                new UserDefinedTaskMapper(parametersUtils, metadataDao);
        mappers.put(TaskType.USER_DEFINED.name(), userDefinedMapper);

        // System task mappers
        ForkJoinTaskMapper forkJoinMapper = new ForkJoinTaskMapper();
        mappers.put(TaskType.FORK_JOIN.name(), forkJoinMapper);

        JoinTaskMapper joinMapper = new JoinTaskMapper();
        mappers.put(TaskType.JOIN.name(), joinMapper);

        DoWhileTaskMapper doWhileMapper = new DoWhileTaskMapper(metadataDao, parametersUtils);
        mappers.put(TaskType.DO_WHILE.name(), doWhileMapper);

        WaitTaskMapper waitMapper = new WaitTaskMapper(parametersUtils);
        mappers.put(TaskType.WAIT.name(), waitMapper);

        SetVariableTaskMapper setVariableMapper = new SetVariableTaskMapper();
        mappers.put(TaskType.SET_VARIABLE.name(), setVariableMapper);

        TerminateTaskMapper terminateMapper = new TerminateTaskMapper(parametersUtils);
        mappers.put(TaskType.TERMINATE.name(), terminateMapper);

        return mappers;
    }

    /**
     * Create a factory configured for deterministic Temporal workflow execution.
     *
     * @param metadataDao the MetadataDAO to use
     * @param workflowRunId the workflow run ID for deterministic ID generation
     * @return a new factory instance
     */
    public static TemporalDeciderServiceFactory forTemporalWorkflow(
            MetadataDAO metadataDao,
            String workflowRunId) {
        return new TemporalDeciderServiceFactory(
                metadataDao,
                new DeterministicIdGenerator(workflowRunId)
        );
    }

    /**
     * Create a factory with default (non-deterministic) ID generation.
     *
     * @param metadataDao the MetadataDAO to use
     * @return a new factory instance
     */
    public static TemporalDeciderServiceFactory withDefaults(MetadataDAO metadataDao) {
        return new TemporalDeciderServiceFactory(
                metadataDao,
                new DefaultIdGeneratorProvider()
        );
    }
}
