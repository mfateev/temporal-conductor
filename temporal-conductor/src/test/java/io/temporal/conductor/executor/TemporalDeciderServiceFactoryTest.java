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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for TemporalDeciderServiceFactory.
 *
 * <p>These tests verify that all required TaskMappers are registered in the DeciderService.
 * Missing mappers cause runtime failures when workflows try to schedule tasks of those types.
 *
 * <p>This test would have caught the EventTaskMapper missing from DeciderService bug.
 */
class TemporalDeciderServiceFactoryTest {

    /**
     * All task types that must have mappers registered in the DeciderService.
     * If a mapper is missing for any of these types, workflow scheduling will fail at runtime.
     */
    private static final Set<String> REQUIRED_TASK_TYPES = Set.of(
            TaskType.SIMPLE.name(),
            TaskType.USER_DEFINED.name(),
            TaskType.FORK_JOIN.name(),
            TaskType.JOIN.name(),
            TaskType.DO_WHILE.name(),
            TaskType.WAIT.name(),
            TaskType.SET_VARIABLE.name(),
            TaskType.TERMINATE.name(),
            TaskType.SUB_WORKFLOW.name(),
            TaskType.DYNAMIC.name(),
            TaskType.FORK_JOIN_DYNAMIC.name(),
            TaskType.HUMAN.name(),
            TaskType.EVENT.name()
    );

    @Test
    void testAllRequiredTaskMappersAreRegistered() throws Exception {
        // Given
        InMemoryMetadataDAO metadataDao = new InMemoryMetadataDAO();
        TemporalDeciderServiceFactory factory =
                TemporalDeciderServiceFactory.forTemporalWorkflow(metadataDao, "test-run-id");

        // When
        DeciderService deciderService = factory.create();

        // Then - extract taskMappers using reflection
        Map<String, TaskMapper> taskMappers = extractTaskMappers(deciderService);

        // Verify all required task types have mappers
        for (String taskType : REQUIRED_TASK_TYPES) {
            assertNotNull(taskMappers.get(taskType),
                    "TaskMapper for " + taskType + " should be registered. "
                    + "If this test fails, add the mapper in TemporalDeciderServiceFactory.createTaskMappers()");
        }
    }

    @Test
    void testFactoryCreatesDeciderServiceSuccessfully() {
        // Given
        InMemoryMetadataDAO metadataDao = new InMemoryMetadataDAO();
        TemporalDeciderServiceFactory factory =
                TemporalDeciderServiceFactory.forTemporalWorkflow(metadataDao, "test-run-id");

        // When
        DeciderService deciderService = factory.create();

        // Then
        assertNotNull(deciderService, "DeciderService should not be null");
    }

    @Test
    void testFactoryWithDefaultsCreatesDeciderService() {
        // Given
        InMemoryMetadataDAO metadataDao = new InMemoryMetadataDAO();
        TemporalDeciderServiceFactory factory =
                TemporalDeciderServiceFactory.withDefaults(metadataDao);

        // When
        DeciderService deciderService = factory.create();

        // Then
        assertNotNull(deciderService, "DeciderService should not be null");
    }

    @Test
    void testFactoryWithStartSequenceCreatesDeciderService() throws Exception {
        // Given
        InMemoryMetadataDAO metadataDao = new InMemoryMetadataDAO();
        long startSequence = 100L;
        TemporalDeciderServiceFactory factory =
                TemporalDeciderServiceFactory.forTemporalWorkflow(metadataDao, "test-run-id", startSequence);

        // When
        DeciderService deciderService = factory.create();

        // Then
        assertNotNull(deciderService, "DeciderService should not be null");

        // Verify mappers are still registered
        Map<String, TaskMapper> taskMappers = extractTaskMappers(deciderService);
        assertTrue(taskMappers.containsKey(TaskType.SIMPLE.name()),
                "SIMPLE TaskMapper should be registered");
        assertTrue(taskMappers.containsKey(TaskType.EVENT.name()),
                "EVENT TaskMapper should be registered");
    }

    /**
     * Extract the taskMappers field from DeciderService using reflection.
     * This is needed because DeciderService doesn't expose a public getter for taskMappers.
     */
    @SuppressWarnings("unchecked")
    private Map<String, TaskMapper> extractTaskMappers(DeciderService deciderService) throws Exception {
        Field taskMappersField = DeciderService.class.getDeclaredField("taskMappers");
        taskMappersField.setAccessible(true);
        return (Map<String, TaskMapper>) taskMappersField.get(deciderService);
    }
}
