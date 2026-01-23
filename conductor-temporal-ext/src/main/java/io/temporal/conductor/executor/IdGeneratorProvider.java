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

/**
 * Extension point for ID generation in Temporal workflows.
 *
 * <p>The default Conductor IDGenerator uses UUID.randomUUID() which is non-deterministic
 * and breaks Temporal workflow replay. This interface allows injecting a deterministic
 * ID generator that uses workflow-scoped counters.
 *
 * <p>Usage in Temporal workflows:
 * <pre>
 * IdGeneratorProvider idGenerator = new DeterministicIdGenerator(workflowRunId);
 * TemporalDeciderServiceFactory factory = new TemporalDeciderServiceFactory(metadataDAO, idGenerator);
 * DeciderService deciderService = factory.create();
 * </pre>
 */
public interface IdGeneratorProvider {

    /**
     * Generate a unique ID for a task or workflow.
     *
     * <p>For Temporal workflows, this MUST be deterministic - the same sequence
     * of calls must produce the same IDs on replay.
     *
     * @return a unique ID
     */
    String generate();

    /**
     * Reset the generator state (optional).
     * Called when starting a new workflow instance.
     */
    default void reset() {
    }
}
