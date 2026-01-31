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
 * Deterministic ID generator for Temporal workflows.
 *
 * <p>Generates IDs in the format: {prefix}-{sequence}
 * where prefix is typically the workflow run ID and sequence is an incrementing counter.
 *
 * <p>Example IDs:
 * <ul>
 *   <li>"abc123-1" (first task)</li>
 *   <li>"abc123-2" (second task)</li>
 *   <li>"abc123-3" (third task)</li>
 * </ul>
 *
 * <p>This ensures:
 * <ol>
 *   <li>IDs are unique within a workflow execution</li>
 *   <li>IDs are deterministic on replay (same sequence produces same IDs)</li>
 *   <li>IDs are still globally unique (workflow run IDs are unique)</li>
 * </ol>
 */
public class DeterministicIdGenerator implements IdGeneratorProvider {

    private final String prefix;
    private long sequence = 0;

    /**
     * Creates a new DeterministicIdGenerator with the specified prefix.
     *
     * @param prefix the prefix for generated IDs (typically the workflow run ID)
     */
    public DeterministicIdGenerator(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Creates a new DeterministicIdGenerator with the specified prefix and starting sequence.
     * Used when restoring from a continue-as-new checkpoint.
     *
     * @param prefix the prefix for generated IDs (typically the workflow run ID)
     * @param startSequence the sequence number to start from
     */
    public DeterministicIdGenerator(String prefix, long startSequence) {
        this.prefix = prefix;
        this.sequence = startSequence;
    }

    @Override
    public String generate() {
        sequence++;
        return prefix + "-" + sequence;
    }

    @Override
    public void reset() {
        sequence = 0;
    }

    /**
     * Get current sequence number (for debugging/logging).
     *
     * @return the current sequence number
     */
    public long getCurrentSequence() {
        return sequence;
    }
}
