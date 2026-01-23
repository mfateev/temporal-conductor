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

import com.netflix.conductor.core.utils.IDGenerator;

/**
 * Adapter that wraps an IdGeneratorProvider to implement Conductor's IDGenerator interface.
 */
public class TemporalIdGenerator extends IDGenerator {

    private final IdGeneratorProvider provider;

    /**
     * Creates a new TemporalIdGenerator wrapping the specified provider.
     *
     * @param provider the IdGeneratorProvider to delegate to
     */
    public TemporalIdGenerator(IdGeneratorProvider provider) {
        this.provider = provider;
    }

    @Override
    public String generate() {
        return provider.generate();
    }
}
