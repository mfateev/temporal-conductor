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
 * Default implementation that wraps Conductor's IDGenerator.
 * Uses UUID.randomUUID() - NOT deterministic, NOT safe for Temporal workflows.
 */
public class DefaultIdGeneratorProvider implements IdGeneratorProvider {

    private final IDGenerator delegate = new IDGenerator();

    @Override
    public String generate() {
        return delegate.generate();
    }
}
