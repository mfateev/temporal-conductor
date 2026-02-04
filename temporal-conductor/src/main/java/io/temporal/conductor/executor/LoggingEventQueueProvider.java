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

import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;

/**
 * A stub EventQueueProvider that returns LoggingObservableQueue instances.
 * This provider supports any queue type by logging messages instead of publishing
 * to a real event system.
 */
public class LoggingEventQueueProvider implements EventQueueProvider {

    private final String queueType;

    public LoggingEventQueueProvider(String queueType) {
        this.queueType = queueType;
    }

    @Override
    public String getQueueType() {
        return queueType;
    }

    @Override
    public ObservableQueue getQueue(String queueURI) throws IllegalArgumentException {
        return new LoggingObservableQueue(queueType, queueURI);
    }
}
