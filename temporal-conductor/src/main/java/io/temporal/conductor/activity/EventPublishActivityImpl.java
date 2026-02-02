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

package io.temporal.conductor.activity;

import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Activity implementation for publishing events using Conductor's EventQueues infrastructure.
 * Configuration follows Conductor's pattern - EventQueueProviders are injected via Spring.
 */
@Component
public class EventPublishActivityImpl implements EventPublishActivity {

    private static final Logger logger = LoggerFactory.getLogger(EventPublishActivityImpl.class);

    private final EventQueues eventQueues;

    public EventPublishActivityImpl(EventQueues eventQueues) {
        this.eventQueues = eventQueues;
    }

    @Override
    public void publish(String queueName, String messageId, String payload) {
        logger.info("Publishing event to queue '{}': messageId={}", queueName, messageId);
        logger.debug("Event payload: {}", payload);

        ObservableQueue queue = eventQueues.getQueue(queueName);
        Message message = new Message(messageId, payload, messageId);
        queue.publish(List.of(message));

        logger.debug("Event published successfully to queue '{}'", queueName);
    }
}
