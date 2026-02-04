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

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-memory ObservableQueue implementation that stores published messages.
 *
 * <p>This queue provides real message storage for testing EVENT tasks without
 * requiring external message brokers like Kafka or SQS. Messages can be retrieved
 * for verification in tests.
 *
 * <p>This is NOT a production implementation - it's for testing and development.
 * For production, use real queue providers (Kafka, SQS, etc.).
 */
public class InMemoryObservableQueue implements ObservableQueue {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryObservableQueue.class);

    private final String queueType;
    private final String queueName;
    private final List<Message> publishedMessages = new CopyOnWriteArrayList<>();
    private volatile boolean running = false;

    public InMemoryObservableQueue(String queueType, String queueName) {
        this.queueType = queueType;
        this.queueName = queueName;
    }

    @Override
    public Observable<Message> observe() {
        // Return observable over published messages for consumers
        return Observable.from(publishedMessages);
    }

    @Override
    public String getType() {
        return queueType;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public String getURI() {
        return queueType + ":" + queueName;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        logger.debug("ACK {} messages on queue {}", messages.size(), getURI());
        // Remove acknowledged messages
        for (Message msg : messages) {
            publishedMessages.removeIf(m -> m.getId().equals(msg.getId()));
        }
        return Collections.emptyList();
    }

    @Override
    public void publish(List<Message> messages) {
        for (Message message : messages) {
            publishedMessages.add(message);
            logger.info("EVENT published to queue '{}': id={}, payload={}",
                    getURI(), message.getId(), message.getPayload());
        }
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
        // No-op for in-memory queue
    }

    @Override
    public long size() {
        return publishedMessages.size();
    }

    @Override
    public void start() {
        running = true;
        logger.debug("In-memory queue '{}' started", getURI());
    }

    @Override
    public void stop() {
        running = false;
        logger.debug("In-memory queue '{}' stopped", getURI());
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // ==================== Test Helper Methods ====================

    /**
     * Get all published messages for verification.
     *
     * @return unmodifiable list of published messages
     */
    public List<Message> getPublishedMessages() {
        return Collections.unmodifiableList(new ArrayList<>(publishedMessages));
    }

    /**
     * Get the most recently published message.
     *
     * @return the last message or null if empty
     */
    public Message getLastPublishedMessage() {
        if (publishedMessages.isEmpty()) {
            return null;
        }
        return publishedMessages.get(publishedMessages.size() - 1);
    }

    /**
     * Clear all published messages.
     */
    public void clear() {
        publishedMessages.clear();
    }

    /**
     * Check if a message with the given ID was published.
     *
     * @param messageId the message ID to check
     * @return true if a message with this ID was published
     */
    public boolean hasMessage(String messageId) {
        return publishedMessages.stream()
                .anyMatch(m -> m.getId().equals(messageId));
    }
}
