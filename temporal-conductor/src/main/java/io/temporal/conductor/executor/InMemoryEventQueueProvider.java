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
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * An in-memory EventQueueProvider that stores published messages for testing.
 *
 * <p>This provider creates {@link InMemoryObservableQueue} instances that actually
 * store messages, allowing tests to verify that EVENT tasks publish correct data.
 *
 * <p>Features:
 * <ul>
 *   <li>Real message storage (not just logging)</li>
 *   <li>Queue retrieval by name for test verification</li>
 *   <li>Message inspection and clearing</li>
 * </ul>
 *
 * <p>This is NOT a production implementation - use real queue providers for production.
 */
public class InMemoryEventQueueProvider implements EventQueueProvider {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryEventQueueProvider.class);

    private final String queueType;
    private final Map<String, InMemoryObservableQueue> queues = new ConcurrentHashMap<>();

    public InMemoryEventQueueProvider(String queueType) {
        this.queueType = queueType;
        logger.info("Created InMemoryEventQueueProvider for type: {}", queueType);
    }

    @Override
    public String getQueueType() {
        return queueType;
    }

    @Override
    public ObservableQueue getQueue(String queueURI) throws IllegalArgumentException {
        return queues.computeIfAbsent(queueURI, uri -> {
            logger.debug("Creating in-memory queue: {}", uri);
            return new InMemoryObservableQueue(queueType, uri);
        });
    }

    // ==================== Test Helper Methods ====================

    /**
     * Get a specific queue by URI for test verification.
     *
     * @param queueURI the queue URI
     * @return the queue or null if not created
     */
    public InMemoryObservableQueue getQueueForVerification(String queueURI) {
        return queues.get(queueURI);
    }

    /**
     * Get all messages published to all queues of this type.
     *
     * @return list of all published messages across all queues
     */
    public List<Message> getAllPublishedMessages() {
        return queues.values().stream()
                .flatMap(q -> q.getPublishedMessages().stream())
                .collect(Collectors.toList());
    }

    /**
     * Get messages published to a specific queue.
     *
     * @param queueURI the queue URI
     * @return list of messages or empty list if queue doesn't exist
     */
    public List<Message> getMessagesForQueue(String queueURI) {
        InMemoryObservableQueue queue = queues.get(queueURI);
        return queue != null ? queue.getPublishedMessages() : Collections.emptyList();
    }

    /**
     * Get all queue URIs that have been created.
     *
     * @return set of queue URIs
     */
    public java.util.Set<String> getQueueURIs() {
        return Collections.unmodifiableSet(queues.keySet());
    }

    /**
     * Clear all messages from all queues.
     */
    public void clearAll() {
        queues.values().forEach(InMemoryObservableQueue::clear);
        logger.debug("Cleared all in-memory queues for type: {}", queueType);
    }

    /**
     * Get total message count across all queues.
     *
     * @return total number of messages
     */
    public long getTotalMessageCount() {
        return queues.values().stream()
                .mapToLong(InMemoryObservableQueue::size)
                .sum();
    }
}
