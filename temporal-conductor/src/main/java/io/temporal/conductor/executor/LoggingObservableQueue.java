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

import java.util.Collections;
import java.util.List;

/**
 * A stub ObservableQueue implementation that logs messages instead of publishing to a real queue.
 * This allows EVENT tasks to complete successfully while providing visibility into what would
 * have been published.
 */
public class LoggingObservableQueue implements ObservableQueue {

    private static final Logger logger = LoggerFactory.getLogger(LoggingObservableQueue.class);

    private final String queueType;
    private final String queueName;

    public LoggingObservableQueue(String queueType, String queueName) {
        this.queueType = queueType;
        this.queueName = queueName;
    }

    @Override
    public Observable<Message> observe() {
        return Observable.empty();
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
        return Collections.emptyList();
    }

    @Override
    public void publish(List<Message> messages) {
        for (Message message : messages) {
            logger.info("EVENT published to queue '{}': id={}, payload={}",
                    getURI(), message.getId(), message.getPayload());
        }
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
        // No-op for logging queue
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void start() {
        // No-op
    }

    @Override
    public void stop() {
        // No-op
    }

    @Override
    public boolean isRunning() {
        return true;
    }
}
