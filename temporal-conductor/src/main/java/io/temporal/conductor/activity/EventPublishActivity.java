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

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Activity interface for publishing events from EVENT tasks.
 * Uses Conductor's EventQueues infrastructure for actual publishing.
 */
@ActivityInterface
public interface EventPublishActivity {

    /**
     * Publish an event message to the specified queue.
     *
     * @param queueName the fully qualified queue name (e.g., "conductor:workflow:event", "sqs:my-queue")
     * @param messageId unique message identifier
     * @param payload JSON-serialized message payload
     */
    @ActivityMethod
    void publish(String queueName, String messageId, String payload);
}
