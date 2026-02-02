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

package io.temporal.conductor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.utils.ParametersUtils;
import io.temporal.conductor.executor.LoggingEventQueueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.netflix.conductor.core.events.EventQueues.EVENT_QUEUE_PROVIDERS_QUALIFIER;
import static java.util.function.Function.identity;

/**
 * Configuration for Conductor's EventQueues infrastructure.
 *
 * <p>This configuration follows Conductor's pattern of auto-discovering EventQueueProvider beans.
 * By default, provides logging-only queue providers for common queue types.
 *
 * <p>To use real queue providers (Kafka, SQS, etc.), add the appropriate Conductor
 * dependencies and define EventQueueProvider beans. They will be automatically discovered
 * and registered.
 *
 * <p>Example for Kafka:
 * <pre>
 * &#64;Bean
 * public EventQueueProvider kafkaEventQueueProvider() {
 *     return new KafkaEventQueueProvider(...);
 * }
 * </pre>
 */
@Configuration
public class EventQueueConfig {

    private static final Logger logger = LoggerFactory.getLogger(EventQueueConfig.class);

    // Default logging-only providers for common queue types.
    // Users can override by defining their own EventQueueProvider beans.

    @Bean
    @ConditionalOnMissingBean(name = "conductorEventQueueProvider")
    public EventQueueProvider conductorEventQueueProvider() {
        return new LoggingEventQueueProvider("conductor");
    }

    @Bean
    @ConditionalOnMissingBean(name = "sqsEventQueueProvider")
    public EventQueueProvider sqsEventQueueProvider() {
        return new LoggingEventQueueProvider("sqs");
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaEventQueueProvider")
    public EventQueueProvider kafkaEventQueueProvider() {
        return new LoggingEventQueueProvider("kafka");
    }

    @Bean
    @ConditionalOnMissingBean(name = "amqpEventQueueProvider")
    public EventQueueProvider amqpEventQueueProvider() {
        return new LoggingEventQueueProvider("amqp");
    }

    @Bean
    @ConditionalOnMissingBean(name = "natsEventQueueProvider")
    public EventQueueProvider natsEventQueueProvider() {
        return new LoggingEventQueueProvider("nats");
    }

    /**
     * Collects all EventQueueProvider beans and creates a map keyed by queue type.
     * This matches Conductor's ConductorCoreConfiguration pattern.
     */
    @Bean
    @Qualifier(EVENT_QUEUE_PROVIDERS_QUALIFIER)
    public Map<String, EventQueueProvider> eventQueueProviders(List<EventQueueProvider> providers) {
        logger.info("Auto-discovered {} event queue providers", providers.size());
        Map<String, EventQueueProvider> providerMap = providers.stream()
                .collect(Collectors.toMap(EventQueueProvider::getQueueType, identity()));
        logger.info("Event queue providers: {}", providerMap.keySet());
        return providerMap;
    }

    @Bean
    @ConditionalOnMissingBean
    public ParametersUtils parametersUtils(ObjectMapper objectMapper) {
        return new ParametersUtils(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventQueues eventQueues(
            @Qualifier(EVENT_QUEUE_PROVIDERS_QUALIFIER) Map<String, EventQueueProvider> providers,
            ParametersUtils parametersUtils) {
        logger.info("Creating EventQueues with providers: {}", providers.keySet());
        return new EventQueues(providers, parametersUtils);
    }
}
