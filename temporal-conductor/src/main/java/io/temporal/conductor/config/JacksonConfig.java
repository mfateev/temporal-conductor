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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Jackson configuration for proper JSON serialization.
 *
 * <p>Handles edge cases like zero timestamps that the Conductor UI can't parse.
 * The UI throws "Invalid time value" errors when timestamps are 0.
 */
@Configuration
public class JacksonConfig {

    // Fields that are timestamps or timeout values that should be null instead of 0
    // The Conductor UI can't parse 0 as a valid timestamp
    private static final Set<String> TIMESTAMP_FIELDS = Set.of(
            "createTime", "updateTime", "startTime", "endTime",
            "scheduledTime", "queueWaitTime", "callbackAfterSeconds",
            "responseTimeoutSeconds", "startDelayInSeconds", "firstStartTime",
            "timeoutSeconds", "startDelay"
    );

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Register module that converts 0 timestamps to null
        SimpleModule module = new SimpleModule();
        module.setSerializerModifier(new TimestampSerializerModifier());
        mapper.registerModule(module);

        return mapper;
    }

    /**
     * Modifies bean serializers to convert 0 timestamps to null.
     */
    private static class TimestampSerializerModifier extends BeanSerializerModifier {
        @Override
        public List<BeanPropertyWriter> changeProperties(
                SerializationConfig config,
                BeanDescription beanDesc,
                List<BeanPropertyWriter> beanProperties) {

            for (int i = 0; i < beanProperties.size(); i++) {
                BeanPropertyWriter writer = beanProperties.get(i);
                String name = writer.getName();

                // Check if this is a timestamp field with numeric type
                if (TIMESTAMP_FIELDS.contains(name)) {
                    Class<?> rawClass = writer.getType().getRawClass();
                    if (rawClass == long.class || rawClass == Long.class
                            || rawClass == int.class || rawClass == Integer.class) {
                        beanProperties.set(i, new ZeroToNullPropertyWriter(writer));
                    }
                }
            }
            return beanProperties;
        }
    }

    /**
     * Property writer that converts 0 or negative values to null for timestamp fields.
     */
    private static class ZeroToNullPropertyWriter extends BeanPropertyWriter {
        private final BeanPropertyWriter delegate;

        ZeroToNullPropertyWriter(BeanPropertyWriter delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        public void serializeAsField(Object bean, JsonGenerator gen, SerializerProvider prov)
                throws Exception {
            Object value = delegate.get(bean);

            // Convert 0, negative, or null to JSON null
            // Negative timestamps/durations are invalid and cause UI errors
            // Always write null (even if NON_NULL is set) because the UI expects these fields
            if (value == null || (value instanceof Number && ((Number) value).longValue() <= 0)) {
                gen.writeNullField(delegate.getName());
            } else {
                delegate.serializeAsField(bean, gen, prov);
            }
        }
    }
}
