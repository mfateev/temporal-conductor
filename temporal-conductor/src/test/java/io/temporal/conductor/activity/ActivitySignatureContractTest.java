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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import io.temporal.activity.ActivityMethod;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for activity method signatures.
 *
 * <p>These tests verify that activity interfaces have the correct method signatures
 * to match the invocation pattern used by {@code TaskExecutionContextImpl.executeActivityAsync()}.
 *
 * <p>The standard activity invocation pattern is:
 * <pre>
 * activityStub.executeAsync(activityType, TaskExecutionResult.class,
 *     taskRefName,        // arg 0: String
 *     conductorTaskType,  // arg 1: String
 *     inputData           // arg 2: Map&lt;String, Object&gt;
 * );
 * </pre>
 *
 * <p>This test would have caught the EventPublishActivity parameter count mismatch bug
 * where executeEvent() had 2 parameters but executeActivityAsync() passed 3 parameters.
 */
class ActivitySignatureContractTest {

    /**
     * Verify that EventPublishActivity.executeEvent() has the correct signature
     * to match the standard activity invocation pattern.
     *
     * <p>The activity is invoked via executeActivityAsync() which passes:
     * (taskRefName, conductorTaskType, inputData)
     */
    @Test
    void testEventPublishActivitySignatureMatchesInvocationPattern() {
        // Find the executeEvent method with custom name "event-publish"
        Method executeEvent = findActivityMethodByName(EventPublishActivity.class, "event-publish");

        assertNotNull(executeEvent,
                "EventPublishActivity must have a method with @ActivityMethod(name = \"event-publish\")");

        // Verify signature matches standard pattern: (String, String, Map)
        Class<?>[] paramTypes = executeEvent.getParameterTypes();
        assertEquals(3, paramTypes.length,
                "event-publish activity must accept 3 parameters to match executeActivityAsync pattern. "
                + "Expected: (String taskRefName, String conductorTaskType, Map<String, Object> inputData)");
        assertEquals(String.class, paramTypes[0],
                "Parameter 0 should be String (taskRefName)");
        assertEquals(String.class, paramTypes[1],
                "Parameter 1 should be String (conductorTaskType)");
        assertEquals(Map.class, paramTypes[2],
                "Parameter 2 should be Map (inputData)");
    }

    /**
     * Verify that ExtensionTaskExecutionActivity.execute() has the correct signature.
     *
     * <p>Note: ExtensionTaskExecutionActivity uses a different parameter order because
     * it wraps the actual data inside the inputData map. The ExtensionTaskAdapter
     * packages taskType, taskRefName, and original inputData into the activityInput map.
     */
    @Test
    void testExtensionTaskActivityHasExecuteMethod() {
        // Verify the execute method exists
        Method execute = null;
        try {
            execute = ExtensionTaskExecutionActivity.class.getMethod("execute",
                    String.class, String.class, Map.class);
        } catch (NoSuchMethodException e) {
            fail("ExtensionTaskExecutionActivity must have execute(String, String, Map) method");
        }

        assertNotNull(execute, "execute method should exist");

        // Verify parameter types
        Class<?>[] paramTypes = execute.getParameterTypes();
        assertEquals(3, paramTypes.length,
                "execute activity must accept 3 parameters");
        assertEquals(String.class, paramTypes[0], "Parameter 0 should be String");
        assertEquals(String.class, paramTypes[1], "Parameter 1 should be String");
        assertEquals(Map.class, paramTypes[2], "Parameter 2 should be Map");
    }

    /**
     * Verify EventPublishActivity also has the publish() method for direct publishing.
     */
    @Test
    void testEventPublishActivityHasPublishMethod() {
        Method publish = null;
        try {
            publish = EventPublishActivity.class.getMethod("publish",
                    String.class, String.class, String.class);
        } catch (NoSuchMethodException e) {
            fail("EventPublishActivity must have publish(String, String, String) method");
        }

        assertNotNull(publish, "publish method should exist");
    }

    /**
     * Verify that activities with custom names have @ActivityMethod annotation.
     * This ensures the activity can be invoked by the custom name.
     */
    @Test
    void testEventPublishActivityHasActivityMethodAnnotation() {
        Method executeEvent = findActivityMethodByName(EventPublishActivity.class, "event-publish");

        assertNotNull(executeEvent,
                "EventPublishActivity must have a method annotated with @ActivityMethod(name = \"event-publish\")");

        ActivityMethod annotation = executeEvent.getAnnotation(ActivityMethod.class);
        assertNotNull(annotation, "Method should have @ActivityMethod annotation");
        assertEquals("event-publish", annotation.name(),
                "Activity method name should be 'event-publish'");
    }

    /**
     * Find an activity method by its custom name from @ActivityMethod annotation.
     */
    private Method findActivityMethodByName(Class<?> activityClass, String name) {
        return Arrays.stream(activityClass.getMethods())
                .filter(m -> {
                    ActivityMethod am = m.getAnnotation(ActivityMethod.class);
                    return am != null && name.equals(am.name());
                })
                .findFirst()
                .orElse(null);
    }
}
