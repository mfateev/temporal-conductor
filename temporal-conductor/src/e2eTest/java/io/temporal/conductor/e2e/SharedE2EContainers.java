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

package io.temporal.conductor.e2e;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

/**
 * Singleton holder for shared Docker Compose containers used by E2E tests.
 *
 * <p>This class ensures that containers are started once and reused across all
 * test classes, significantly reducing E2E test execution time.
 *
 * <p>Containers are automatically stopped via JVM shutdown hook when all tests complete.
 */
public final class SharedE2EContainers {

    private static final Logger log = LoggerFactory.getLogger(SharedE2EContainers.class);

    private static final String TEMPORAL_SERVICE = "temporal";
    private static final int TEMPORAL_PORT = 7233;
    private static final String CONDUCTOR_SERVICE = "conductor-server";
    private static final int CONDUCTOR_PORT = 8080;
    private static final String NAMESPACE = "conductor";

    private static volatile SharedE2EContainers instance;
    private static final Object lock = new Object();

    private final ComposeContainer composeContainer;
    private final WorkflowServiceStubs workflowServiceStubs;
    private final WorkflowClient workflowClient;
    private final WebClient conductorClient;

    private SharedE2EContainers() {
        File composeFile = new File("src/e2eTest/resources/docker-compose-e2e.yml");
        if (!composeFile.exists()) {
            throw new IllegalStateException("docker-compose-e2e.yml not found at: " + composeFile.getAbsolutePath());
        }

        log.info("Starting shared Docker Compose stack from: {}", composeFile.getAbsolutePath());

        composeContainer = new ComposeContainer(composeFile)
                .withExposedService(TEMPORAL_SERVICE, TEMPORAL_PORT,
                        Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(2)))
                .withExposedService(CONDUCTOR_SERVICE, CONDUCTOR_PORT,
                        Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(2)))
                .withLocalCompose(true);

        composeContainer.start();

        // Get mapped ports
        String temporalHost = composeContainer.getServiceHost(TEMPORAL_SERVICE, TEMPORAL_PORT);
        int temporalPort = composeContainer.getServicePort(TEMPORAL_SERVICE, TEMPORAL_PORT);
        String temporalAddress = temporalHost + ":" + temporalPort;

        String conductorHost = composeContainer.getServiceHost(CONDUCTOR_SERVICE, CONDUCTOR_PORT);
        int conductorPort = composeContainer.getServicePort(CONDUCTOR_SERVICE, CONDUCTOR_PORT);
        String conductorBaseUrl = "http://" + conductorHost + ":" + conductorPort;

        log.info("Temporal server available at: {}", temporalAddress);
        log.info("Conductor server available at: {}", conductorBaseUrl);

        // Initialize Temporal client
        workflowServiceStubs = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder()
                        .setTarget(temporalAddress)
                        .build());

        workflowClient = WorkflowClient.newInstance(
                workflowServiceStubs,
                WorkflowClientOptions.newBuilder()
                        .setNamespace(NAMESPACE)
                        .build());

        // Initialize Conductor REST client with increased buffer size for large workflow responses
        int bufferSize = 16 * 1024 * 1024; // 16MB buffer for workflows with many tasks
        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(bufferSize))
                .build();

        conductorClient = WebClient.builder()
                .baseUrl(conductorBaseUrl)
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .exchangeStrategies(strategies)
                .build();

        // Wait for Conductor to be fully ready
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> {
                    try {
                        String health = conductorClient.get()
                                .uri("/actuator/health/readiness")
                                .retrieve()
                                .bodyToMono(String.class)
                                .block(Duration.ofSeconds(5));
                        return health != null && health.contains("UP");
                    } catch (Exception e) {
                        log.debug("Waiting for Conductor readiness: {}", e.getMessage());
                        return false;
                    }
                });

        log.info("Shared Docker Compose stack is ready");

        // Register shutdown hook to stop containers when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down shared E2E containers...");
            if (workflowServiceStubs != null) {
                workflowServiceStubs.shutdown();
            }
            if (composeContainer != null) {
                composeContainer.stop();
            }
            log.info("Shared E2E containers stopped");
        }));
    }

    /**
     * Get the singleton instance, initializing containers on first access.
     */
    public static SharedE2EContainers getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new SharedE2EContainers();
                }
            }
        }
        return instance;
    }

    public ComposeContainer getComposeContainer() {
        return composeContainer;
    }

    public WorkflowServiceStubs getWorkflowServiceStubs() {
        return workflowServiceStubs;
    }

    public WorkflowClient getWorkflowClient() {
        return workflowClient;
    }

    public WebClient getConductorClient() {
        return conductorClient;
    }

    public String getNamespace() {
        return NAMESPACE;
    }
}
