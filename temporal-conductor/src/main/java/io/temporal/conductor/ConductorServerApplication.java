package io.temporal.conductor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Spring Boot application for the Conductor server with Temporal backend.
 *
 * <p>The component scan includes:
 * <ul>
 *   <li>io.temporal.conductor - Main application components</li>
 *   <li>com.netflix.conductor.tasks - Conductor extension tasks (JSON_JQ_TRANSFORM, etc.)</li>
 * </ul>
 */
@SpringBootApplication
@ComponentScan(basePackages = {"io.temporal.conductor", "com.netflix.conductor.tasks"})
public class ConductorServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConductorServerApplication.class, args);
    }
}
