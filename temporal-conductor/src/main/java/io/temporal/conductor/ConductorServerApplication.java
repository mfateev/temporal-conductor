package io.temporal.conductor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application for the Conductor server with Temporal backend.
 */
@SpringBootApplication
public class ConductorServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConductorServerApplication.class, args);
    }
}
