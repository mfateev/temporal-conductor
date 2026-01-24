# Multi-stage Dockerfile for Temporal Conductor
# Builds the unified temporal-conductor module

# ==============================================================================
# Stage 1: Build the application
# ==============================================================================
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /app

# Copy Gradle wrapper and build files
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .

# Copy source code
COPY temporal-conductor temporal-conductor

# Build the server JAR (skip tests for faster build)
RUN chmod +x gradlew && ./gradlew :temporal-conductor:bootJar --no-daemon -x test

# ==============================================================================
# Stage 2: Runtime image
# ==============================================================================
FROM eclipse-temurin:21-jre-alpine AS runtime

WORKDIR /app

# Install wget for health checks
RUN apk add --no-cache wget

# Add non-root user for security
RUN addgroup -S conductor && adduser -S conductor -G conductor

# Copy the built JAR from builder stage
COPY --from=builder /app/temporal-conductor/build/libs/*.jar app.jar

# Change ownership to non-root user
RUN chown -R conductor:conductor /app

USER conductor

# Expose the application port
EXPOSE 8080

# Health check using actuator endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health/liveness || exit 1

# Set JVM options for containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"

# Default to temporal profile in production
ENV SPRING_PROFILES_ACTIVE="temporal"

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
