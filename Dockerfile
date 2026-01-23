# Multi-stage Dockerfile for Conductor Server Temporal
# Builds both the library and server in a single image

# ==============================================================================
# Stage 1: Build the core library
# ==============================================================================
FROM eclipse-temurin:21-jdk-alpine AS library-builder

WORKDIR /library

# Copy library source
COPY conductor-temporal-ext/gradlew .
COPY conductor-temporal-ext/gradle gradle
COPY conductor-temporal-ext/build.gradle .
COPY conductor-temporal-ext/settings.gradle .
COPY conductor-temporal-ext/src src

# Build and publish to local Maven repository
RUN chmod +x gradlew && ./gradlew publishToMavenLocal --no-daemon -x javadoc

# ==============================================================================
# Stage 2: Build the server application
# ==============================================================================
FROM eclipse-temurin:21-jdk-alpine AS server-builder

WORKDIR /app

# Copy Maven local repository from library build
COPY --from=library-builder /root/.m2 /root/.m2

# Copy server source
COPY conductor-server-temporal/gradlew .
COPY conductor-server-temporal/gradle gradle
COPY conductor-server-temporal/build.gradle .
COPY conductor-server-temporal/settings.gradle .
COPY conductor-server-temporal/src src

# Build the server JAR (skip tests for faster build)
RUN chmod +x gradlew && ./gradlew bootJar --no-daemon -x test -x checkstyleMain -x checkstyleTest

# ==============================================================================
# Stage 3: Runtime image
# ==============================================================================
FROM eclipse-temurin:21-jre-alpine AS runtime

WORKDIR /app

# Install wget for health checks
RUN apk add --no-cache wget

# Add non-root user for security
RUN addgroup -S conductor && adduser -S conductor -G conductor

# Copy the built JAR from builder stage
COPY --from=server-builder /app/build/libs/*.jar app.jar

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
