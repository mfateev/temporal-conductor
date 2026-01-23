plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "io.temporal.conductor"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Conductor dependencies (org.conductoross group)
    implementation("org.conductoross:conductor-core:3.21.23")
    implementation("org.conductoross:conductor-common:3.21.23")

    // Spring dependencies (required by ConductorProperties)
    implementation("org.springframework.boot:spring-boot:3.3.5")
    implementation("org.springframework:spring-context:6.1.14")

    // Jackson for JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.3")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.3")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("io.temporal.conductor.poc1.MainKt")
}
