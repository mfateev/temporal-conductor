# temporal-conductor

Execute Conductor workflows using Temporal as the execution backend.

## Overview

This project provides a system to execute [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflows using Temporal, similar to how `temporal-airflow` executes Airflow DAGs.

## Build Requirements

- **Java**: JDK 11+ (minimum requirement from Conductor)
  - Compatible: Java 11, 17, 21
  - Not compatible: Java 25 (Gradle 8.x limitation)
- **Gradle**: 8.12+ (wrapper included)

## Building

```bash
./gradlew build
```

**Note**: Requires Java 11-21. Java 25 is not yet supported by Gradle 8.x's Groovy compiler.

## Modules

- `temporal-conductor-core`: Core library for executing Conductor workflows on Temporal

## Development Status

This is a proof-of-concept implementation demonstrating the feasibility of running Conductor workflows on Temporal.
