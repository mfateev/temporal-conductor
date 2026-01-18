# temporal-conductor

Execute Conductor workflows using Temporal as the execution backend.

## Overview

This project provides a system to execute [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflows using Temporal, similar to how `temporal-airflow` executes Airflow DAGs.

## Build Requirements

- **Java**: JDK 11, 17, or 21 (Java 25 not yet supported by Gradle 8.x)
- **Gradle**: 8.12+ (wrapper included)

## Building

```bash
./gradlew build
```

**Note**: Current build requires Java 21 or earlier due to Gradle/Groovy compatibility.

## Modules

- `temporal-conductor-core`: Core library for executing Conductor workflows on Temporal

## Development Status

This is a proof-of-concept implementation demonstrating the feasibility of running Conductor workflows on Temporal.
