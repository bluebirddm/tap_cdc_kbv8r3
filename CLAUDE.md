# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spring Boot application that integrates Debezium connector for Kingbase (金仓数据库) database with Elasticsearch, implementing log-based Change Data Capture (CDC) and batch SQL synchronization. Built on Debezium 1.5.4.Final, Spring Boot 2.7.18, and Elasticsearch 7.17.18.

## Development Commands

### Build and Package
```bash
mvn clean compile           # Compile only
mvn clean package           # Create executable JAR
mvn clean install           # Install to local repository
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn -q -DskipTests package
```

### Run Application
```bash
mvn spring-boot:run         # Run with Maven
java -jar target/debezium-kingbase-1.0-SNAPSHOT.jar  # Run JAR directly
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn spring-boot:run -DskipTests
```

### Run Tests
```bash
mvn test                    # Run all tests
mvn test -Dtest=KingbaseTest  # Run specific test
mvn -q -Dexec.classpathScope=test -Dexec.mainClass=KingbaseTest exec:java  # Run standalone test
```

### Override Kingbase JDBC Driver Path (if needed)
```bash
mvn compile -Dkingbase.jdbc.jar=/path/to/kingbase8.jar
```

## Architecture

### Core Components

1. **Spring Boot Application** (`com.tapdata.cdc`)
   - `TapCdcApplication`: Main entry point with scheduling enabled
   - `config/`: Database and Elasticsearch configuration classes
   - `controller/`: REST API endpoints for search and sync management

2. **Kingbase Integration** (`com.tapdata.cdc.kingbase`)
   - `KingBaseSqlSyncService`: Batch SQL synchronization with chunking and streaming
   - `KingBaseSyncStateRepository`: Cursor persistence for incremental sync
   - `KingBaseInitialSync`: Initial data synchronization

3. **Elasticsearch Integration** (`com.tapdata.cdc.elasticsearch`)
   - `ElasticsearchService`: Bulk indexing with configurable batch size and threading
   - `IndexManager`: Index lifecycle management
   - `SearchService`: Multi-index search capabilities

4. **Debezium Connector** (`io.debezium.connector.kingbasees`)
   - Reuses PostgreSQL connector architecture adapted for Kingbase
   - `PostgresConnector`: Main connector entry point
   - `connection/`: Replication connection handling with logical decoding
   - Protocol buffer message serialization via `proto/` package

### Key Design Patterns

- **Parallel SQL Execution**: Configurable parallelism for multiple SQL statements
- **Chunk-based Processing**: Prevents memory overflow with large result sets
- **Cursor Persistence**: Incremental sync support with automatic resume
- **External Configuration**: YAML files in JAR directory override embedded configs
- **Scheduled Tasks**: Cron-based sync with separate schedules for different table groups

## Configuration Structure

### Application Configuration
- `src/main/resources/application.yml`: Main Spring Boot configuration
- External overrides: `kingbase.yml`, `elasticsearch.yml` in JAR directory
- SQL statements: `sql-statements.yml`, `sql-statements-large.yml`

### Key Properties
- `tap.kingbase.*`: Kingbase connection and sync settings
- `tap.elasticsearch.*`: ES connection and bulk parameters
- `tap.debezium.*`: CDC connector configuration
- `tap.kingbase.statement-parallelism`: Concurrent SQL execution limit
- `tap.kingbase.sql-sync-cron`: Sync schedule expression

## API Endpoints

### Sync Management (port 8080)
```bash
POST   /kingbase/sync              # Trigger immediate sync
DELETE /kingbase/sync-state        # Clear cursor table
PUT    /kingbase/sync-groups/refresh  # Reload statement groups
```

### Search API
```bash
GET  /api/search/global             # Global keyword search
GET  /api/search/global/ids         # Search returning only IDs
GET  /api/search/table              # Search specific table
GET  /api/search/schema             # Search within schema
GET  /api/search/indices            # List available indices
POST /api/search/advanced           # Advanced search with JSON body
```

## Database Requirements

### Kingbase Setup
- Enable logical replication in `sys_hba.conf`
- Create user with replication privileges
- Default port: 54321
- Supports PostgreSQL wire protocol

### Sync State Table
- Automatically created: `kingbase_sync_state`
- Stores cursor position for each SQL statement
- Enables incremental synchronization

## Performance Tuning

### Batch Processing
- `chunk-size`: Rows per database fetch
- `fetch-size`: JDBC fetch size
- `bulk-size`: Elasticsearch bulk operation size
- `bulk-threads`: Parallel bulk indexing threads

### Logging Control
- Sampling for bulk operations: `-Dtap.es.logEvery=5000`
- Log levels in `application.yml` under `logging.level`

## External Dependencies

- Kingbase JDBC driver must be available (default: `/opt/kingbase_bare_metal/ES/V8/Jdbc/jdk/kingbase8-8.2.0.jar`)
- Elasticsearch cluster (default: localhost:9200)
- File-based offset storage for Debezium (./offsets.dat)