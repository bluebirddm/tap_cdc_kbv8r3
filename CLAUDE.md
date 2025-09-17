# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Debezium connector for Kingbase (金仓数据库) database, implementing log-based Change Data Capture (CDC). The project is built on Debezium 1.5.4.final and uses Kingbase's log parsing API to capture database changes.

## Build and Development Commands

### Build
```bash
mvn clean compile
mvn clean package
mvn clean install
```

### Run Tests
```bash
mvn test
mvn test -Dtest=KingbaseTest  # Run specific test
```

### Clean
```bash
mvn clean
```

## Architecture

The connector reuses PostgreSQL connector architecture from Debezium, adapting it for Kingbase database:

- **Main Connector**: `PostgresConnector` class serves as the entry point
- **Configuration**: `PostgresConnectorConfig` handles connector configuration
- **Change Event Processing**: `PostgresChangeEventSourceFactory` and `PostgresChangeRecordEmitter` handle change events
- **Replication**: Uses logical replication via `ReplicationConnection` in the connection package
- **Protocol Buffers**: Uses protobuf for message serialization (proto definitions in `PgProto.java`)

Key architectural decisions:
- The connector leverages PostgreSQL compatibility of Kingbase database
- Uses Debezium's embedded engine for standalone operation
- Supports snapshot modes for initial data synchronization
- File-based offset and history storage for tracking replication progress

## Key Configuration Properties

When configuring the connector:
- `connector.class`: io.debezium.connector.kingbasees.PostgresConnector
- `database.hostname`: Kingbase server host
- `database.port`: Kingbase server port (default: 54321)
- `database.user`: Database user with replication privileges
- `database.password`: Database password
- `database.dbname`: Target database name
- `database.server.name`: Logical name for the server
- `table.include.list`: Tables to monitor (format: schema.table)
- `plugin.name`: Logical decoding plugin (e.g., "decoderbufs")
- `snapshot.mode`: Initial snapshot behavior

## Testing

The project includes a standalone test in `src/test/java/KingbaseTest.java` that demonstrates:
- Setting up the Debezium embedded engine
- Configuring the Kingbase connector
- Processing change events from the database

To run the test with custom database connection:
1. Update connection parameters in KingbaseTest.java
2. Ensure Kingbase database has logical replication enabled
3. Run: `mvn test -Dtest=KingbaseTest`