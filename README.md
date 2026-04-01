# CDC Service

SQL Server Change Data Capture (CDC) service that processes document changes and publishes them to Kafka.

## Overview

This example .NET 8 worker service monitors SQL Server database changes using CDC (Change Data Capture) technology. It tracks changes to documents and publishes them to message broker for downstream processing.

## Architecture

```
CDCService (Main Application)
├── Domain (Business Logic & Models)
├── Kafka (Kafka Publisher Service)
└── Repository (Data Access Layer)
```

## Features

- **LSN Tracking**: Uses Log Sequence Numbers to track processed changes
- **Batch Processing**: Processes changes in configurable batches
- **Kafka Integration**: Idempotent message publishing with configurable retries
- **Retry Mechanism**: Handles transient faults with configurable retry delays
- **Multi-Document Support**: Processes multiple document types simultaneously
- **Docker Support**: Ready for containerized deployment

## Prerequisites

- .NET 8.0 SDK
- SQL Server with CDC enabled
- Kafka broker
- Docker (optional, for containerized deployment)

## Project Structure

```
CDCService/
├── CDCService/          # Main application
│   ├── CdcWorker.cs     # Background service for CDC processing
│   ├── Program.cs       # Application entry point
│   └── Models/          # Data models
├── Domain/              # Business logic
├── Kafka/               # Kafka integration
├── Repository/          # Data access layer
└── settings/            # Configuration files
    └── sql/             # SQL queries
        └── CdcQueries.yaml
```

## SQL Server CDC Setup

Enable CDC on your database:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo',
    @source_name = 'Documents',
    @role_name = NULL,
    @supports_net_changes = 0;
```

Create a table to track processed changes in your database:

```sql
CREATE TABLE cdc.ProcessedLSN (
	Id INT IDENTITY(1,1) PRIMARY KEY,
	LastLSN BINARY(10) NOT NULL,
	TableName NVARCHAR(100) NOT NULL
);
```

## License

For education purposes only.
