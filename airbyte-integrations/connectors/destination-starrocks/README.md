# StarRocks Destination Connector

High-performance Kotlin-based destination connector for [StarRocks](https://www.starrocks.io/) and [CelerData](https://www.celerdata.com/).

## Overview

This connector uses the StarRocks Stream Load API for high-performance bulk loading, achieving throughput of 4+ MB/s for cloud-to-cloud syncs.

### Key Features

- **Stream Load API**: Uses HTTP-based bulk loading instead of INSERT statements (10-40x faster)
- **Cross-state Buffering**: Accumulates records across state messages for optimal batch sizes (50K records)
- **Typed Mode**: Creates properly typed tables with column-level data types
- **Raw Mode**: Stores all data as JSON for maximum schema flexibility
- **SSL Support**: Works with cloud StarRocks/CelerData clusters
- **Auto-discovery**: Automatically detects HTTPS (port 443) or HTTP (port 8030) for Stream Load

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| host | string | yes | - | StarRocks FE hostname |
| port | integer | no | 9030 | MySQL protocol port |
| http_port | integer | no | 8030 | HTTP port for Stream Load API |
| username | string | yes | - | Authentication username |
| password | string | yes | - | Authentication password (secret) |
| database | string | yes | - | Target database name |
| ssl | boolean | no | false | Use SSL/TLS encryption |
| loading_mode | enum | no | typed | Data loading mode (typed/raw) |

## Loading Modes

### Typed Mode (Recommended)

Creates tables with proper column types based on source schema:

```sql
CREATE TABLE products (
    id BIGINT,
    name STRING,
    price DOUBLE,
    created_at DATETIME,
    metadata JSON,
    _airbyte_ab_id VARCHAR(36),
    _airbyte_emitted_at DATETIME
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id)
```

**Benefits:**
- Direct SQL queries without JSON extraction
- Better query performance
- Proper data types and indexing
- Schema enforcement

### Raw Mode

Stores all data as JSON in a single column:

```sql
CREATE TABLE products (
    _airbyte_ab_id VARCHAR(36),
    _airbyte_emitted_at DATETIME,
    _airbyte_data JSON
)
UNIQUE KEY(_airbyte_ab_id)
DISTRIBUTED BY HASH(_airbyte_ab_id)
```

**Benefits:**
- Maximum schema flexibility
- Handles frequent schema changes
- No schema migration needed

## Performance

Based on testing with 8M rows (3.69 GB):

- **Throughput**: 4.08 MB/s (cloud-to-cloud)
- **Records/sec**: 8,839 records/second
- **Batch size**: 50,000 records per Stream Load
- **Load time**: ~3.5 seconds per 50K batch

**Comparison:**
- 16% faster than raw mode
- 45x faster than MotherDuck destination

## Architecture

```
Source → Airbyte → StarRocks Destination
                    ├─ MySQL Protocol (DDL)
                    └─ Stream Load API (Bulk Data)
```

### Stream Load Flow

1. **Buffer Accumulation**: Records accumulate in memory (50K batch)
2. **CSV Generation**: Convert records to CSV format
3. **Stream Load**: HTTP PUT to `https://host:443/api/db/table/_stream_load`
4. **State Emission**: Emit state messages after successful load

## Cloud StarRocks / CelerData

For cloud deployments:
- Uses HTTPS on port 443 automatically
- SSL certificate verification disabled for self-signed certs
- Authentication via HTTP Basic Auth

## Development

### Build

```bash
./gradlew :airbyte-integrations:connectors:destination-starrocks:build
```

### Test

```bash
./gradlew :airbyte-integrations:connectors:destination-starrocks:test
```

### Local Run

```bash
./gradlew :airbyte-integrations:connectors:destination-starrocks:run
```

## Type Mapping

| Airbyte Type | StarRocks Type |
|--------------|----------------|
| string | STRING |
| integer | BIGINT |
| number | DOUBLE |
| boolean | BOOLEAN |
| array | JSON |
| object | JSON |
| timestamp_with_timezone | DATETIME |
| date | DATE |
| date-time | DATETIME |

## Changelog

### 0.1.0 (Alpha)
- Initial Kotlin implementation
- Stream Load API support
- Typed and raw modes
- SSL support
- Cross-state buffering
