# StarRocks Backup & Restore - CLI Usage Guide

## Overview

The StarRocks Backup & Restore tool provides production-grade automation for backup and restore operations.

## Installation

```bash
# Activate virtual environment
source .venv/bin/activate

# The CLI is already installed as: starrocks-br
```

## Configuration

Create a `config.yaml` file with your StarRocks connection details:

```yaml
host: "127.0.0.1"
port: 9030
user: "root"
database: "your_database"
repository: "your_repo_name"
```

**Password Management**

The database password must be provided via the `STARROCKS_PASSWORD` environment variable. This is a security measure to prevent storing credentials in configuration files.

```bash
export STARROCKS_PASSWORD="your_password"
```

**Note:** The repository must be created in StarRocks using the `CREATE REPOSITORY` command before running backups. For example:

```sql
CREATE REPOSITORY `your_repo_name`
WITH S3
ON LOCATION "s3://your-backup-bucket/backups/"
PROPERTIES (
    "aws.s3.access_key" = "your-access-key",
    "aws.s3.secret_key" = "your-secret-key",
    "aws.s3.endpoint" = "https://s3.amazonaws.com"
);
```

## Commands

### Initialize Schema

Before running backups, initialize the ops database and control tables:

```bash
starrocks-br init --config config.yaml
```

**What it does:**
- Creates `ops` database
- Creates `ops.table_inventory`: Inventory groups mapping to databases/tables
- Creates `ops.backup_history`: Backup operation history
- Creates `ops.restore_history`: Restore operation history
- Creates `ops.run_status`: Job concurrency control

**Next step:** Populate `ops.table_inventory` with your backup groups. For example:
```sql
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('daily_facts', 'your_db', 'fact_sales'),
  ('weekly_dims', 'your_db', 'dim_users'),
  ('weekly_dims', 'your_db', 'dim_products'),
  ('full_db_backup', 'your_db', '*'); -- Wildcard for all tables
```

**Note:** If you skip this step, the ops schema will be auto-created on your first backup/restore operation (with a warning).

### Backup Commands

Backups are managed through "inventory groups" defined in `ops.table_inventory`. This provides a flexible way to schedule different backup strategies for different sets of tables.

#### 1. Full Backup

Runs a full backup for all tables within a specified inventory group.

```bash
starrocks-br backup full --config config.yaml --group <group_name>
```

**Parameters:**
- `--group`: The inventory group to back up.

**Flow:**
1. Load config → verify cluster health → ensure repository exists
2. Reserve job slot (prevent concurrent backups)
3. Query `ops.table_inventory` for all tables in the specified group.
4. Generate a unique backup label.
5. Build and execute the `BACKUP` command for the resolved tables.
6. Poll `SHOW BACKUP` until completion and log results.

#### 2. Incremental Backup

Backs up only the partitions that have changed since the last successful full backup for a given inventory group.

```bash
starrocks-br backup incremental --config config.yaml --group <group_name>
```

**Parameters:**
- `--group`: The inventory group to back up.
- `--baseline-backup` (Optional): Specify a backup label to use as the baseline instead of the latest full backup.

**Flow:**
1. Load config → verify cluster health → ensure repository exists
2. Reserve job slot
3. Find the latest successful full backup for the group to use as a baseline.
4. Find recent partitions from `information_schema.partitions` for tables in the group.
5. Generate a unique backup label.
6. Build and execute the `BACKUP` command for the new partitions.
7. Poll `SHOW BACKUP` until completion and log results.

### Restore Commands

#### Restore Single Partition

Restores a specific partition from a backup snapshot.

```bash
starrocks-br restore-partition \
  --config config.yaml \
  --backup-label my_db_20251016_inc \
  --table my_db.fact_sales \
  --partition p20251016
```

**Parameters:**
- `--backup-label`: The snapshot label to restore from
- `--table`: Fully qualified table name (database.table)
- `--partition`: Partition name to restore

**Flow:**
1. Load config
2. Build `RESTORE SNAPSHOT ... ON (TABLE ... PARTITION (...))` command
3. Execute restore
4. Poll `SHOW RESTORE` until completion
5. Log to `ops.restore_history`

## Example Usage Scenarios

### Initial Setup

```bash
# 1. Initialize ops schema (run once)
starrocks-br init --config config.yaml

# 2. Populate table inventory with your groups (in StarRocks)
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('daily_incrementals', 'sales_db', 'fact_orders'),
  ('weekly_full', 'sales_db', 'dim_customers'),
  ('weekly_full', 'sales_db', 'dim_products');
```

### Daily Incremental Backup (Mon-Sat)

```bash
# Run via cron at 01:00
0 1 * * 1-6 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup incremental --config config.yaml --group daily_incrementals
```

### Weekly Full Backup (Sunday)

```bash
# Run via cron at 01:00 on Sundays
0 1 * * 0 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup full --config config.yaml --group weekly_full
```

### Disaster Recovery - Restore Recent Partition

```bash
# Restore yesterday's partition from incremental backup
starrocks-br restore-partition \
  --config config.yaml \
  --backup-label sales_db_20251015_inc \
  --table sales_db.fact_sales \
  --partition p20251015
```

## Error Handling

The CLI automatically handles:

- **Job slot conflicts**: Prevents overlapping backups/restores via `ops.run_status`
- **Label collisions**: Automatically appends `_r#` suffix if label exists
- **Cluster health**: Verifies FE/BE status before starting operations
- **Repository validation**: Ensures repository exists and is accessible
- **Graceful failures**: All errors are logged to history tables with proper status

## Monitoring

All operations are logged to:
- `ops.backup_history`: Tracks all backup attempts with status, timestamps, and error messages
- `ops.restore_history`: Tracks all restore operations with verification checksums
- `ops.run_status`: Tracks active jobs to prevent conflicts

Query examples:

```sql
-- Check recent backup status
SELECT label, backup_type, status, started_at, finished_at
FROM ops.backup_history
ORDER BY started_at DESC
LIMIT 10;

-- Check for failed backups
SELECT label, backup_type, error_message, started_at
FROM ops.backup_history
WHERE status = 'FAILED'
ORDER BY started_at DESC;

-- Check active jobs
SELECT scope, label, state, started_at
FROM ops.run_status
WHERE state = 'ACTIVE';
```

## Testing

The project includes comprehensive tests (150+ tests, 90%+ coverage).

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src/starrocks_br --cov-report=term-missing

# Run specific test file
pytest tests/test_cli.py -v
```

## Project Status

✅ **Completed:**
- Config loader & validation
- Database connection wrapper
- StarRocks repository management
- Cluster health checks
- Job slot reservation (concurrency control)
- Label generation with collision handling
- Group-based backup planners for full and incremental backups
- Schema initialization (ops tables) with auto-creation
- Backup & restore history logging
- Backup executor with polling
- Restore operations with polling
- CLI commands (init, backup full, backup incremental, restore-partition)

📋 **Optional (deferred):**
- Exponential backoff retry for job conflicts
- Disk space precheck (requires external monitoring)
- Formal runbooks and DR drill procedures
- Monitoring dashboards and alerting integration