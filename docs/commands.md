# Command Reference

Detailed reference for all StarRocks Backup & Restore commands.

## init

Initialize the ops database and control tables.

### Syntax

```bash
starrocks-br init --config <config_file>
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--config` | Yes | Path to configuration file |

### What It Creates

| Table | Purpose |
|-------|---------|
| `ops.table_inventory` | Inventory groups (collections of tables) |
| `ops.backup_history` | Backup operation log |
| `ops.restore_history` | Restore operation log |
| `ops.run_status` | Job concurrency control |
| `ops.backup_partitions` | Partition-level backup details |

### Example

```bash
starrocks-br init --config config.yaml
```

### After Initialization

Populate the inventory with your backup groups:

```sql
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('my_group', 'production_db', 'users'),
  ('my_group', 'production_db', 'orders');
```

## backup full

Run a full backup of all tables in an inventory group.

### Syntax

```bash
starrocks-br backup full --config <config_file> --group <group_name> [--name <label>]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--config` | Yes | Path to configuration file |
| `--group` | Yes | Inventory group to backup |
| `--name` | No | Custom backup label. Supports `-v#r` placeholder for auto-versioning |

### Examples

**Basic full backup:**
```bash
starrocks-br backup full --config config.yaml --group production_tables
```

**With custom label:**
```bash
starrocks-br backup full --config config.yaml --group production_tables --name my_backup_v1
```

### Monitoring

```sql
-- Active backups
SHOW BACKUP;

-- Backup history
SELECT label, status, started_at, finished_at
FROM ops.backup_history
ORDER BY started_at DESC
LIMIT 10;
```

## backup incremental

Backup only partitions that changed since the last full backup.

### Syntax

```bash
starrocks-br backup incremental --config <config_file> --group <group_name> [--baseline-backup <label>] [--name <label>]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--config` | Yes | Path to configuration file |
| `--group` | Yes | Inventory group to backup |
| `--baseline-backup` | No | Specific full backup to use as baseline (default: most recent) |
| `--name` | No | Custom backup label. Supports `-v#r` placeholder for auto-versioning |

### Examples

**Basic incremental backup:**
```bash
starrocks-br backup incremental --config config.yaml --group production_tables
```

**With specific baseline:**
```bash
starrocks-br backup incremental \
  --config config.yaml \
  --group production_tables \
  --baseline-backup sales_db_20251118_full
```

**With custom label:**
```bash
starrocks-br backup incremental \
  --config config.yaml \
  --group production_tables \
  --name my_incremental_v1
```

### Requirements

- Must have at least one successful full backup for the group
- Works best with partitioned tables

## restore

Restore data from a backup with automatic backup chain resolution.

### Syntax

```bash
starrocks-br restore --config <config_file> --target-label <backup_label> [--group <group_name>] [--table <table_name>] [--rename-suffix <suffix>] [--yes]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--config` | Yes | Path to configuration file |
| `--target-label` | Yes | Backup label to restore from |
| `--group` | No | Restore only this inventory group |
| `--table` | No | Restore only this table (name only, no database prefix) |
| `--rename-suffix` | No | Suffix for temp tables (default: `_restored`) |
| `--yes` | No | Skip confirmation prompt |

**Note:** Cannot use both `--group` and `--table` together.

### Examples

**Full restore:**
```bash
starrocks-br restore --config config.yaml --target-label sales_db_20251118_full
```

**Group-based restore:**
```bash
starrocks-br restore \
  --config config.yaml \
  --target-label sales_db_20251118_full \
  --group critical_tables
```

**Single table restore:**
```bash
starrocks-br restore \
  --config config.yaml \
  --target-label sales_db_20251118_full \
  --table orders
```

**Skip confirmation:**
```bash
starrocks-br restore \
  --config config.yaml \
  --target-label sales_db_20251118_full \
  --yes
```

### Finding Available Backups

```sql
SELECT label, backup_type, finished_at
FROM ops.backup_history
WHERE status = 'SUCCESS'
ORDER BY finished_at DESC;
```

### Monitoring

```sql
-- Active restores
SHOW RESTORE;

-- Restore history
SELECT restore_label, target_backup, status, started_at
FROM ops.restore_history
ORDER BY started_at DESC
LIMIT 10;
```

## Next Steps

- [Scheduling and Monitoring](scheduling.md)
- [Core Concepts](core-concepts.md)
