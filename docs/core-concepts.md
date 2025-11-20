# Core Concepts

This guide explains the key concepts you need to understand before using StarRocks Backup & Restore.

## Table of Contents

- [Inventory Groups](#inventory-groups)
- [The ops Database](#the-ops-database)
- [Backup Types](#backup-types)
- [Backup Repositories](#backup-repositories)
- [Backup Chains and Restore](#backup-chains-and-restore)

## Inventory Groups

An **inventory group** is a named collection of tables that you want to back up together. Think of it as labeling which tables belong to the same backup operation.

### Why Use Groups?

Instead of specifying tables manually every time you run a backup, you define groups once and reference them by name. This provides:

- **Consistency**: Same tables are always backed up together
- **Flexibility**: Different groups can have different backup strategies
- **Automation**: Easy to schedule different backup operations for different groups

### How You Might Group Tables

The grouping strategy depends on your needs. Here are some factors to consider:

**By Change Frequency:**
- Tables that change constantly vs. tables that rarely change
- Fast-moving transactional data vs. slow-moving reference data

**By Business Criticality:**
- Mission-critical tables vs. less critical ones
- Tables that need frequent backups vs. those that don't

**By Data Characteristics:**
- Large fact tables vs. small dimension tables
- Partitioned tables vs. non-partitioned tables

**By Database or Schema:**
- All tables from a specific database
- Related tables that form a logical unit

### Example Groupings

```sql
-- Example 1: Grouping by data type
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('fact_tables', 'sales_db', 'orders'),
  ('fact_tables', 'sales_db', 'transactions');

INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('dimension_tables', 'sales_db', 'customers'),
  ('dimension_tables', 'sales_db', 'products');

-- Example 2: Grouping by criticality
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('critical', 'app_db', 'users'),
  ('critical', 'app_db', 'payments');

INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('non_critical', 'app_db', 'logs'),
  ('non_critical', 'app_db', 'temp_data');

-- Example 3: Full database backup using wildcard
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES ('complete_backup', 'sales_db', '*');
```

You can name your groups anything that makes sense for your use case: `group_a`, `production_tables`, `reporting_data`, etc.

### How Groups Work

1. **Define groups** by inserting rows into `ops.table_inventory`
2. **Run backups** by specifying the group name:
   ```bash
   starrocks-br backup full --group fact_tables
   ```
3. **The tool** looks up all tables in that group and backs them up together

### Table Selection Patterns

**Explicit Table Names:**
```sql
VALUES ('my_group', 'mydb', 'users'),
       ('my_group', 'mydb', 'orders');
```
Use when you need precise control over which tables are included.

**Wildcards:**
```sql
VALUES ('my_group', 'mydb', '*');
```
Use `*` to include all tables in a database.

## The ops Database

The tool creates a dedicated `ops` database to store backup and restore metadata **separately from your business data**.

### Why a Separate Database?

- **Isolation**: Keeps tool metadata separate from your application data
- **Cross-database tracking**: Can track backups across multiple databases from one location
- **Clean separation**: Your data schemas stay clean, tool internals are hidden

### Tables in ops Database

| Table | Purpose |
|-------|---------|
| `table_inventory` | Defines inventory groups (which tables to backup together) |
| `backup_history` | Records all backup operations, status, and errors |
| `restore_history` | Records all restore operations |
| `run_status` | Prevents concurrent backup/restore operations |
| `backup_partitions` | Stores partition-level details for each backup (enables intelligent restore) |

### Data Flow

```
Your Database (sales_db, etc.)
         ↓
    backup operations
         ↓
ops Database (tracks everything)
         ↓
Repository (S3/HDFS - actual backup data)
```

## Backup Types

### Full Backup

A **full backup** copies all data from the selected tables at a point in time.

**Characteristics:**
- Creates a complete snapshot of the data
- Can be restored independently (no dependencies)
- Serves as a baseline for incremental backups

**Command:**
```bash
starrocks-br backup full --config config.yaml --group my_group
```

### Incremental Backup

An **incremental backup** copies only the partitions that have changed since the last full backup.

**How it works:**
1. Tool finds the most recent successful full backup for the group (the "baseline")
2. Compares current partition metadata with the baseline
3. Backs up only new or modified partitions

**Characteristics:**
- Faster than full backups (only changed data)
- Requires less storage space
- Depends on a full backup as baseline

**Command:**
```bash
starrocks-br backup incremental --config config.yaml --group my_group
```

**Important:** You must have a full backup before running an incremental backup. The tool uses the full backup as the comparison baseline.

### Choosing Between Full and Incremental

Consider these factors:

**Full Backups:**
- Provide complete snapshots
- Simpler to restore (single backup)
- Better for smaller datasets or infrequent backups

**Incremental Backups:**
- Reduce backup time and storage
- Good for large datasets with predictable changes
- Require managing backup chains (full + incrementals)

**Common Pattern:**
Many users combine both strategies - periodic full backups with incremental backups in between. For example:
- Run a full backup at regular intervals
- Run incremental backups between full backups

The specific schedule (daily, weekly, monthly) depends on your data volume, change rate, and recovery requirements.

## Backup Repositories

A **repository** in StarRocks is a storage location where backup data is physically stored (S3, HDFS, Azure Blob, etc.).

### Creating a Repository

You must create a repository in StarRocks **before** using this tool:

```sql
CREATE REPOSITORY `s3_backup_repo`
WITH S3
ON LOCATION "s3://your-backup-bucket/backups/"
PROPERTIES (
    "aws.s3.access_key" = "your-access-key",
    "aws.s3.secret_key" = "your-secret-key",
    "aws.s3.endpoint" = "https://s3.amazonaws.com"
);
```

See the [StarRocks documentation](https://docs.starrocks.io/docs/administration/management/Backup_and_restore/) for details on creating repositories with different storage backends.

### Using the Repository

Reference the repository name in your `config.yaml`:

```yaml
repository: "s3_backup_repo"
```

The tool uses this repository for all backup and restore operations.

### Verifying Repository

Check that your repository exists:

```sql
SHOW REPOSITORIES;
```

## Backup Chains and Restore

### What is a Backup Chain?

A **backup chain** is the sequence of backups needed to restore data to a specific point in time.

**Example:**
```
Full Backup (baseline)
    ↓
Incremental Backup A → depends on the full backup
    ↓
Incremental Backup B → depends on the full backup
```

To restore data from Incremental Backup B, you need **both**:
1. The full backup (baseline)
2. Incremental Backup B

### Intelligent Restore

The tool automatically resolves backup chains for you:

```bash
# You specify a target backup (could be full or incremental)
starrocks-br restore --config config.yaml --target-label my_backup_label

# The tool automatically:
# - Detects if it's a full or incremental backup
# - If incremental, finds the required full backup
# - Restores in the correct order
```

### Restore Modes

**1. Full Restore (Disaster Recovery)**
```bash
# Restore all tables from a backup
starrocks-br restore --config config.yaml --target-label my_backup_label
```

**2. Group-Based Restore**
```bash
# Restore only tables in a specific inventory group
starrocks-br restore --config config.yaml --target-label my_backup_label --group fact_tables
```

**3. Single Table Restore**
```bash
# Restore just one table
starrocks-br restore --config config.yaml --target-label my_backup_label --table orders
```

Note: Provide only the table name (e.g., `orders`), not `database.table`. The database is taken from your config file.

### Safe Restore with Temporary Tables

The restore process uses temporary tables to prevent data loss:

1. **Restore to temp tables** (e.g., `orders_restored`)
2. **Verify** the restore succeeded
3. **Atomic rename** to swap temp tables with live tables

This ensures you can verify restored data before it replaces production data.

**Customizing the suffix:**
```bash
starrocks-br restore \
  --config config.yaml \
  --target-label my_backup \
  --rename-suffix _verified  # Creates orders_verified instead of orders_restored
```

## Next Steps

Now that you understand the core concepts:

- **New users**: Continue to [Getting Started](getting-started.md) for a step-by-step tutorial
- **Ready to configure**: See [Configuration Reference](configuration.md)
- **Need command details**: Check [Command Reference](commands.md)
- **Setting up automation**: Read [Scheduling and Monitoring](scheduling.md)
