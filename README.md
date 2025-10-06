# Project: Python CLI for StarRocks Backups (`starrocks-bbr`) - MVP

## 1. Goal
Develop a minimal, functional MVP of a Python CLI to automate the core backup and restore workflow for a StarRocks shared-nothing cluster. The focus is on implementing the essential logic correctly.

## 2. Core Logic to Implement
- **Backup Strategy:** The tool must distinguish between a `full` backup (the baseline "Master Copy") and an `incremental` backup.
- **Incremental Definition:** An "incremental" backup must contain only the partitions that have changed since the **last successful backup of any type**.
- **State Management:** All state must be tracked in a metadata table within StarRocks (e.g., `ops.backup_history`). The tool itself must be stateless.

## 3. MVP Command Scope

Implement the basic structure for the following commands:

#### a. `starrocks-bbr init`
- **Function:** Creates the metadata table.
- **Flags:** `--config <path>` (Required).
- **Logic:** Connects to the database and runs a `CREATE TABLE IF NOT EXISTS ops.backup_history (...)` statement. The table must be able to store backup type, status, start/end times, snapshot label, and the official `backup_timestamp`.

#### b. `starrocks-bbr backup`
- **Function:** Runs the main backup workflow automatically.
- **Flags:** `--config <path>` (Required).
- **Logic:**
    1.  Reads the DB config.
    2.  Checks the metadata table for the last successful backup.
    3.  If none exists, triggers a **full** backup of all tables listed in the config.
    4.  If one exists, triggers an **incremental** backup by finding changed partitions since the last backup's timestamp.
    5.  For both cases, it must:
        - Insert a "RUNNING" record into the metadata table.
        - Execute the appropriate `BACKUP SNAPSHOT` command.
        - Poll `SHOW BACKUP` until the job is complete.
        - Update the metadata record with the final status ("FINISHED" or "FAILED") and the `backup_timestamp`.

#### c. `starrocks-bbr restore`
- **Function:** Performs a basic point-in-time recovery of a single table.
- **Flags:** `--config <path>`, `--table <name>`, `--timestamp "<YYYY-MM-DD HH:MM:SS>"` (All required).
- **Logic:**
    1.  Finds the correct restore chain (the last full backup + all subsequent incrementals before the target timestamp) from the metadata table.
    2.  Identifies the latest version of each partition needed for the restore.
    3.  Executes the `RESTORE` commands in the correct order (full first, then partitions), using the correct `backup_timestamp` for each.
    4.  **MVP constraint:** If the target table already exists, the command should fail with an error message.

#### d. `starrocks-bbr list`
- **Function:** Shows a simple history of backups.
- **Flags:** `--config <path>` (Required).
- **Logic:** Queries the `ops.backup_history` table and prints the results to the console.

## 4. MVP Technical Requirements
- **Language:** Python 3.9+
- **Libraries:** Use `mysql-connector-python`, `PyYAML`, and `click`.
- **Error Handling:** Implement basic error handling for critical failures (e.g., DB connection fails, SQL query fails).
- **Logging:** Use the `logging` module to print informative status messages for each step.