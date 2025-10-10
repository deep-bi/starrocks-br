from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from .db import Database


POLL_INTERVAL_SECONDS = 0.1


@dataclass(frozen=True)
class BackupPlan:
    backup_type: str  # 'full' | 'incremental'
    tables: List[str]
    partitions_by_table: dict[str, List[str]]  # only for incremental; empty for full


def get_last_successful_backup(db: Database) -> Optional[str]:
    rows = db.query(
        "SELECT DATE_FORMAT(MAX(backup_timestamp), '%Y-%m-%d %H:%i:%s') FROM ops.backup_history WHERE status='FINISHED'"
    )
    if not rows:
        return None
    ts = rows[0][0]
    return ts if ts else None


def get_changed_partitions_since(db: Database, table: str, since_ts: str) -> List[str]:
    """Query information_schema.partitions to find partitions modified since given timestamp.
    
    Note: UPDATE_TIME may be stale for external catalogs (e.g., Hive) and is subject to 
    session time_zone. For real-time accuracy, refresh metadata or query 
    information_schema.partitions_meta (or the source metastore) instead.
    """
    rows = db.query(
        "SELECT PARTITION_NAME FROM information_schema.partitions "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND UPDATE_TIME > %s",
        (table, since_ts)
    )
    return [row[0] for row in rows if row[0] is not None]


def decide_backup_plan(db: Database, tables: List[str]) -> BackupPlan:
    last_ts = get_last_successful_backup(db)
    if not last_ts:
        return BackupPlan(backup_type="full", tables=tables, partitions_by_table={})

    partitions_by_table: dict[str, List[str]] = {}
    for t in tables:
        parts = get_changed_partitions_since(db, t, last_ts)
        partitions_by_table[t] = parts

    has_any_changes = any(partitions_by_table.get(t) for t in tables)
    if not has_any_changes:
        # Nothing changed; still perform a lightweight snapshot to record state (design choice)
        return BackupPlan(backup_type="incremental", tables=[], partitions_by_table={})

    return BackupPlan(backup_type="incremental", tables=tables, partitions_by_table=partitions_by_table)


def insert_running_history(db: Database, plan: BackupPlan, snapshot_label: str) -> None:
    """Insert backup record with partitions_json for incremental backups."""
    partitions_json = None
    if plan.backup_type == "incremental" and plan.partitions_by_table:
        partitions_data = {table: parts for table, parts in plan.partitions_by_table.items() if parts}
        if partitions_data:
            partitions_json = json.dumps(partitions_data)
    
    db.execute(
        "INSERT INTO ops.backup_history (backup_type, status, start_time, snapshot_label, database_name, partitions_json) "
        "VALUES (%s, 'RUNNING', NOW(), %s, %s, %s)",
        (plan.backup_type, snapshot_label, "ops", partitions_json),
    )


def issue_backup_commands(db: Database, plan: BackupPlan, repository: str = "repo") -> None:
    """Issue StarRocks BACKUP commands with proper syntax."""
    if plan.backup_type == "full":
        tables_list = ", ".join(plan.tables + ["ops.backup_history"])
        sql = f"BACKUP DATABASE ops ON ({tables_list}) TO {repository}"
        db.execute(sql)
    else:
        for table in plan.tables:
            partitions = plan.partitions_by_table.get(table) or []
            if partitions:
                parts = ", ".join(partitions)
                objects = f"{table} PARTITION ({parts}), ops.backup_history"
            else:
                objects = f"{table}, ops.backup_history"
            sql = f"BACKUP DATABASE ops ON ({objects}) TO {repository}"
            db.execute(sql)


def poll_backup_until_done(db: Database) -> Tuple[str, Optional[str]]:
    while True:
        rows = db.query("SHOW BACKUP")
        if not rows:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        status, ts = rows[0][0], rows[0][1]
        if status in ("FINISHED", "FAILED"):
            return status, ts
        time.sleep(POLL_INTERVAL_SECONDS)


def update_history_final(db: Database, status: str, backup_timestamp: Optional[str]) -> None:
    if status == "FINISHED":
        db.execute(
            "UPDATE ops.backup_history SET status='FINISHED', end_time=NOW(), backup_timestamp=%s WHERE status='RUNNING' ORDER BY id DESC LIMIT 1",
            (backup_timestamp,),
        )
    else:
        db.execute(
            "UPDATE ops.backup_history SET status='FAILED', end_time=NOW() WHERE status='RUNNING' ORDER BY id DESC LIMIT 1"
        )


def run_backup(db: Database, tables: List[str], repository: str = "repo") -> None:
    plan = decide_backup_plan(db, tables)
    snapshot_label = f"bbr_{int(time.time())}"
    insert_running_history(db, plan, snapshot_label)
    try:
        issue_backup_commands(db, plan, repository)
        status, ts = poll_backup_until_done(db)
        update_history_final(db, status, ts)
    except Exception:
        update_history_final(db, "FAILED", None)
        raise
