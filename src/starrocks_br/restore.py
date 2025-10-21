import time
from typing import Dict, List, Optional
from . import history, concurrency, logger

MAX_POLLS = 21600 # 6 hours

def build_partition_restore_command(
    database: str,
    table: str,
    partition: str,
    backup_label: str,
    repository: str,
) -> str:
    """Build RESTORE command for single partition recovery."""
    return f"""
    RESTORE SNAPSHOT {backup_label}
    FROM {repository}
    ON (TABLE {database}.{table} PARTITION ({partition}))"""


def build_table_restore_command(
    database: str,
    table: str,
    backup_label: str,
    repository: str,
) -> str:
    """Build RESTORE command for full table recovery."""
    return f"""
    RESTORE SNAPSHOT {backup_label}
    FROM {repository}
    ON (TABLE {database}.{table})"""


def build_database_restore_command(
    database: str,
    backup_label: str,
    repository: str,
) -> str:
    """Build RESTORE command for full database recovery."""
    return f"""
    RESTORE DATABASE {database}
    FROM {repository}
    SNAPSHOT {backup_label}"""


def poll_restore_status(db, label: str, max_polls: int = MAX_POLLS, poll_interval: float = 1.0) -> Dict[str, str]:
    """Poll restore status until completion or timeout.
    
    Returns dictionary with keys: state, label
    """
    query = f"SHOW RESTORE WHERE label = '{label}'"
    
    for _ in range(max_polls):
        try:
            rows = db.query(query)
            
            if not rows:
                return {"state": "UNKNOWN", "label": label}
            
            result = rows[0]
            
            if isinstance(result, dict):
                state = result.get("state", "UNKNOWN")
            else:
                state = result[1] if len(result) > 1 else "UNKNOWN"
            
            if state in ["FINISHED", "FAILED", "CANCELLED", "UNKNOWN"]:
                return {"state": state, "label": label}
            
            time.sleep(poll_interval)
            
        except Exception:
            return {"state": "ERROR", "label": label}
    
    return {"state": "TIMEOUT", "label": label}


def execute_restore(
    db,
    restore_command: str,
    backup_label: str,
    restore_type: str,
    repository: str,
    max_polls: int = MAX_POLLS,
    poll_interval: float = 1.0,
    scope: str = "restore",
) -> Dict:
    """Execute a complete restore workflow: submit command and monitor progress.
    
    Returns dictionary with keys: success, final_status, error_message
    """
    try:
        db.execute(restore_command.strip())
    except Exception as e:
        return {
            "success": False,
            "final_status": None,
            "error_message": f"Failed to submit restore command: {str(e)}"
        }
    
    label = backup_label
    
    try:
        final_status = poll_restore_status(db, label, max_polls, poll_interval)
        
        success = final_status["state"] == "FINISHED"
        
        try:
            history.log_restore(
                db,
                {
                    "job_id": label,
                    "backup_label": backup_label,
                    "restore_type": restore_type,
                    "status": final_status["state"],
                    "repository": repository,
                    "started_at": None,
                    "finished_at": None,
                    "error_message": None if success else final_status["state"],
                },
            )
        except Exception:
            pass
        
        try:
            concurrency.complete_job_slot(db, scope=scope, label=label, final_state=final_status["state"])
        except Exception:
            pass
        
        return {
            "success": success,
            "final_status": final_status,
            "error_message": None if success else f"Restore failed with state: {final_status['state']}"
        }
        
    except Exception as e:
        return {
            "success": False,
            "final_status": None,
            "error_message": str(e)
        }


def find_restore_pair(db, target_label: str) -> List[str]:
    """Find the correct sequence of backups needed for restore.
    
    Args:
        db: Database connection
        target_label: The backup label to restore to
        
    Returns:
        List of backup labels in restore order [base_full_backup, target_label]
        or [target_label] if target is a full backup
        
    Raises:
        ValueError: If target label not found or incremental has no preceding full backup
    """
    query = f"""
    SELECT label, backup_type, finished_at
    FROM ops.backup_history
    WHERE label = '{target_label}'
    AND status = 'FINISHED'
    """
    
    rows = db.query(query)
    if not rows:
        raise ValueError(f"Backup label '{target_label}' not found or not successful")
    
    target_info = {
        "label": rows[0][0],
        "backup_type": rows[0][1],
        "finished_at": rows[0][2]
    }
    
    if target_info["backup_type"] == "full":
        return [target_label]
    
    if target_info["backup_type"] == "incremental":
        database_name = target_label.split('_')[0]
        
        full_backup_query = f"""
        SELECT label, backup_type, finished_at
        FROM ops.backup_history
        WHERE backup_type = 'full'
        AND status = 'FINISHED'
        AND label LIKE '{database_name}_%'
        AND finished_at < '{target_info["finished_at"]}'
        ORDER BY finished_at DESC
        LIMIT 1
        """
        
        full_rows = db.query(full_backup_query)
        if not full_rows:
            raise ValueError(f"No successful full backup found before incremental '{target_label}'")
        
        base_full_backup = full_rows[0][0]
        return [base_full_backup, target_label]
    
    raise ValueError(f"Unknown backup type '{target_info['backup_type']}' for label '{target_label}'")


def get_tables_from_backup(db, label: str, group: Optional[str] = None) -> List[str]:
    """Get list of tables to restore from backup manifest.
    
    Args:
        db: Database connection
        label: Backup label
        group: Optional inventory group to filter tables
        
    Returns:
        List of table names to restore
    """
    query = f"""
    SELECT DISTINCT database_name, table_name
    FROM ops.backup_partitions
    WHERE label = '{label}'
    ORDER BY database_name, table_name
    """
    
    rows = db.query(query)
    if not rows:
        return []
    
    tables = [f"{row[0]}.{row[1]}" for row in rows]
    
    if group:
        group_query = f"""
        SELECT database_name, table_name
        FROM ops.table_inventory
        WHERE inventory_group = '{group}'
        """
        
        group_rows = db.query(group_query)
        if not group_rows:
            return []
        
        group_tables = {f"{row[0]}.{row[1]}" for row in group_rows}
        
        tables = [table for table in tables if table in group_tables]
    
    return tables


def execute_restore_flow(db, repo_name: str, restore_pair: List[str], tables_to_restore: List[str], rename_suffix: str = "_restored") -> Dict:
    """Execute the complete restore flow with safety measures.
    
    Args:
        db: Database connection
        repo_name: Repository name
        restore_pair: List of backup labels in restore order
        tables_to_restore: List of tables to restore (format: database.table)
        rename_suffix: Suffix for temporary tables
        
    Returns:
        Dictionary with success status and details
    """
    if not restore_pair:
        return {
            "success": False,
            "error_message": "No restore pair provided"
        }
    
    if not tables_to_restore:
        return {
            "success": False,
            "error_message": "No tables to restore"
        }
    
    logger.info("")
    logger.info("=== RESTORE PLAN ===")
    logger.info(f"Repository: {repo_name}")
    logger.info(f"Restore sequence: {' -> '.join(restore_pair)}")
    logger.info(f"Tables to restore: {', '.join(tables_to_restore)}")
    logger.info(f"Temporary table suffix: {rename_suffix}")
    logger.info("")
    logger.info("This will restore data to temporary tables and then perform atomic rename.")
    logger.warning("WARNING: This operation will replace existing tables!")
    
    confirmation = input("\nDo you want to proceed? [Y/n]: ").strip()
    if confirmation.lower() != 'y':
        return {
            "success": False,
            "error_message": "Restore operation cancelled by user"
        }
    
    try:
        base_label = restore_pair[0]
        logger.info("")
        logger.info(f"Step 1: Restoring base backup '{base_label}'...")
        
        base_restore_command = _build_restore_command_with_rename(
            base_label, repo_name, tables_to_restore, rename_suffix
        )
        
        base_result = execute_restore(
            db, base_restore_command, base_label, "full", repo_name, scope="restore"
        )
        
        if not base_result["success"]:
            return {
                "success": False,
                "error_message": f"Base restore failed: {base_result['error_message']}"
            }
        
        logger.success("Base restore completed successfully")
        
        if len(restore_pair) > 1:
            incremental_label = restore_pair[1]
            logger.info("")
            logger.info(f"Step 2: Applying incremental backup '{incremental_label}'...")
            
            incremental_restore_command = _build_restore_command_without_rename(
                incremental_label, repo_name, tables_to_restore
            )
            
            incremental_result = execute_restore(
                db, incremental_restore_command, incremental_label, "incremental", repo_name, scope="restore"
            )
            
            if not incremental_result["success"]:
                return {
                    "success": False,
                    "error_message": f"Incremental restore failed: {incremental_result['error_message']}"
                }
            
            logger.success("Incremental restore completed successfully")
        
        logger.info("")
        logger.info("Step 3: Performing atomic rename...")
        rename_result = _perform_atomic_rename(db, tables_to_restore, rename_suffix)
        
        if not rename_result["success"]:
            return {
                "success": False,
                "error_message": f"Atomic rename failed: {rename_result['error_message']}"
            }
        
        logger.success("Atomic rename completed successfully")
        
        return {
            "success": True,
            "message": f"Restore completed successfully. Restored {len(tables_to_restore)} tables."
        }
        
    except Exception as e:
        return {
            "success": False,
            "error_message": f"Restore flow failed: {str(e)}"
        }


def _build_restore_command_with_rename(backup_label: str, repo_name: str, tables: List[str], rename_suffix: str) -> str:
    """Build restore command with AS clause for temporary table names."""
    table_clauses = []
    for table in tables:
        database, table_name = table.split('.', 1)
        temp_table_name = f"{table_name}{rename_suffix}"
        table_clauses.append(f"TABLE {table_name} AS {temp_table_name}")
    
    on_clause = ",\n    ".join(table_clauses)
    
    return f"""RESTORE SNAPSHOT {backup_label}
    FROM {repo_name}
    ON ({on_clause})"""


def _build_restore_command_without_rename(backup_label: str, repo_name: str, tables: List[str]) -> str:
    """Build restore command without AS clause (for incremental restores to existing temp tables)."""
    table_clauses = []
    for table in tables:
        database, table_name = table.split('.', 1)
        table_clauses.append(f"TABLE {table_name}")
    
    on_clause = ",\n    ".join(table_clauses)
    
    return f"""RESTORE SNAPSHOT {backup_label}
    FROM {repo_name}
    ON ({on_clause})"""


def _perform_atomic_rename(db, tables: List[str], rename_suffix: str) -> Dict:
    """Perform atomic rename of temporary tables to make them live."""
    try:
        rename_statements = []
        for table in tables:
            database, table_name = table.split('.', 1)
            temp_table_name = f"{table_name}{rename_suffix}"
            
            rename_statements.append(f"RENAME TABLE {database}.{table_name} TO {database}.{table_name}_backup")
            rename_statements.append(f"RENAME TABLE {database}.{temp_table_name} TO {database}.{table_name}")
        
        for statement in rename_statements:
            db.execute(statement)
        
        return {"success": True}
        
    except Exception as e:
        return {
            "success": False,
            "error_message": f"Failed to perform atomic rename: {str(e)}"
        }
