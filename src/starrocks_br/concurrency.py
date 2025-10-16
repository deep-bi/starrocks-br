from typing import Literal


def reserve_job_slot(db, scope: str, label: str) -> None:
    """Reserve a job slot in ops.run_status to prevent overlapping jobs.

    We consider any row with state='ACTIVE' for the same scope as a conflict.
    """
    rows = db.query("SELECT scope, label, state FROM ops.run_status WHERE state = 'ACTIVE'")
    if any(r[0] == scope for r in rows):
        raise RuntimeError("active job conflict for scope; retry later")

    sql = (
        "INSERT INTO ops.run_status (scope, label, state, started_at) "
        "VALUES ('%s','%s','ACTIVE', NOW())" % (scope, label)
    )
    db.execute(sql)


def complete_job_slot(db, scope: str, label: str, final_state: Literal['FINISHED','FAILED','CANCELLED']) -> None:
    """Complete job slot and persist final state.

    Simple approach: update the same row by scope/label.
    """
    sql = (
        "UPDATE ops.run_status SET state='%s', finished_at=NOW() WHERE scope='%s' AND label='%s'"
        % (final_state, scope, label)
    )
    db.execute(sql)
