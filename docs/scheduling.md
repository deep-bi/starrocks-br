# Scheduling and Monitoring

Guide for automating backups and monitoring their status.

## Scheduling Backups

### Using Cron (Linux)

**Example: Full backup on Sundays at 1 AM**

```bash
# Edit crontab
crontab -e

# Add this line
0 1 * * 0 cd /path/to/project && source .venv/bin/activate && starrocks-br backup full --config config.yaml --group my_group
```

**Example: Incremental backup Monday-Saturday at 1 AM**

```bash
0 1 * * 1-6 cd /path/to/project && source .venv/bin/activate && starrocks-br backup incremental --config config.yaml --group my_group
```

**Important:** Remember to:
- Set `STARROCKS_PASSWORD` environment variable in your cron script
- Activate the virtual environment before running the command
- Use absolute paths

**Complete cron script example:**

```bash
#!/bin/bash
export STARROCKS_PASSWORD="your_password"
cd /path/to/starrocks-br
source .venv/bin/activate
starrocks-br backup full --config config.yaml --group production_tables
```

Then in crontab:
```
0 1 * * 0 /path/to/backup-script.sh
```

### Using Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: starrocks-backup
spec:
  schedule: "0 1 * * 0"  # Sunday at 1 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: your-starrocks-br-image
            command:
            - starrocks-br
            - backup
            - full
            - --config
            - /config/config.yaml
            - --group
            - production_tables
            env:
            - name: STARROCKS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: starrocks-credentials
                  key: password
            volumeMounts:
            - name: config
              mountPath: /config
          restartPolicy: OnFailure
          volumes:
          - name: config
            configMap:
              name: starrocks-br-config
```

## Monitoring Backups

### Check Recent Backup Status

```sql
SELECT
  label,
  backup_type,
  status,
  started_at,
  finished_at,
  error_message
FROM ops.backup_history
ORDER BY started_at DESC
LIMIT 10;
```

### Find Failed Backups

```sql
SELECT
  label,
  backup_type,
  status,
  error_message,
  started_at
FROM ops.backup_history
WHERE status = 'FAILED'
ORDER BY started_at DESC;
```

### Check Active Jobs

```sql
-- Check for active backups
SHOW BACKUP;

-- Check run status locks
SELECT scope, label, state, started_at
FROM ops.run_status
WHERE state = 'ACTIVE';
```

### Monitor Backup Success Rate

```sql
SELECT
  backup_type,
  COUNT(*) as total,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
FROM ops.backup_history
WHERE started_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY backup_type;
```

## Monitoring Restores

### Check Recent Restore Operations

```sql
SELECT
  restore_label,
  target_backup,
  status,
  started_at,
  finished_at,
  error_message
FROM ops.restore_history
ORDER BY started_at DESC
LIMIT 10;
```

### Find Failed Restores

```sql
SELECT
  restore_label,
  target_backup,
  error_message,
  started_at
FROM ops.restore_history
WHERE status = 'FAILED'
ORDER BY started_at DESC;
```

## Alerting

Set up alerts based on backup status. Here are example queries for your monitoring system:

**Alert on failed backups:**
```sql
SELECT COUNT(*) as failed_backups
FROM ops.backup_history
WHERE status = 'FAILED'
  AND started_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR);
```

**Alert on missing backups:**
```sql
SELECT
  CASE
    WHEN MAX(started_at) < DATE_SUB(NOW(), INTERVAL 24 HOUR)
    THEN 1
    ELSE 0
  END as backup_missing
FROM ops.backup_history;
```

## Retention Management

The tool logs all operations but doesn't automatically clean up old records. Consider periodic cleanup:

```sql
-- Delete backup history older than 90 days
DELETE FROM ops.backup_history
WHERE started_at < DATE_SUB(NOW(), INTERVAL 90 DAY);

-- Delete restore history older than 90 days
DELETE FROM ops.restore_history
WHERE started_at < DATE_SUB(NOW(), INTERVAL 90 DAY);
```

**Note:** This only cleans metadata in the `ops` database. Manage actual backup data in your repository (S3/HDFS) using your storage provider's lifecycle policies.

## Next Steps

- [Command Reference](commands.md)
- [Core Concepts](core-concepts.md)
