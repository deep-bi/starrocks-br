from __future__ import annotations

import pytest


def make_repo_cfg(**overrides):
    base = {
        "name": "br_repo",
        "type": "s3",
        "endpoint": "http://minio:9000",
        "region": "us-east-1",
        "bucket": "backups",
        "prefix": "/starrocks",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "force_https": False,
    }
    base.update(overrides)
    return base


def test_ensure_repository_creates_when_missing(mocker):
    db = mocker.Mock()
    db.query.return_value = []

    from starrocks_br.repository import ensure_repository

    cfg = make_repo_cfg()
    ensure_repository(db, cfg)

    assert db.execute.call_count == 1
    assert "CREATE REPOSITORY" in db.execute.call_args[0][0]
    assert db.query.call_count >= 1


def test_ensure_repository_noop_when_matching(mocker):
    db = mocker.Mock()
    db.query.return_value = [
        ("br_repo", "s3", "s3://backups/starrocks", "endpoint=http://minio:9000;region=us-east-1"),
    ]

    from starrocks_br.repository import ensure_repository

    cfg = make_repo_cfg()
    ensure_repository(db, cfg)

    assert db.execute.call_count == 0
    assert db.query.call_count >= 1


def test_ensure_repository_mismatch_raises(mocker):
    db = mocker.Mock()
    db.query.return_value = [
        ("br_repo", "s3", "s3://backups/starrocks", "endpoint=http://wrong:9000;region=us-east-1"),
    ]

    from starrocks_br.repository import ensure_repository

    cfg = make_repo_cfg(endpoint="http://minio:9000")
    with pytest.raises(RuntimeError):
        ensure_repository(db, cfg)


def test_ensure_repository_surfaces_create_error(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    db.execute.side_effect = RuntimeError("create failed: auth error")

    from starrocks_br.repository import ensure_repository

    cfg = make_repo_cfg()
    with pytest.raises(RuntimeError) as err:
        ensure_repository(db, cfg)

    assert "create failed" in str(err.value)


