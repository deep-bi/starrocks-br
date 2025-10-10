from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def default_config(tables: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "host": "localhost",
        "port": 9030,
        "user": "root",
        "password": "secret",
        "database": "ops",
        "tables": tables or [],
        "repository": "test_repo",
    }


essential_keys = {"host", "port", "user", "password", "database", "tables", "repository"}


def write_cfg(tmp_path: Path, tables: Optional[List[str]] = None, overrides: Optional[Dict[str, Any]] = None, filename: str = "config.yaml") -> Path:
    cfg = default_config(tables)
    if overrides:
        cfg.update(overrides)
    # Safety: ensure required keys exist
    missing = essential_keys - set(cfg.keys())
    if missing:
        raise ValueError(f"Missing required config keys: {', '.join(sorted(missing))}")

    p = tmp_path / filename
    p.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return p
