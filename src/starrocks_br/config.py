from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    tables: List[str]


REQUIRED_KEYS = {"host", "port", "user", "password", "database", "tables"}


def load_config(config_path: str | Path) -> DatabaseConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    missing = REQUIRED_KEYS - set(raw.keys())
    if missing:
        missing_keys = ", ".join(sorted(missing))
        raise ValueError(f"Missing required config keys: {missing_keys}")

    tables = raw["tables"] or []
    if not isinstance(tables, list) or not all(isinstance(t, str) for t in tables):
        raise ValueError("'tables' must be a list of strings")

    return DatabaseConfig(
        host=str(raw["host"]),
        port=int(raw["port"]),
        user=str(raw["user"]),
        password=str(raw["password"]),
        database=str(raw["database"]),
        tables=tables,
    )
