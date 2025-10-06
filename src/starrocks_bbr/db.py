from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, List, Tuple

import mysql.connector


@dataclass(frozen=True)
class Database:
    host: str
    port: int
    user: str
    password: str
    database: str

    @contextmanager
    def connect(self):
        conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        cursor = conn.cursor()
        try:
            yield conn
        finally:
            conn.commit()
            cursor.close()
            conn.close()

    def _cursor(self):
        conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        return conn, conn.cursor()

    def execute(self, sql: str, params: Tuple[Any, ...] | None = None) -> None:
        conn, cur = self._cursor()
        try:
            cur.execute(sql, params or ())
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def executemany(self, sql: str, seq_params: Iterable[Tuple[Any, ...]]) -> None:
        conn, cur = self._cursor()
        try:
            cur.executemany(sql, list(seq_params))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def query(self, sql: str, params: Tuple[Any, ...] | None = None) -> List[Tuple[Any, ...]]:
        conn, cur = self._cursor()
        try:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return rows
        finally:
            cur.close()
            conn.close()
