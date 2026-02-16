# src/hypermill_nctools_inventory_exporter/db.py
from __future__ import annotations

import sqlite3
from pathlib import Path

def connect_readonly(db_path: Path) -> sqlite3.Connection:
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(db_uri, uri=True)

def fetch_one_int(conn: sqlite3.Connection, sql: str, params: tuple) -> int | None:
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0]) if row else None
