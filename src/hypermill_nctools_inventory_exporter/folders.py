# src/hypermill_nctools_inventory_exporter/folders.py
from __future__ import annotations

import uuid
import sqlite3
from dataclasses import dataclass
from typing import Any
from .db import connect_readonly

@dataclass(frozen=True)
class FolderRow:
    folder_id: int
    parent_id: int | None
    name: str
    obj_guid: str | None
    comment: str | None

def _uuid_from_blob(blob: Any) -> str | None:
    if blob is None:
        return None
    try:
        return str(uuid.UUID(bytes_le=blob))
    except Exception:
        return None

def _fetch_folders(conn: sqlite3.Connection) -> tuple[dict[int, FolderRow], dict[int | None, list[int]]]:
    cur = conn.cursor()
    cur.execute("SELECT folder_id, parent_id, name, obj_guid, comment FROM Folders")
    nodes: dict[int, FolderRow] = {}
    children: dict[int | None, list[int]] = {}

    for folder_id, parent_id, name, obj_guid, comment in cur.fetchall():
        row = FolderRow(
            folder_id=int(folder_id),
            parent_id=int(parent_id) if parent_id is not None else None,
            name=str(name),
            obj_guid=_uuid_from_blob(obj_guid),
            comment=str(comment) if comment is not None else None,
        )
        nodes[row.folder_id] = row
        children.setdefault(row.parent_id, []).append(row.folder_id)

    return nodes, children

def _find_root_folder_id(conn: sqlite3.Connection, root_name: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT folder_id FROM Folders WHERE name = ?", (root_name,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Root folder not found: {root_name!r}")
    return int(row[0])

def _collect_subtree_paths(
    nodes: dict[int, FolderRow],
    children: dict[int | None, list[int]],
    root_id: int,
    sep: str = "\\",
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    def walk(node_id: int, stack: list[str]) -> None:
        node = nodes[node_id]
        stack2 = stack + [node.name]
        path = sep.join(stack2)
        records.append(
            {
                "path": path,
                "depth": len(stack2),
                "name": node.name,
                "obj_guid": node.obj_guid,
                "comment": node.comment,
            }
        )
        for child_id in children.get(node_id, []):
            walk(child_id, stack2)

    for child_id in children.get(root_id, []):
        walk(child_id, [])
    return records

def get_nctools_folder_paths(db_path) -> list[dict[str, Any]]:
    conn = connect_readonly(db_path)
    try:
        nodes, children = _fetch_folders(conn)
        root_id = _find_root_folder_id(conn, "NCTools")
        return _collect_subtree_paths(nodes, children, root_id)
    finally:
        conn.close()

def resolve_folder_id_by_nctools_path(conn: sqlite3.Connection, nctools_folder_path: str) -> int:
    """
    'DD(...)\DD0600...' のような NCTools直下パスを folder_id に解決する。
    """
    cur = conn.cursor()

    cur.execute("SELECT folder_id FROM Folders WHERE name='NCTools'")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Folders に 'NCTools' が見つかりません")
    root_id = int(row[0])

    cur.execute(
        r"""
        WITH RECURSIVE tree(folder_id, parent_id, name, path) AS (
          SELECT folder_id, parent_id, name, name as path
          FROM Folders
          WHERE parent_id = ?
          UNION ALL
          SELECT f.folder_id, f.parent_id, f.name, tree.path || '\' || f.name
          FROM Folders f
          JOIN tree ON f.parent_id = tree.folder_id
        )
        SELECT folder_id
        FROM tree
        WHERE path = ?
        """,
        (root_id, nctools_folder_path),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"指定パスが見つかりません: {nctools_folder_path}")
    return int(row[0])
