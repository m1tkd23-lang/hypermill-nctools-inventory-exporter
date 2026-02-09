#src\hypermill_nctools_inventory_exporter\core.py
from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
        # hyperMILL系は bytes_le のことが多い（omtdx側と整合しやすい）
        return str(uuid.UUID(bytes_le=blob))
    except Exception:
        return None


def _fetch_folders(conn: sqlite3.Connection) -> tuple[dict[int, FolderRow], dict[int | None, list[int]]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT folder_id, parent_id, name, obj_guid, comment
        FROM Folders
        """
    )
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
    cur.execute(
        """
        SELECT folder_id
        FROM Folders
        WHERE name = ?
        """,
        (root_name,),
    )
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

    # NCTools 直下から出力（root自身を含めたいならここを walk(root_id, []) にする）
    for child_id in children.get(root_id, []):
        walk(child_id, [])

    return records


def export_nctools_to_excel(db_path: Path, output_path: Path) -> None:
    # 共有上や他プロセスが開いていても読めるよう read-only + uri を使う
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)

    try:
        nodes, children = _fetch_folders(conn)
        nctools_root_id = _find_root_folder_id(conn, "NCTools")
        records = _collect_subtree_paths(nodes, children, nctools_root_id)

    finally:
        conn.close()

    # Excel出力（pandas）
    import pandas as pd  # 遅延import（起動軽くする）
    df = pd.DataFrame(records).sort_values("path")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)



def get_nctools_folder_paths(db_path: Path) -> list[dict[str, Any]]:
    """
    NCTools 配下のフォルダツリーを path として返す。
    返り値: [{"folder_id": int, "path": str, "depth": int, "name": str, "obj_guid": str|None, "comment": str|None}, ...]
    """
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    try:
        nodes, children = _fetch_folders(conn)
        root_id = _find_root_folder_id(conn, "NCTools")
        return _collect_subtree_paths(nodes, children, root_id)
    finally:
        conn.close()


def export_nc_tool_list_for_folder_path(
    db_path: Path,
    nctools_folder_path: str,
    output_path: Path,
) -> None:
    """
    指定された ncTools フォルダパス（例: DD(...)\DD0003-00-00(SCM440)）に含まれる NCTools を抽出し Excel 出力する。
    """
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)

    try:
        # NCTools ルート
        cur = conn.cursor()
        cur.execute("SELECT folder_id FROM Folders WHERE name='NCTools'")
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Folders に 'NCTools' が見つかりません")
        root_id = int(row[0])

        # ルート直下から再帰でパス→folder_id解決
        cur.execute(
            """
            WITH RECURSIVE tree(folder_id, parent_id, name, path) AS (
              SELECT folder_id, parent_id, name, name as path
              FROM Folders
              WHERE parent_id = ?
              UNION ALL
              SELECT f.folder_id, f.parent_id, f.name, tree.path || '\\' || f.name
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
        folder_id = int(row[0])

        # NCTools 実体を抽出（JOINで読みやすく）
        cur.execute(
            """
            SELECT
              ?                                AS nctools_folder_path,
              nt.nc_number_val                 AS nc_number,
              nt.nc_name                       AS nc_name,
              nt.comment                       AS nc_comment,
              t.name                           AS tool_name,
              h.name                           AS holder_name,
              COALESCE(GROUP_CONCAT(e.name, ' / '), '') AS subholder_name,
              nt.gage_length                   AS gage_length,
              nt.clearance_length              AS holder_protrusion,
              nt.tool_length                   AS tool_length,
              nt.id                            AS nctool_id,
              nt.obj_guid                      AS nctool_obj_guid
            FROM NCTools nt
            LEFT JOIN Tools   t ON t.id  = nt.tool_id
            LEFT JOIN Holders h ON h.id  = nt.holder_id
            LEFT JOIN Components c ON c.nctool_id = nt.id
            LEFT JOIN Extensions  e ON e.extension_id = c.extension_id
            WHERE nt.folder_id = ?
            GROUP BY nt.id
            ORDER BY nt.nc_number_val
            """,
            (nctools_folder_path, folder_id),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    finally:
        conn.close()

    # ここからExcel整形（DB接続外）
    import pandas as pd

    df = pd.DataFrame(rows, columns=cols)

    # BLOB GUID → UUID文字列
    def _blob_to_uuid(v):
        if v is None:
            return ""
        try:
            return str(uuid.UUID(bytes_le=v))
        except Exception:
            return ""

    df["nctool_obj_guid"] = df["nctool_obj_guid"].map(_blob_to_uuid)

    # 列名を日本語寄せ
    df = df.rename(
        columns={
            "gage_length": "ゲージ長さ",
            "holder_protrusion": "ホルダーからの突き出し",
            "tool_length": "tool_length(サブホルダーからの突き出し)",
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
