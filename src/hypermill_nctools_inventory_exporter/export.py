# src/hypermill_nctools_inventory_exporter/export.py
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from .db import connect_readonly
from .queries import (
    NCTOOLS_FOR_FOLDER_SQL_TEMPLATE,
    NCTOOLS_ALL_FAST_SQL_TEMPLATE,
)

import hypermill_nctools_inventory_exporter.export as ex
print("export.py loaded from:", ex.__file__)

RENAME_MAP = {
    "gage_length": "ゲージ長",
    "tool_length": "tool_length（刃物側突き出し）",
    "ext_reach_sum": "reach_sum（延長）",
    "overhang_est": "推定突き出し（tool_length+reach_sum）",
    "extensions": "extensions（pos:name(reach)）",
}

ProgressCb = Callable[[int, int, str], None]  # (done, total, message)


def _sanitize_sheet_name(name: str) -> str:
    """
    Excelシート名制限:
    - 最大31文字
    - 禁止文字: []:*?/\
    - 先頭/末尾の ' は避ける
    """
    bad = '[]:*?/\\'
    for ch in bad:
        name = name.replace(ch, "_")
    name = name.strip().strip("'")
    return name[:31] if name else "Sheet"


def _dedupe_sheet_name(name: str, used: set[str]) -> str:
    """
    同一シート名が出た場合に _01, _02 ... を付与して重複回避する。
    31文字制限を維持する。
    """
    base = name
    if base not in used:
        used.add(base)
        return base

    for i in range(1, 1000):
        suffix = f"_{i:02d}"
        trimmed = base[: (31 - len(suffix))]
        candidate = trimmed + suffix
        if candidate not in used:
            used.add(candidate)
            return candidate

    raise RuntimeError("シート名の重複解決に失敗しました（想定外の大量重複）")


def _detect_components_reach_col(conn) -> str:
    """
    Components テーブルの「延長長さ寄与」列名を推定して返す。
    DB差分に強くするための保険。
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(Components)")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = column name

    candidates = [
        "reach",
        #"reach_val",
        "reach_length",
        "reach_len",
        "reach_mm",
        "extension_reach",
        "extension_length",
        "len",
        "length",
    ]
    for c in candidates:
        if c in cols:
            return c

    raise RuntimeError(
        "Components テーブルに reach 相当の列が見つかりません。"
        f" columns={cols}"
    )


def _resolve_folder_id_by_nctools_path(conn, nctools_folder_path: str) -> int:
    r"""
    'DD(...)\DD0600...' のような NCTools直下パスを folder_id に解決する。
    """
    cur = conn.cursor()

    cur.execute("SELECT folder_id FROM Folders WHERE name='NCTools' LIMIT 1")
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


def export_nc_tool_list_for_folder_path(db_path: Path, nctools_folder_path: str, output_path: Path) -> None:
    """
    指定フォルダのNCツール一覧をXLSX出力。
    reach列名は自動判定してSQLテンプレに差し込む。
    """
    conn = connect_readonly(db_path)
    try:
        reach_col = _detect_components_reach_col(conn)

        folder_id = _resolve_folder_id_by_nctools_path(conn, nctools_folder_path)

        sql = NCTOOLS_FOR_FOLDER_SQL_TEMPLATE.format(reach_col=reach_col)
        if reach_col == "reach_val":
            raise RuntimeError("BUG: reach_col resolved to 'reach_val' (not a real column). Check export.py candidates and import cache.")


        cur = conn.cursor()
        cur.execute(sql, (nctools_folder_path, folder_id))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=cols).rename(columns=RENAME_MAP)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)


def export_all_nctools_to_excel_fast(
    db_path: Path,
    output_path: Path,
    progress: Optional[ProgressCb] = None,
) -> None:
    """
    全NCツールを1発SQLで取得して、1つのXLSX（1シート）に出す高速版。
    """
    if progress:
        progress(0, 2, "DBから全NCツールを取得中...")

    conn = connect_readonly(db_path)
    try:
        reach_col = _detect_components_reach_col(conn)

        sql = NCTOOLS_ALL_FAST_SQL_TEMPLATE.format(reach_col=reach_col)
        if reach_col == "reach_val":
            raise RuntimeError("BUG: reach_col resolved to 'reach_val' (not a real column). Check export.py candidates and import cache.")


        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=cols).rename(columns=RENAME_MAP)

    if progress:
        progress(1, 2, "Excelを書き込み中...")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)

    if progress:
        progress(2, 2, "完了")


def export_all_nctools_to_excel_by_sheet(
    db_path: Path,
    output_path: Path,
    progress: Optional[ProgressCb] = None,
) -> None:
    """
    全NCツールを1発SQLで取得し、nctools_folder_path ごとにシート分割して出力。
    シート名は31文字制限と重複を回避する。
    """
    if progress:
        progress(0, 2, "DBから全NCツールを取得中...")

    conn = connect_readonly(db_path)
    try:
        reach_col = _detect_components_reach_col(conn)

        sql = NCTOOLS_ALL_FAST_SQL_TEMPLATE.format(reach_col=reach_col)
        if reach_col == "reach_val":
            raise RuntimeError("BUG: reach_col resolved to 'reach_val' (not a real column). Check export.py candidates and import cache.")


        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        conn.close()

    df_all = pd.DataFrame(rows, columns=cols).rename(columns=RENAME_MAP)

    if progress:
        progress(1, 2, "Excel（シート分割）を書き込み中...")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    used: set[str] = set()
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        if df_all.empty:
            df_all.to_excel(writer, sheet_name="Empty", index=False)
        else:
            for folder_path, g in df_all.groupby("nctools_folder_path", sort=True):
                sheet = _dedupe_sheet_name(_sanitize_sheet_name(str(folder_path)), used)
                g.to_excel(writer, sheet_name=sheet, index=False)

    if progress:
        progress(2, 2, "完了")
