# apps/gui.py
from __future__ import annotations

import json
import os
import sys
import threading
import queue
from pathlib import Path

# src を import path に追加
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dotenv import load_dotenv
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from hypermill_nctools_inventory_exporter.core import (
    get_nctools_folder_paths,
    export_nc_tool_list_for_folder_path,
    export_all_nctools_to_excel_fast,
    export_all_nctools_to_excel_by_sheet,
)

# ------------------------------------------------------------
# Config (last used DB path)
# ------------------------------------------------------------

APP_NAME = "hypermill-nctools-inventory-exporter"


def _get_config_path() -> Path:
    """
    apps/gui.py から見てプロジェクトルートに config.json を置く。
    例: <repo>/config.json
    """
    root = Path(__file__).resolve().parents[1]
    return root / "config.json"


def _load_config() -> dict:
    cfg_path = _get_config_path()
    if not cfg_path.exists():
        return {}
    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        # 壊れていても起動不能にしない
        return {}


def _save_config(cfg: dict) -> None:
    cfg_path = _get_config_path()
    try:
        with cfg_path.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        # 保存失敗は致命ではないので黙殺（必要ならGUIで警告にしてもOK）
        pass


def sanitize_filename(name: str) -> str:
    invalid = '<>:"/\\|?*'
    for ch in invalid:
        name = name.replace(ch, "_")
    name = name.rstrip(". ").strip()
    return name or "output"


def main() -> int:
    load_dotenv()

    # .env 初期値
    env_db = os.getenv("HYPERMILL_TOOLDB_PATH", "").strip()
    env_out = os.getenv("OUTPUT_XLSX_PATH", "./nctools_inventory.xlsx")

    default_out_dir = Path(env_out).resolve().parent

    # config.json（最後に使ったDB）
    cfg = _load_config()
    last_db = str(cfg.get("last_db_path", "")).strip()

    # 起動時DBの初期値優先順位:
    # 1) config.json（最後に使ったDB）
    # 2) .env の HYPERMILL_TOOLDB_PATH
    # 3) 空
    initial_db = last_db or env_db

    # ------------------------------------------------------------
    # GUI
    # ------------------------------------------------------------
    root = tk.Tk()
    root.title("hypermill ncTools exporter (XLSX)")
    root.geometry("980x760")

    # --- 上：検索 ---
    frm_top = ttk.Frame(root, padding=10)
    frm_top.pack(fill="x")

    ttk.Label(frm_top, text="検索（部分一致）").pack(side="left")
    filter_var = tk.StringVar()
    ttk.Entry(frm_top, textvariable=filter_var, width=60).pack(side="left", padx=8)

    # --- 中：リスト ---
    frm_mid = ttk.Frame(root, padding=10)
    frm_mid.pack(fill="both", expand=True)

    ttk.Label(frm_mid, text="ncTools フォルダ（Path）").pack(anchor="w")

    listbox = tk.Listbox(frm_mid, height=22)
    listbox.pack(fill="both", expand=True)

    # --- 下：出力先 / DB指定 / 実行ボタン ---
    frm_bottom = ttk.Frame(root, padding=10)
    frm_bottom.pack(fill="x")

    # 出力先フォルダ
    ttk.Label(frm_bottom, text="出力先フォルダ").grid(row=0, column=0, sticky="w")
    out_dir_var = tk.StringVar(value=str(default_out_dir))
    ttk.Entry(frm_bottom, textvariable=out_dir_var, width=80).grid(row=0, column=1, padx=8, sticky="we")
    frm_bottom.columnconfigure(1, weight=1)

    def choose_out_dir() -> None:
        d = filedialog.askdirectory(initialdir=str(default_out_dir))
        if d:
            out_dir_var.set(d)

    ttk.Button(frm_bottom, text="参照", command=choose_out_dir).grid(row=0, column=2)

    # DBファイル
    ttk.Label(frm_bottom, text="ToolDB（.db）").grid(row=1, column=0, sticky="w")
    db_path_var = tk.StringVar(value=initial_db)
    ttk.Entry(frm_bottom, textvariable=db_path_var, width=80).grid(row=1, column=1, padx=8, sticky="we")

    def choose_db() -> None:
        init_dir = ""
        try:
            p = Path(db_path_var.get())
            init_dir = str(p.parent) if p.exists() else ""
        except Exception:
            init_dir = ""
        file = filedialog.askopenfilename(
            title="ToolDB（SQLite .db）を選択",
            initialdir=init_dir or None,
            filetypes=[("SQLite DB", "*.db"), ("All files", "*.*")],
        )
        if file:
            db_path_var.set(file)

    ttk.Button(frm_bottom, text="参照", command=choose_db).grid(row=1, column=2)

    # --- 進捗UI ---
    status_var = tk.StringVar(value="待機中")
    prog = ttk.Progressbar(frm_bottom, orient="horizontal", mode="determinate")
    lbl = ttk.Label(frm_bottom, textvariable=status_var)

    prog.grid(row=3, column=0, columnspan=3, sticky="we", pady=(12, 0))
    lbl.grid(row=4, column=0, columnspan=3, sticky="w", pady=(6, 0))

    # Queue for worker -> UI
    q: queue.Queue[tuple[str, int, int, str]] = queue.Queue()
    busy = {"flag": False}

    def set_busy(on: bool) -> None:
        busy["flag"] = on

    def pump_queue() -> None:
        try:
            while True:
                kind, done, total, msg = q.get_nowait()
                if kind == "progress":
                    status_var.set(msg)
                    prog["maximum"] = max(1, total)
                    prog["value"] = done
                elif kind == "done":
                    set_busy(False)
                    status_var.set("完了")
                    messagebox.showinfo("完了", msg)
                elif kind == "error":
                    set_busy(False)
                    status_var.set("エラー")
                    messagebox.showerror("エラー", msg)
        except queue.Empty:
            pass
        root.after(100, pump_queue)

    root.after(100, pump_queue)

    def _validate_db_path(p: Path) -> bool:
        if not p:
            messagebox.showerror("DBエラー", "ToolDB（.db）のパスが空です。")
            return False
        if not p.exists():
            messagebox.showerror("DBエラー", f"ToolDB が見つかりません:\n{p}")
            return False
        if p.is_dir():
            messagebox.showerror("DBエラー", f"ToolDB の指定がフォルダになっています:\n{p}")
            return False
        # 拡張子チェックは任意（.dbじゃない場合もあるので厳密にはしない）
        return True

    def _get_db_path_or_show_error() -> Path | None:
        p = Path(db_path_var.get()).expanduser()
        try:
            p = p.resolve()
        except Exception:
            # resolveできないケースでもexistsは見れるのでそのまま
            pass

        if not _validate_db_path(p):
            return None

        # 最後に使ったDBを保存
        cfg = _load_config()
        cfg["last_db_path"] = str(p)
        _save_config(cfg)
        return p

    # 起動時：DBが有効ならフォルダ一覧をロード
    folders: list[dict] = []
    all_paths: list[str] = []

    def _reload_folder_list(db_path: Path) -> None:
        nonlocal folders, all_paths
        folders = get_nctools_folder_paths(db_path)
        all_paths = [f["path"] for f in folders]
        refresh_list()

    def refresh_list() -> None:
        listbox.delete(0, tk.END)
        key = filter_var.get().strip()
        items = all_paths if not key else [p for p in all_paths if key in p]
        for p in items:
            listbox.insert(tk.END, p)

    filter_var.trace_add("write", lambda *args: refresh_list())

    # 起動時の自動復元
    try:
        p0 = Path(initial_db).expanduser() if initial_db else None
        if p0 and p0.exists() and p0.is_file():
            _reload_folder_list(p0)
        else:
            # DBが未設定/無効でもGUIは起動させる
            status_var.set("ToolDB を指定してください（参照ボタン）")
    except Exception as e:
        status_var.set("DB読込エラー（ToolDBを選び直してください）")
        messagebox.showerror("DB読込エラー", str(e))

    # DB切り替え→フォルダ再読込ボタン
    def do_reload_db() -> None:
        db_path = _get_db_path_or_show_error()
        if db_path is None:
            return
        try:
            _reload_folder_list(db_path)
            status_var.set("フォルダ一覧を更新しました")
        except Exception as e:
            messagebox.showerror("DB読込エラー", str(e))

    ttk.Button(frm_bottom, text="DB再読込（フォルダ一覧更新）", command=do_reload_db).grid(
        row=2, column=0, columnspan=3, pady=(8, 0), sticky="we"
    )

    def run_in_thread(worker_fn):
        if busy["flag"]:
            messagebox.showwarning("実行中", "処理が実行中です。完了後に再実行してください。")
            return
        set_busy(True)
        prog["value"] = 0
        status_var.set("開始...")

        t = threading.Thread(target=worker_fn, daemon=True)
        t.start()

    # --- 選択フォルダ出力 ---
    def do_export_selected() -> None:
        db_path = _get_db_path_or_show_error()
        if db_path is None:
            return

        sel = listbox.curselection()
        if not sel:
            messagebox.showwarning("未選択", "ncTools フォルダを選択してください")
            return
        nctools_folder_path = listbox.get(sel[0])

        base_out_dir = Path(out_dir_var.get()).expanduser().resolve()
        safe = sanitize_filename(nctools_folder_path)
        out_folder = base_out_dir / safe
        out_folder.mkdir(parents=True, exist_ok=True)
        out_xlsx = out_folder / f"nctools_list__{safe}.xlsx"

        def worker():
            try:
                q.put(("progress", 0, 2, "選択フォルダを出力中..."))
                export_nc_tool_list_for_folder_path(db_path, nctools_folder_path, out_xlsx)
                q.put(("progress", 2, 2, "完了"))
                q.put(("done", 2, 2, f"出力しました:\n{out_xlsx}"))
            except Exception as e:
                q.put(("error", 0, 1, str(e)))

        run_in_thread(worker)

    # --- 全件高速（1ファイル） ---
    def do_export_all_fast() -> None:
        db_path = _get_db_path_or_show_error()
        if db_path is None:
            return

        base_out_dir = Path(out_dir_var.get()).expanduser().resolve()
        out_xlsx = base_out_dir / "all_nctools_inventory_fast.xlsx"

        def worker():
            try:
                def progress(done: int, total: int, msg: str) -> None:
                    q.put(("progress", done, total, msg))

                export_all_nctools_to_excel_fast(db_path, out_xlsx, progress=progress)
                q.put(("done", 1, 1, f"出力しました:\n{out_xlsx}"))
            except Exception as e:
                q.put(("error", 0, 1, str(e)))

        run_in_thread(worker)

    # --- 全件（フォルダ別シート） ---
    def do_export_all_by_sheet() -> None:
        db_path = _get_db_path_or_show_error()
        if db_path is None:
            return

        base_out_dir = Path(out_dir_var.get()).expanduser().resolve()
        out_xlsx = base_out_dir / "all_nctools_inventory_by_sheet.xlsx"

        def worker():
            try:
                def progress(done: int, total: int, msg: str) -> None:
                    q.put(("progress", done, total, msg))

                export_all_nctools_to_excel_by_sheet(db_path, out_xlsx, progress=progress)
                q.put(("done", 1, 1, f"出力しました:\n{out_xlsx}"))
            except Exception as e:
                q.put(("error", 0, 1, str(e)))

        run_in_thread(worker)

    ttk.Button(frm_bottom, text="選択フォルダのNCツール一覧を出力（XLSX）", command=do_export_selected).grid(
        row=5, column=0, columnspan=3, pady=(10, 0), sticky="we"
    )
    ttk.Button(frm_bottom, text="全NCツールを一括出力（高速・1ファイル）", command=do_export_all_fast).grid(
        row=6, column=0, columnspan=3, pady=(10, 0), sticky="we"
    )
    ttk.Button(frm_bottom, text="全NCツールを一括出力（フォルダ別シート）", command=do_export_all_by_sheet).grid(
        row=7, column=0, columnspan=3, pady=(6, 0), sticky="we"
    )

    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
