#apps\gui.py
from __future__ import annotations

import os
import sys
from pathlib import Path

# src を import path に追加（main.py と同じ方針）
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from dotenv import load_dotenv
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from hypermill_nctools_inventory_exporter.core import (
    get_nctools_folder_paths,
    export_nc_tool_list_for_folder_path,
)


def main() -> int:
    load_dotenv()

    db_path_str = os.getenv("HYPERMILL_TOOLDB_PATH")
    if not db_path_str:
        messagebox.showerror("設定エラー", "HYPERMILL_TOOLDB_PATH が未設定です（.env を確認）")
        return 1
    db_path = Path(db_path_str)

    default_out = os.getenv("OUTPUT_XLSX_PATH", "./nctools_inventory.xlsx")
    default_out = Path(default_out).resolve().parent

    # パス一覧ロード
    try:
        folders = get_nctools_folder_paths(db_path)
    except Exception as e:
        messagebox.showerror("DB読込エラー", str(e))
        return 1

    # GUI
    root = tk.Tk()
    root.title("hypermill ncTools exporter")

    root.geometry("980x640")

    # フィルタ
    filter_var = tk.StringVar()

    # コンボ用の path 一覧（深い階層もそのまま出す）
    all_paths = [f["path"] for f in folders]

    frm_top = ttk.Frame(root, padding=10)
    frm_top.pack(fill="x")

    ttk.Label(frm_top, text="検索（部分一致）").pack(side="left")
    ent = ttk.Entry(frm_top, textvariable=filter_var, width=60)
    ent.pack(side="left", padx=8)

    frm_mid = ttk.Frame(root, padding=10)
    frm_mid.pack(fill="both", expand=True)

    ttk.Label(frm_mid, text="ncTools フォルダ（Path）").pack(anchor="w")

    listbox = tk.Listbox(frm_mid, height=22)
    listbox.pack(fill="both", expand=True)

    def refresh_list():
        listbox.delete(0, tk.END)
        key = filter_var.get().strip()
        items = all_paths if not key else [p for p in all_paths if key in p]
        for p in items:
            listbox.insert(tk.END, p)

    filter_var.trace_add("write", lambda *args: refresh_list())
    refresh_list()

    frm_bottom = ttk.Frame(root, padding=10)
    frm_bottom.pack(fill="x")

    ttk.Label(frm_bottom, text="出力先フォルダ").grid(row=0, column=0, sticky="w")
    out_dir_var = tk.StringVar(value=str(default_out))
    out_ent = ttk.Entry(frm_bottom, textvariable=out_dir_var, width=80)
    out_ent.grid(row=0, column=1, padx=8, sticky="we")

    frm_bottom.columnconfigure(1, weight=1)

    def choose_dir():
        d = filedialog.askdirectory(initialdir=str(default_out))
        if d:
            out_dir_var.set(d)

    ttk.Button(frm_bottom, text="参照", command=choose_dir).grid(row=0, column=2)

    def do_export():
        sel = listbox.curselection()
        if not sel:
            messagebox.showwarning("未選択", "ncTools フォルダを選択してください")
            return
        path = listbox.get(sel[0])

        # ファイル名に使えない文字を潰す（Windows想定）
        safe = path.replace("\\", "__")
        safe = "".join("_" if c in r'<>:"/\\|?*' else c for c in safe)
        out_path = Path(out_dir_var.get()) / f"nctools_list__{safe}.xlsx"

        try:
            export_nc_tool_list_for_folder_path(db_path, path, out_path)
        except Exception as e:
            messagebox.showerror("出力失敗", str(e))
            return

        messagebox.showinfo("完了", f"出力しました:\n{out_path}")

    ttk.Button(frm_bottom, text="選択したフォルダのNCツール一覧を出力", command=do_export).grid(
        row=1, column=0, columnspan=3, pady=(10, 0), sticky="we"
    )

    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
