#src\hypermill_nctools_inventory_exporter\nctool_plot.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

from hypermill_nctools_inventory_exporter.geometry_polyline import (
    PolylineFormat,
    guess_polyline_format,
    parse_polyline,
    read_geometry_polyline_blob,
)

# ---- 小さめユーティリティ ----

@dataclass
class ToolSimple:
    tool_id: int
    name: str
    dia: float
    length: float


def _safe_max_pos(*vals: float) -> float:
    cands = []
    for v in vals:
        if v is None:
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        if fv > 0:
            cands.append(fv)
    return max(cands) if cands else 0.0


def load_tool_simple(cur: sqlite3.Cursor, tool_id: int) -> ToolSimple | None:
    cur.execute(
        """
        SELECT id, name, total_length,
               dbl_param1, dbl_param2, dbl_param3, dbl_param4, dbl_param5, dbl_param6
        FROM Tools
        WHERE id = ?
        """,
        (tool_id,),
    )
    row = cur.fetchone()
    if not row:
        return None

    (_id, name, total_len, p1, p2, p3, p4, p5, p6) = row
    length = float(total_len or 0.0)

    # 直径は “それっぽい候補から最大の正値”
    dia = _safe_max_pos(p4 or 0.0, p1 or 0.0, p2 or 0.0)

    return ToolSimple(tool_id=int(_id), name=str(name or ""), dia=dia, length=length)


def tool_cylinder_profile(tool: ToolSimple, tip_z: float) -> list[tuple[float, float]]:
    """
    2D断面（片側）プロファイルを作る： (Z, R)
    tip_z で工具先端（Z+側端面）を合わせる。
    """
    r = max(tool.dia * 0.5, 0.0)
    L = max(tool.length, 0.0)

    z0 = tip_z - L
    z1 = tip_z

    return [(z0, 0.0), (z0, r), (z1, r), (z1, 0.0)]


def mirror_profile(profile_zr: list[tuple[float, float]]) -> tuple[list[float], list[float]]:
    # profile_zr: [(Z, R), ...]   R>=0
    zs = [p[0] for p in profile_zr]
    rs = [p[1] for p in profile_zr]
    # 右側 + 左側（反転）で閉じる
    z2 = zs + zs[::-1]
    r2 = rs + [-v for v in rs[::-1]]
    return z2, r2


def _extract_points_f64_be_xyz(recs, only_type: int | None, stop_at_zero: bool, max_points: int | None):
    pts: list[tuple[float, float, float]] = []
    for r in recs:
        if only_type is not None and getattr(r, "rec_type_u16", None) != only_type:
            continue
        f64_be = getattr(r, "f64_be", None)
        if not f64_be or len(f64_be) < 2:
            continue

        x = float(f64_be[0])
        y = float(f64_be[1])
        z = float(f64_be[2]) if len(f64_be) >= 3 else 0.0

        if stop_at_zero and x == 0.0 and y == 0.0 and z == 0.0:
            break

        pts.append((x, y, z))
        if max_points is not None and len(pts) >= max_points:
            break
    return pts


def _polyline_to_section_RZ(
    pts_xyz: list[tuple[float, float, float]],
    *,
    only_type: int | None,
    stop_at_zero: bool,
    max_points: int | None,
    swap_rz: bool,
    flip_r: bool,
    flip_z: bool,
) -> tuple[list[float], list[float]]:
    # 今回のデータは概ね z=0 で、(R=x, Z=y) と解釈
    zs: list[float] = []
    rs: list[float] = []
    for (x, y, _z) in pts_xyz:
        Z = float(y)
        R = float(x)

        if swap_rz:
            Z, R = R, Z
        if flip_r:
            R = -R
        if flip_z:
            Z = -Z

        zs.append(Z)
        rs.append(R)
    return zs, rs


def sanitize_filename(s: str) -> str:
    s = s.replace("\\", "__")
    return "".join("_" if c in r'<>:"/\\|?*' else c for c in s)


def export_nctool_pngs_for_folder_id(
    db_path: Path,
    folder_id: int,
    out_dir: Path,
    *,
    poly_header: int | None = 74,
    poly_record_len: int | None = 26,
    poly_rec_type: int | None = 76,
    tool_tip_mode: str = "zero",  # "zero" | "zmax" | "zmin" | "gage"
    annotate: bool = False,
) -> tuple[int, int]:
    """
    指定folder_id配下のNCToolsを列挙して、holder_geometry_idの断面をPNG保存する。
    戻り値: (n_ok, n_total)
    """
    import matplotlib.pyplot as plt

    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, tool_id, holder_geometry_id, gage_length, holder_reach, tool_length
        FROM NCTools
        WHERE folder_id = ?
          AND holder_geometry_id IS NOT NULL
        ORDER BY id
        """,
        (folder_id,),
    )
    rows = cur.fetchall()
    n_total = len(rows)
    n_ok = 0

    for (nctool_id, tool_id, geometry_id, gage_len, holder_reach, tool_len_nc) in rows:
        geometry_id = int(geometry_id)

        # ---- polyline 読み取り ----
        blob = read_geometry_polyline_blob(db_path, geometry_id)

        if poly_header is not None and poly_record_len is not None:
            fmt = PolylineFormat(int(poly_header), int(poly_record_len))
        else:
            fmt = guess_polyline_format(blob)

        if fmt is None:
            # このgeometryはスキップ
            continue

        try:
            _, recs = parse_polyline(blob, fmt)
        except Exception:
            continue

        pts_xyz = _extract_points_f64_be_xyz(recs, only_type=poly_rec_type, stop_at_zero=True, max_points=None)
        if not pts_xyz:
            continue

        zs, rs = _polyline_to_section_RZ(
            pts_xyz,
            only_type=poly_rec_type,
            stop_at_zero=True,
            max_points=None,
            swap_rz=False,
            flip_r=False,
            flip_z=False,
        )

        zmin, zmax = min(zs), max(zs)

        # ---- プロット（ホルダー：右側+左側のミラーで塗りつぶし）----
        fig, ax = plt.subplots()

        # 右側（元データ）
        ax.plot(rs, zs, marker="o")

        # 左側（ミラー）
        rs_m = [-r for r in rs]
        ax.plot(rs_m, zs)

        # ミラー+fill（簡易：R範囲の外形っぽく）
        ax.fill(rs + rs_m[::-1], zs + zs[::-1], alpha=0.20)

        # 中心線
        ax.axvline(0.0)

        ax.set_title(f"nctool_id={nctool_id}  geom={geometry_id}  (header={fmt.header_len}, record={fmt.record_len}, type={poly_rec_type})")
        ax.set_xlabel("R")
        ax.set_ylabel("Z")
        ax.grid(True)
        ax.axis("equal")

        if annotate:
            for i, (r, z) in enumerate(zip(rs, zs)):
                ax.text(r, z, str(i), fontsize=8)

        # ---- 工具オーバーレイ（簡易シリンダ）----
        tool = load_tool_simple(cur, int(tool_id))
        if tool and tool.dia > 0 and tool.length > 0:
            if tool_tip_mode == "zero":
                tip_z = 0.0
            elif tool_tip_mode == "zmax":
                tip_z = float(zmax)
            elif tool_tip_mode == "zmin":
                tip_z = float(zmin)
            else:  # "gage"
                tip_z = float(gage_len or 0.0)

            prof = tool_cylinder_profile(tool, tip_z=tip_z)
            z_poly, r_poly = mirror_profile(prof)
            ax.fill(r_poly, z_poly, alpha=0.30)

        # ---- ファイル名 ----
        d_txt = f"{tool.dia:g}" if tool else "0"
        l_txt = f"{tool.length:g}" if tool else "0"
        fname = f"nctool{nctool_id}_tool{tool_id}_D{d_txt}_L{l_txt}_geom{geometry_id}.png"
        fname = sanitize_filename(fname)
        save_path = out_dir / fname

        fig.savefig(save_path, dpi=160)
        plt.close(fig)
        n_ok += 1

    conn.close()
    return n_ok, n_total
