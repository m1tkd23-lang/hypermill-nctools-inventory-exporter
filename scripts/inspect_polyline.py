# scripts/inspect_polyline.py
from __future__ import annotations

import argparse
from pathlib import Path
from dataclasses import dataclass

# scripts/ 直下から実行されても src/ がimportできるように
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from hypermill_nctools_inventory_exporter.geometry_polyline import (
    PolylineFormat,
    guess_polyline_format,
    hexdump,
    iter_geometry_ids_with_polyline,
    parse_polyline,
    read_geometry_polyline_blob,
    summarize_record_types,
)

import sqlite3


def cmd_summary(db_path: Path, sample_n: int, header: int | None, record_len: int | None) -> int:
    gids = iter_geometry_ids_with_polyline(db_path, limit=max(sample_n, 1))
    if not gids:
        print("polyline を持つ Geometries が見つかりません。")
        return 1

    fmt_fixed = None
    if header is not None and record_len is not None:
        fmt_fixed = PolylineFormat(header, record_len)

    global_counts: dict[int, int] = {}
    fmt_used_counts: dict[tuple[int, int], int] = {}

    for gid in gids[:sample_n]:
        blob = read_geometry_polyline_blob(db_path, gid)

        fmt = fmt_fixed or guess_polyline_format(blob)
        if fmt is None:
            print(f"[skip] geometry_id={gid}: format not guessed")
            continue

        fmt_used_counts[(fmt.header_len, fmt.record_len)] = fmt_used_counts.get((fmt.header_len, fmt.record_len), 0) + 1

        try:
            _, recs = parse_polyline(blob, fmt)
        except Exception as e:
            print(f"[skip] geometry_id={gid}: parse failed: {e}")
            continue

        counts = summarize_record_types(recs)
        for k, v in counts.items():
            global_counts[k] = global_counts.get(k, 0) + v

    print("=== Format usage (header_len, record_len) ===")
    for (h, r), c in sorted(fmt_used_counts.items(), key=lambda kv: -kv[1]):
        print(f"  ({h}, {r}) : {c}")

    print("\n=== Record type global counts (u16) ===")
    for t, c in sorted(global_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {t:>6} : {c}")

    return 0


def _fmt_preview(seq, n: int, fmt: str = "{:.6g}") -> str:
    if not seq:
        return ""
    out = []
    for x in seq[:n]:
        try:
            out.append(fmt.format(x))
        except Exception:
            out.append(str(x))
    return ", ".join(out)


def cmd_dump(db_path: Path, geometry_id: int, header: int | None, record_len: int | None, max_dump_recs: int) -> int:
    blob = read_geometry_polyline_blob(db_path, geometry_id)
    print(f"geometry_id={geometry_id}  polyline_bytes={len(blob)}")

    print("\n--- hexdump(head) ---")
    print(hexdump(blob, max_bytes=512))

    if header is not None and record_len is not None:
        fmt = PolylineFormat(header, record_len)
    else:
        fmt = guess_polyline_format(blob)

    if fmt is None:
        print("\n[ERROR] format not guessed. try --header and --record-len")
        return 1

    print(f"\n--- guessed format ---\nheader_len={fmt.header_len}, record_len={fmt.record_len}")

    try:
        hdr, recs = parse_polyline(blob, fmt)
    except Exception as e:
        print(f"\n[ERROR] parse failed: {e}")
        return 1

    print(f"\nheader_bytes={len(hdr)} records={len(recs)}")

    print("\n--- record type summary ---")
    counts = summarize_record_types(recs)
    for t, c in counts.items():
        print(f"  {t:>6} : {c}")

    print("\n--- first records ---")
    for r in recs[:max_dump_recs]:
        phex = r.payload[:24].hex()

        f64le = _fmt_preview(r.f64_le, 3, "{:.6g}")
        f64be = _fmt_preview(r.f64_be, 3, "{:.6g}")
        f32le = _fmt_preview(r.f32_le, 6, "{:.6g}")
        i32le = _fmt_preview(r.i32_le, 6, "{}")

        print(
            f"[{r.index:>4}] "
            f"off=0x{r.offset:08x} "
            f"type={r.rec_type_u16:>6} "
            f"payload_len={len(r.payload):>2} "
            f"payload_head={phex} "
            f"f64_le=({f64le}) "
            f"f64_be=({f64be}) "
            f"f32_le=({f32le}) "
            f"i32_le=({i32le})"
        )

    return 0


def _extract_points_f64_be(
    recs,
    only_type: int | None,
    stop_at_zero: bool,
    max_points: int | None,
) -> list[tuple[float, float, float]]:
    pts: list[tuple[float, float, float]] = []
    for r in recs:
        if only_type is not None and r.rec_type_u16 != only_type:
            continue
        if len(r.f64_be) < 2:
            continue

        x = float(r.f64_be[0])
        y = float(r.f64_be[1])
        z = float(r.f64_be[2]) if len(r.f64_be) >= 3 else 0.0

        if stop_at_zero and x == 0.0 and y == 0.0 and z == 0.0:
            break

        pts.append((x, y, z))
        if max_points is not None and len(pts) >= max_points:
            break
    return pts


# -------------------------
# Tool (parametric simple)
# -------------------------
@dataclass
class ToolSimple:
    tool_id: int
    name: str
    dia: float
    length: float


def _safe_max_pos(*vals: float) -> float:
    cands = [v for v in vals if isinstance(v, (int, float)) and v is not None and v > 0]
    return max(cands) if cands else 0.0


def load_tool_simple(cur, tool_id: int) -> ToolSimple | None:
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

    return ToolSimple(tool_id=_id, name=name or "", dia=dia, length=length)


def tool_cylinder_profile(tool: ToolSimple, tip_z: float) -> list[tuple[float, float]]:
    """
    2D断面（片側）プロファイル： (z, r)
    tip_z で工具先端（Z+側端面）を合わせる。
    """
    r = max(tool.dia * 0.5, 0.0)
    L = max(tool.length, 0.0)

    z0 = tip_z - L
    z1 = tip_z

    # 片側断面：Z軸方向に棒（半径r）
    return [
        (z0, 0.0),
        (z0, r),
        (z1, r),
        (z1, 0.0),
    ]


def mirror_profile(profile_zr: list[tuple[float, float]]) -> tuple[list[float], list[float]]:
    # profile_zr: [(z, r), ...]   r>=0
    z = [p[0] for p in profile_zr]
    r = [p[1] for p in profile_zr]
    # 上側 + 下側（反転）で閉じる
    z2 = z + z[::-1]
    y2 = r + [-v for v in r[::-1]]
    return z2, y2


def load_nctool_basic(cur, nctool_id: int):
    # NCToolsの列名はあなたのPRAGMA結果に合わせています
    cur.execute(
        """
        SELECT id, tool_id, holder_geometry_id, gage_length, holder_reach, tool_length
        FROM NCTools
        WHERE id = ?
        """,
        (nctool_id,),
    )
    return cur.fetchone()


def cmd_plot(
    db_path: Path,
    geometry_id: int,
    header: int | None,
    record_len: int | None,
    only_type: int | None,
    stop_at_zero: bool,
    max_points: int | None,
    swap_xy: bool,
    flip_x: bool,
    flip_y: bool,
    annotate: bool,
    save: Path | None,
    nctool_id: int | None,
    tool_tip_mode: str,
) -> int:
    """
    polyline(片側断面)を (R, Z) として解釈し、左右ミラーして断面として fill 表示。
    さらに NCTools の tool_id から簡易シリンダ形状を生成して同座標系に重ねる。

    座標解釈（重要）:
      pts = (x, y, z) だが、このDBでは z≈0 で (R=x, Z=y) が自然、という前提で固定。
    """
    import sqlite3
    import matplotlib.pyplot as plt

    # ---------------------------
    # helpers (この関数内で完結)
    # ---------------------------
    def build_mirror_polygon(rs: list[float], zs: list[float]) -> tuple[list[float], list[float]]:
        """右側断面(rs>=0想定)を左右ミラーして閉じたポリゴン (R, Z) を返す。"""
        rs_neg = [-r for r in rs[::-1]]
        zs_neg = zs[::-1]
        poly_r = rs + rs_neg
        poly_z = zs + zs_neg
        return poly_r, poly_z

    def apply_rz_transforms(R: float, Z: float) -> tuple[float, float]:
        """swap/flip を (R,Z) に適用。geometry と tool で同じ処理を使う。"""
        if swap_xy:
            Z, R = R, Z
        if flip_x:
            R = -R
        if flip_y:
            Z = -Z
        return R, Z

    def safe_max_pos(*vals: float) -> float:
        cands = []
        for v in vals:
            try:
                fv = float(v)
            except Exception:
                continue
            if fv > 0:
                cands.append(fv)
        return max(cands) if cands else 0.0

    # tool: (z,r) 片側プロファイルを作る
    def tool_cylinder_profile_zr(dia: float, length: float, tip_z: float) -> list[tuple[float, float]]:
        r = max(dia * 0.5, 0.0)
        L = max(length, 0.0)
        z0 = tip_z - L
        z1 = tip_z
        return [(z0, 0.0), (z0, r), (z1, r), (z1, 0.0)]

    def mirror_profile_zr(profile_zr: list[tuple[float, float]]) -> tuple[list[float], list[float]]:
        """
        profile_zr: [(Z, R), ...] (R>=0)
        戻り値: polyline の X,Y に使えるよう (Rlist, Zlist) ではなく
              ここでは (Zlist, Rlist) を返す作りにしている人が多いので注意。
        この関数は「左右ミラーで閉じた輪郭線」を返す。
        """
        zs = [p[0] for p in profile_zr]
        rs = [p[1] for p in profile_zr]
        zs2 = zs + zs[::-1]
        rs2 = rs + [-v for v in rs[::-1]]
        return zs2, rs2

    # NCTools から tool_id / gage_length / holder_reach / tool_length を取る（列が無いDBも想定して段階的に試す）
    def load_nctool_overlay_info(cur, nctool_id_: int):
        # まずは列が多い版（あなたのDBのPRAGMA出力に holder_reach, tool_length がある想定）
        candidates = [
            ("SELECT id, tool_id, gage_length, holder_reach, tool_length FROM NCTools WHERE id = ?", 5),
            ("SELECT id, tool_id, gage_length, holder_reach FROM NCTools WHERE id = ?", 4),
            ("SELECT id, tool_id, gage_length FROM NCTools WHERE id = ?", 3),
            ("SELECT id, tool_id FROM NCTools WHERE id = ?", 2),
        ]
        for sql, ncols in candidates:
            try:
                cur.execute(sql, (nctool_id_,))
                row = cur.fetchone()
                if row:
                    # 足りない分は None で埋める
                    row = tuple(row) + (None,) * (5 - len(row))
                    (nid, tool_id, gage_len, holder_reach, tool_len) = row[:5]
                    return nid, tool_id, gage_len, holder_reach, tool_len
            except Exception:
                pass
        return None

    # Tools から簡易形状(D, L)を推定
    def load_tool_simple_info(cur, tool_id_: int):
        try:
            cur.execute(
                """
                SELECT id, name, total_length,
                       dbl_param1, dbl_param2, dbl_param3, dbl_param4, dbl_param5, dbl_param6
                FROM Tools
                WHERE id = ?
                """,
                (tool_id_,),
            )
            row = cur.fetchone()
            if not row:
                return None
            (_id, name, total_len, p1, p2, p3, p4, p5, p6) = row
            length = float(total_len or 0.0)

            # 直径は「それっぽい候補の最大正値」で割り切り（parametric簡易）
            dia = safe_max_pos(p4, p1, p2)

            return {
                "tool_id": int(_id),
                "name": str(name or ""),
                "dia": float(dia),
                "length": float(length),
            }
        except Exception:
            return None

    # ---------------------------
    # 1) polyline 読み取り & 解析
    # ---------------------------
    blob = read_geometry_polyline_blob(db_path, geometry_id)

    fmt = PolylineFormat(header, record_len) if (header is not None and record_len is not None) else guess_polyline_format(blob)
    if fmt is None:
        print("[ERROR] format not guessed. try --header and --record-len")
        return 1

    try:
        _, recs = parse_polyline(blob, fmt)
    except Exception as e:
        print(f"[ERROR] parse failed: {e}")
        return 1

    pts = _extract_points_f64_be(recs, only_type=only_type, stop_at_zero=stop_at_zero, max_points=max_points)
    if not pts:
        print("[ERROR] no points extracted (check --type / header/record-len)")
        return 1

    # ---------------------------
    # 2) (R=x, Z=y) に変換 + transform
    # ---------------------------
    zs: list[float] = []
    rs: list[float] = []
    for (x, y, _z) in pts:
        Z = float(y)
        R = float(x)
        R, Z = apply_rz_transforms(R, Z)
        rs.append(R)
        zs.append(Z)

    # 表示範囲の参考（tool tip の zmax/zmin に使う）
    zmin, zmax = min(zs), max(zs)

    # ---------------------------
    # 3) plot geometry (mirror+fill)
    # ---------------------------
    fig, ax = plt.subplots()

    # geometry: mirror polygon fill
    poly_r, poly_z = build_mirror_polygon(rs, zs)
    ax.fill(poly_r, poly_z, alpha=0.25)

    # geometry outline (右/左)
    ax.plot(rs, zs, marker="o")
    ax.plot([-r for r in rs], zs)

    # 中心線
    ax.axvline(0.0, linewidth=1)

    # ラベル（基本は不要なら annotate=False で消える）
    if annotate:
        for i, (R, Z) in enumerate(zip(rs, zs)):
            ax.text(R, Z, str(i), fontsize=8)

    # タイトル等
    ttl = f"geometry_id={geometry_id} mirror+fill (header={fmt.header_len}, record={fmt.record_len}"
    if only_type is not None:
        ttl += f", type={only_type}"
    ttl += ")"
    ax.set_title(ttl)

    ax.set_xlabel("R" + (" (swapped)" if swap_xy else ""))
    ax.set_ylabel("Z" + (" (swapped)" if swap_xy else ""))
    ax.grid(True)
    ax.axis("equal")

    # ---------------------------
    # 4) tool overlay (mirror+fill)
    # ---------------------------
    if nctool_id is not None:
        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
        except Exception as e:
            print(f"[WARN] cannot open DB for tool overlay: {e}")
            cur = None
            conn = None

        if cur is not None:
            nct = load_nctool_overlay_info(cur, nctool_id)
            if not nct:
                print(f"[WARN] NCTools not found (or columns missing): id={nctool_id}")
            else:
                (_nid, tool_id, gage_len, holder_reach, tool_len) = nct

                # tip_z をどこに合わせるか（推測しない：モードで切替）
                if tool_tip_mode == "zero":
                    tip_z = 0.0
                elif tool_tip_mode == "zmax":
                    tip_z = float(zmax)
                elif tool_tip_mode == "zmin":
                    tip_z = float(zmin)
                else:  # "gage"
                    tip_z = float(gage_len or 0.0)

                tool = load_tool_simple_info(cur, int(tool_id))
                if tool and tool["dia"] > 0 and tool["length"] > 0:
                    prof_zr = tool_cylinder_profile_zr(tool["dia"], tool["length"], tip_z=tip_z)
                    tz, tr = mirror_profile_zr(prof_zr)  # tz:Zlist, tr:Rlist

                    # tool も同じ transform を適用してから描画（swap/flip の整合性）
                    tr2: list[float] = []
                    tz2: list[float] = []
                    for (Z, R) in zip(tz, tr):
                        R2, Z2 = apply_rz_transforms(float(R), float(Z))
                        tr2.append(R2)
                        tz2.append(Z2)

                    ax.fill(tr2, tz2, alpha=0.30)

                    # ラベルは「基本いらない」運用が良さそうなので annotate の時だけ出す
                    if annotate:
                        ax.text(
                            0.0,
                            tip_z,
                            f"TOOL tool_id={tool['tool_id']}  D={tool['dia']:g}  L={tool['length']:g}\n"
                            f"nctool={nctool_id} tip_mode={tool_tip_mode} tip_z={tip_z:g} "
                            f"gage={float(gage_len or 0):g} reach={float(holder_reach or 0):g} tool_len(nc)={float(tool_len or 0):g}",
                            fontsize=8,
                        )
                else:
                    print(f"[WARN] tool simple not available: tool_id={tool_id}")

            if conn is not None:
                conn.close()

    # ---------------------------
    # 5) save / show
    # ---------------------------
    if save is not None:
        save.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save, dpi=160)
        print(f"[OK] saved: {save}")
    else:
        plt.show()

    return 0



def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect hyperMILL ToolDB Geometries.polyline blobs")
    ap.add_argument("--db", required=True, type=Path, help="SQLite DB path (NC_Tool_log.db)")

    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_sum = sub.add_parser("summary", help="summarize record types for multiple geometries")
    ap_sum.add_argument("--sample", type=int, default=50, help="number of geometries to inspect (default: 50)")
    ap_sum.add_argument("--header", type=int, default=None, help="force header_len")
    ap_sum.add_argument("--record-len", type=int, default=None, help="force record_len")

    ap_dump = sub.add_parser("dump", help="dump one geometry polyline")
    ap_dump.add_argument("--geometry-id", type=int, required=True)
    ap_dump.add_argument("--header", type=int, default=None, help="force header_len")
    ap_dump.add_argument("--record-len", type=int, default=None, help="force record_len")
    ap_dump.add_argument("--max-recs", type=int, default=40, help="dump first N records (default: 40)")

    ap_plot = sub.add_parser("plot", help="plot section for one geometry (assume R=x, Z=y)")
    ap_plot.add_argument("--geometry-id", type=int, required=True)
    ap_plot.add_argument("--header", type=int, default=None, help="force header_len")
    ap_plot.add_argument("--record-len", type=int, default=None, help="force record_len")
    ap_plot.add_argument("--type", type=int, default=None, help="only use records of this u16 type (e.g. 76)")
    ap_plot.add_argument("--no-stop-at-zero", action="store_true", help="do not stop at (0,0,0) terminator")
    ap_plot.add_argument("--max-points", type=int, default=None, help="limit number of points")
    ap_plot.add_argument("--swap-xy", action="store_true", help="swap Z and R interpretation (test)")
    ap_plot.add_argument("--flip-x", action="store_true", help="flip R sign")
    ap_plot.add_argument("--flip-y", action="store_true", help="flip Z sign")
    ap_plot.add_argument("--annotate", action="store_true", help="label point indices")
    ap_plot.add_argument("--save", type=Path, default=None, help="save png to path instead of showing window")

    # 追加：NCToolから工具を簡易描画
    ap_plot.add_argument("--nctool-id", type=int, default=None, help="overlay TOOL (parametric) by nctool_id")
    ap_plot.add_argument(
        "--tool-tip",
        choices=["gage", "zero", "zmax", "zmin"],
        default="gage",
        help="where to place tool tip Z (default: gage). try 'zero' to debug local origin.",
    )

    args = ap.parse_args()
    db_path: Path = args.db

    if args.cmd == "summary":
        return cmd_summary(db_path, args.sample, args.header, args.record_len)

    if args.cmd == "dump":
        return cmd_dump(db_path, args.geometry_id, args.header, args.record_len, args.max_recs)

    if args.cmd == "plot":
        return cmd_plot(
            db_path=db_path,
            geometry_id=args.geometry_id,
            header=args.header,
            record_len=args.record_len,
            only_type=args.type,
            stop_at_zero=not args.no_stop_at_zero,
            max_points=args.max_points,
            swap_xy=args.swap_xy,
            flip_x=args.flip_x,
            flip_y=args.flip_y,
            annotate=args.annotate,
            save=args.save,
            nctool_id=args.nctool_id,
            tool_tip_mode=args.tool_tip,
        )

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
