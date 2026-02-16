# scripts/plot_mirror_fill_min.py
# Minimal: read polyline blob -> decode points -> mirror -> fill -> save/show
# Usage:
#   python .\scripts\plot_mirror_fill_min.py --db "D:\...\NC_Tool_log.db" --geometry-id 64 --header 74 --record-len 26 --type 76 --save .\out\geom64_fill.png --annotate
from __future__ import annotations

import argparse
import sqlite3
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Optional


# ----------------------------
# DB read (adjust table/column if your schema differs)
# ----------------------------
def read_geometry_polyline_blob(db_path: Path, geometry_id: int) -> bytes:
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    try:
        cur = conn.cursor()

        # Try common patterns. If your existing inspect_polyline already works,
        # you can replace this function body with your proven query.
        # 1) Geometry table with blob column named polyline / polyline_bytes
        candidates = [
            ("SELECT polyline FROM Geometry WHERE id=?", "Geometry.polyline"),
            ("SELECT polyline_bytes FROM Geometry WHERE id=?", "Geometry.polyline_bytes"),
            ("SELECT polyline FROM Geometries WHERE id=?", "Geometries.polyline"),
            ("SELECT polyline_bytes FROM Geometries WHERE id=?", "Geometries.polyline_bytes"),
        ]
        for sql, _tag in candidates:
            try:
                cur.execute(sql, (geometry_id,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    return bytes(row[0])
            except sqlite3.Error:
                continue

        # 2) If stored in a generic table (example: "Geometries" with "data")
        candidates2 = [
            ("SELECT data FROM Geometries WHERE id=?", "Geometries.data"),
            ("SELECT data FROM Geometry WHERE id=?", "Geometry.data"),
        ]
        for sql, _tag in candidates2:
            try:
                cur.execute(sql, (geometry_id,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    return bytes(row[0])
            except sqlite3.Error:
                continue

        raise RuntimeError(
            "Could not find polyline blob. "
            "Please adjust SQL in read_geometry_polyline_blob() to match your DB schema."
        )
    finally:
        conn.close()


# ----------------------------
# Polyline decode
# ----------------------------
@dataclass(frozen=True)
class PolyRec:
    idx: int
    rec_type: int
    payload: bytes

    def f64_be_xy(self) -> Tuple[float, float]:
        # payload: 24 bytes. Assume first 16 bytes are (x,y) float64 big-endian.
        x = struct.unpack(">d", self.payload[0:8])[0]
        y = struct.unpack(">d", self.payload[8:16])[0]
        return x, y


def iter_records(blob: bytes, header_len: int, record_len: int) -> Iterable[PolyRec]:
    # record format (your current assumption):
    #   u16 type (LE) + 24-byte payload  => 26 bytes total
    # (If you later find type endian differs, flip "<H" to ">H")
    off = header_len
    idx = 0
    while off + record_len <= len(blob):
        rec = blob[off : off + record_len]
        rec_type = struct.unpack("<H", rec[0:2])[0]
        payload = rec[2:]
        yield PolyRec(idx=idx, rec_type=rec_type, payload=payload)
        idx += 1
        off += record_len


def extract_points_f64_be(blob: bytes, header_len: int, record_len: int, target_type: int) -> List[Tuple[float, float, int]]:
    pts: List[Tuple[float, float, int]] = []
    for r in iter_records(blob, header_len, record_len):
        if r.rec_type != target_type:
            continue
        x, y = r.f64_be_xy()
        pts.append((x, y, r.idx))
    return pts


# ----------------------------
# Mirror + fill plot
# ----------------------------
def mirror_and_close_polygon(points: List[Tuple[float, float, int]]) -> List[Tuple[float, float]]:
    """
    Given right-side polyline points (x>=0 expected),
    create a closed polygon by mirroring across X=0.
    """
    if len(points) < 2:
        return []

    right = [(x, y) for x, y, _i in points]

    # Mirror: reverse order so polygon wraps around nicely
    left = [(-x, y) for x, y in reversed(right)]

    poly = right + left

    # Close polygon explicitly if needed
    if poly and poly[0] != poly[-1]:
        poly.append(poly[0])

    return poly


def plot_mirror_fill(
    points: List[Tuple[float, float, int]],
    annotate: bool,
    save_path: Optional[Path],
    title: str,
) -> None:
    import matplotlib.pyplot as plt

    if not points:
        raise RuntimeError("No points found for requested type/header/record_len.")

    # Polyline (right)
    xs = [x for x, y, _ in points]
    ys = [y for x, y, _ in points]

    # Mirrored polyline (left)
    mxs = [-x for x in reversed(xs)]
    mys = list(reversed(ys))

    # Filled polygon
    poly = mirror_and_close_polygon(points)
    px = [p[0] for p in poly]
    py = [p[1] for p in poly]

    fig, ax = plt.subplots()
    ax.set_title(title)

    # Fill first (so lines/points sit on top)
    ax.fill(px, py, alpha=0.25)

    # Draw polylines
    ax.plot(xs, ys, marker="o")
    ax.plot(mxs, mys, marker="o")

    # Centerline
    ax.axvline(0.0, linewidth=1)

    if annotate:
        for x, y, i in points:
            ax.annotate(str(i), (x, y), textcoords="offset points", xytext=(4, 4), fontsize=8)

    ax.set_aspect("equal", adjustable="datalim")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.grid(True)

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"[OK] saved: {save_path}")
    else:
        plt.show()


# ----------------------------
# CLI
# ----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite ToolDB")
    ap.add_argument("--geometry-id", type=int, required=True)
    ap.add_argument("--header", type=int, required=True, help="Header length in bytes")
    ap.add_argument("--record-len", type=int, required=True, help="Record length in bytes")
    ap.add_argument("--type", type=int, default=76, help="Record type to use as points")
    ap.add_argument("--save", default="", help="If set, save PNG to this path")
    ap.add_argument("--annotate", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    blob = read_geometry_polyline_blob(db_path, args.geometry_id)

    points = extract_points_f64_be(blob, args.header, args.record_len, args.type)

    save_path = Path(args.save) if args.save else None
    title = f"geometry_id={args.geometry_id} mirror+fill (header={args.header}, record={args.record_len}, type={args.type})"
    plot_mirror_fill(points, annotate=args.annotate, save_path=save_path, title=title)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
