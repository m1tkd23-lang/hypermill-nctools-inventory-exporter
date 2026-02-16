from __future__ import annotations

import sqlite3
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class PolylineFormat:
    header_len: int
    record_len: int


@dataclass(frozen=True)
class PolylineRecord:
    index: int
    offset: int
    rec_type_u16: int
    payload: bytes

    # 観測用：同じpayloadを複数の解釈で持つ（まだ確定しない）
    f64_le: tuple[float, ...]
    f64_be: tuple[float, ...]
    f32_le: tuple[float, ...]
    i32_le: tuple[int, ...]


def read_geometry_polyline_blob(db_path: Path, geometry_id: int) -> bytes:
    """
    Geometries.polyline (BLOB) を取得する。
    """
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT polyline FROM Geometries WHERE id = ?", (geometry_id,))
        row = cur.fetchone()
        if not row or row[0] is None:
            raise RuntimeError(f"Geometries.polyline が見つかりません: geometry_id={geometry_id}")
        blob = row[0]
        if not isinstance(blob, (bytes, bytearray, memoryview)):
            raise RuntimeError(f"polyline の型が想定外です: {type(blob)}")
        return bytes(blob)
    finally:
        conn.close()


def iter_geometry_ids_with_polyline(db_path: Path, limit: int = 2000) -> list[int]:
    """
    polyline を持つ geometry_id を列挙（先頭から）。
    """
    db_uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM Geometries
            WHERE polyline IS NOT NULL AND length(polyline) > 0
            ORDER BY id
            LIMIT ?
            """,
            (int(limit),),
        )
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _u16_le(b: bytes) -> int:
    return struct.unpack_from("<H", b, 0)[0]


def _try_unpack_f64(payload: bytes, endian: str) -> tuple[float, ...]:
    """
    endian: '<' or '>'
    """
    n = (len(payload) // 8) * 8
    if n <= 0:
        return tuple()
    cnt = n // 8
    try:
        return struct.unpack_from(endian + ("d" * cnt), payload, 0)
    except Exception:
        return tuple()


def _try_unpack_f32_le(payload: bytes) -> tuple[float, ...]:
    n = (len(payload) // 4) * 4
    if n <= 0:
        return tuple()
    cnt = n // 4
    try:
        return struct.unpack_from("<" + ("f" * cnt), payload, 0)
    except Exception:
        return tuple()


def _try_unpack_i32_le(payload: bytes) -> tuple[int, ...]:
    n = (len(payload) // 4) * 4
    if n <= 0:
        return tuple()
    cnt = n // 4
    try:
        return struct.unpack_from("<" + ("i" * cnt), payload, 0)
    except Exception:
        return tuple()


def parse_polyline(blob: bytes, fmt: PolylineFormat) -> tuple[bytes, list[PolylineRecord]]:
    """
    与えられた header_len / record_len で polyline を分解。
    先頭2バイトを u16(rec_type) とみなし、残りを payload として保持。
    payloadは f64(LE/BE), f32(LE), i32(LE) を併記して観測できるようにする。
    """
    if fmt.header_len < 0 or fmt.record_len <= 0:
        raise ValueError("invalid format")

    if len(blob) < fmt.header_len:
        raise ValueError("blob shorter than header")

    body = blob[fmt.header_len:]
    if (len(body) % fmt.record_len) != 0:
        raise ValueError("body not divisible by record_len")

    recs: list[PolylineRecord] = []
    nrec = len(body) // fmt.record_len

    for i in range(nrec):
        off = fmt.header_len + i * fmt.record_len
        chunk = blob[off : off + fmt.record_len]

        rec_type = _u16_le(chunk[:2])
        payload = chunk[2:]

        recs.append(
            PolylineRecord(
                index=i,
                offset=off,
                rec_type_u16=rec_type,
                payload=payload,
                f64_le=_try_unpack_f64(payload, "<"),
                f64_be=_try_unpack_f64(payload, ">"),
                f32_le=_try_unpack_f32_le(payload),
                i32_le=_try_unpack_i32_le(payload),
            )
        )

    header = blob[: fmt.header_len]
    return header, recs


def guess_polyline_format(
    blob: bytes,
    candidate_headers: Iterable[int] = (0, 16, 32, 48, 64, 74, 80, 96),
    candidate_record_lens: Iterable[int] = (16, 18, 20, 24, 26, 28, 32),
) -> PolylineFormat | None:
    """
    header_len / record_len をヒューリスティックに推定する。
    - bodyが record_len で割り切れる
    - 先頭2バイト(u16)の値が「小さめ」で偏る（=種別コードっぽい）
    """
    best: tuple[float, PolylineFormat] | None = None

    for h in candidate_headers:
        if h < 0 or h >= len(blob):
            continue
        body_len = len(blob) - h
        for rlen in candidate_record_lens:
            if rlen <= 2:
                continue
            if body_len <= 0 or (body_len % rlen) != 0:
                continue

            try:
                _, recs = parse_polyline(blob, PolylineFormat(h, rlen))
            except Exception:
                continue

            if not recs:
                continue

            types = [rec.rec_type_u16 for rec in recs]
            small_ratio = sum(1 for t in types if t <= 1024) / len(types)
            uniq = len(set(types))
            uniq_ratio = uniq / len(types)

            head_bias = 0.0
            head = types[: min(10, len(types))]
            if head:
                most = max(head.count(x) for x in set(head))
                head_bias = most / len(head)

            score = (small_ratio * 1.0) + ((1.0 - uniq_ratio) * 0.6) + (head_bias * 0.2)

            fmt = PolylineFormat(h, rlen)
            if best is None or score > best[0]:
                best = (score, fmt)

    return best[1] if best else None


def summarize_record_types(records: list[PolylineRecord]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for r in records:
        counts[r.rec_type_u16] = counts.get(r.rec_type_u16, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def hexdump(data: bytes, width: int = 16, max_bytes: int = 512) -> str:
    """
    先頭 max_bytes だけの簡易hexdump（観測用）。
    """
    b = data[:max_bytes]
    lines = []
    for i in range(0, len(b), width):
        chunk = b[i : i + width]
        hexs = " ".join(f"{x:02x}" for x in chunk)
        ascii_ = "".join(chr(x) if 32 <= x <= 126 else "." for x in chunk)
        lines.append(f"{i:08x}  {hexs:<{width*3}}  {ascii_}")
    if len(data) > max_bytes:
        lines.append(f"... ({len(data)} bytes total)")
    return "\n".join(lines)
