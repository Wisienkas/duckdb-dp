#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise SystemExit("This script requires pyarrow: pip install pyarrow")


RUN_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_\d+$")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

APP_LOG_LINE_RE = re.compile(
    r"""
    ^
    (?P<time>\d{2}:\d{2}:\d{2}\.\d{3})
    \s+\[(?P<thread>[^\]]+)\]
    \s+(?P<level>TRACE|DEBUG|INFO|WARN|ERROR|FATAL)
    \s+(?P<logger>.*?)
    \s+--\s+
    (?P<message>.*)
    $
    """,
    re.VERBOSE,
)

GC_LOG_RE = re.compile(
    r"""
    ^
    \[(?P<timestamp>[^\]]+)\]
    \[(?P<uptime>[^\]]+)\]
    \[(?P<level>[^\]]+)\]
    \[(?P<tags>[^\]]+)\]
    \s*(?P<message>.*)
    $
    """,
    re.VERBOSE,
)

JCMD_SECTION_RE = re.compile(r"^===== (?P<section>.+?) =====$")
KV_RE = re.compile(r"^(?P<key>[A-Za-z0-9_.\-/]+)\s*=\s*(?P<value>.*)$")


@dataclass
class Row:
    run_id: str
    run_dir: str
    source_file: str
    source_type: str
    record_id: str
    line_no: int
    key: str
    value: str | None
    value_num: float | None
    value_type: str
    raw: str


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


def try_parse_number(value: str) -> tuple[float | None, str]:
    v = value.strip()
    if v == "":
        return None, "empty"

    if v in {"-", "NA", "N/A", "null", "NULL", "None"}:
        return None, "null"

    if re.fullmatch(r"[+-]?\d+", v):
        try:
            return float(int(v)), "int"
        except ValueError:
            pass

    if re.fullmatch(r"[+-]?(?:\d+\.\d*|\.\d+|\d+)", v):
        try:
            return float(v), "float"
        except ValueError:
            pass

    return None, "string"


def make_row(
    *,
    run_id: str,
    run_dir: Path,
    source_file: Path,
    source_type: str,
    record_id: str,
    line_no: int,
    key: str,
    value: str | None,
    raw: str,
    force_type: str | None = None,
) -> Row:
    value_num = None
    value_type = force_type or "string"

    if force_type is None and value is not None:
        value_num, value_type = try_parse_number(value)

    return Row(
        run_id=run_id,
        run_dir=str(run_dir),
        source_file=str(source_file),
        source_type=source_type,
        record_id=record_id,
        line_no=line_no,
        key=key,
        value=value,
        value_num=value_num,
        value_type=value_type,
        raw=raw,
    )


def emit_raw_row(
    *,
    run_id: str,
    run_dir: Path,
    source_file: Path,
    source_type: str,
    record_id: str,
    line_no: int,
    raw: str,
) -> Row:
    return make_row(
        run_id=run_id,
        run_dir=run_dir,
        source_file=source_file,
        source_type=source_type,
        record_id=record_id,
        line_no=line_no,
        key="raw",
        value=raw,
        raw=raw,
        force_type="raw",
    )


def parse_simple_kv_file(
    run_id: str,
    run_dir: Path,
    file_path: Path,
    source_type: str,
) -> Iterator[Row]:
    for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue
        record_id = f"{source_type}:{line_no}"
        yield emit_raw_row(
            run_id=run_id, run_dir=run_dir, source_file=file_path,
            source_type=source_type, record_id=record_id, line_no=line_no, raw=raw
        )
        m = KV_RE.match(raw)
        if m:
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                key=m.group("key"),
                value=m.group("value"),
                raw=raw,
            )
        else:
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                key="text",
                value=raw,
                raw=raw,
                force_type="string",
            )


def parse_scalar_file(
    run_id: str,
    run_dir: Path,
    file_path: Path,
    source_type: str,
    scalar_key: str,
) -> Iterator[Row]:
    for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue
        record_id = f"{source_type}:{line_no}"
        yield emit_raw_row(
            run_id=run_id, run_dir=run_dir, source_file=file_path,
            source_type=source_type, record_id=record_id, line_no=line_no, raw=raw
        )
        yield make_row(
            run_id=run_id,
            run_dir=run_dir,
            source_file=file_path,
            source_type=source_type,
            record_id=record_id,
            line_no=line_no,
            key=scalar_key,
            value=raw,
            raw=raw,
        )


def parse_app_out_log(run_id: str, run_dir: Path, file_path: Path) -> Iterator[Row]:
    for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue
        record_id = f"app_out:{line_no}"
        yield emit_raw_row(
            run_id=run_id, run_dir=run_dir, source_file=file_path,
            source_type="app_out", record_id=record_id, line_no=line_no, raw=raw
        )

        m = KV_RE.match(raw)
        if m:
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type="app_out",
                record_id=record_id,
                line_no=line_no,
                key=m.group("key"),
                value=m.group("value"),
                raw=raw,
            )
            continue

        m = APP_LOG_LINE_RE.match(raw)
        if m:
            for key in ("time", "thread", "level", "logger", "message"):
                yield make_row(
                    run_id=run_id,
                    run_dir=run_dir,
                    source_file=file_path,
                    source_type="app_out",
                    record_id=record_id,
                    line_no=line_no,
                    key=key,
                    value=m.group(key),
                    raw=raw,
                    force_type="string",
                )
            continue

        yield make_row(
            run_id=run_id,
            run_dir=run_dir,
            source_file=file_path,
            source_type="app_out",
            record_id=record_id,
            line_no=line_no,
            key="text",
            value=raw,
            raw=raw,
            force_type="string",
        )


def parse_gc_log(run_id: str, run_dir: Path, file_path: Path) -> Iterator[Row]:
    for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue

        record_id = f"gc:{line_no}"
        yield emit_raw_row(
            run_id=run_id, run_dir=run_dir, source_file=file_path,
            source_type="gc", record_id=record_id, line_no=line_no, raw=raw
        )

        m = GC_LOG_RE.match(raw)
        if m:
            for key in ("timestamp", "uptime", "level", "tags", "message"):
                force_type = "string"
                if key == "uptime":
                    # "0.015s" -> numeric uptime_seconds too
                    uptime_s = m.group("uptime").rstrip("s")
                    yield make_row(
                        run_id=run_id,
                        run_dir=run_dir,
                        source_file=file_path,
                        source_type="gc",
                        record_id=record_id,
                        line_no=line_no,
                        key="uptime_seconds",
                        value=uptime_s,
                        raw=raw,
                    )
                yield make_row(
                    run_id=run_id,
                    run_dir=run_dir,
                    source_file=file_path,
                    source_type="gc",
                    record_id=record_id,
                    line_no=line_no,
                    key=key,
                    value=m.group(key),
                    raw=raw,
                    force_type=force_type,
                )
        else:
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type="gc",
                record_id=record_id,
                line_no=line_no,
                key="text",
                value=raw,
                raw=raw,
                force_type="string",
            )


def parse_space_table(
    run_id: str,
    run_dir: Path,
    file_path: Path,
    source_type: str,
    skip_prefixes: tuple[str, ...] = (),
    header_prefix: str | None = None,
) -> Iterator[Row]:
    lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()

    header: list[str] | None = None
    sample_idx = 0

    for line_no, line in enumerate(lines, start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue

        stripped = raw.strip()
        if any(stripped.startswith(p) for p in skip_prefixes):
            continue

        if header is None:
            if header_prefix is not None and not stripped.startswith(header_prefix):
                continue
            header = stripped.split()
            continue

        values = stripped.split()
        if len(values) < len(header):
            continue

        sample_idx += 1
        record_id = f"{source_type}:{sample_idx}"

        yield emit_raw_row(
            run_id=run_id,
            run_dir=run_dir,
            source_file=file_path,
            source_type=source_type,
            record_id=record_id,
            line_no=line_no,
            raw=raw,
        )

        for key, value in zip(header, values):
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                key=key,
                value=value,
                raw=raw,
            )


def parse_pidstat(run_id: str, run_dir: Path, file_path: Path) -> Iterator[Row]:
    lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
    header: list[str] | None = None
    sample_idx = 0

    for line_no, line in enumerate(lines, start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue
        stripped = raw.strip()

        if stripped.startswith("Linux "):
            continue

        if stripped.startswith("# Time"):
            header = stripped.lstrip("#").split()
            continue

        if header is None:
            continue

        parts = stripped.split()
        if len(parts) < len(header):
            continue

        sample_idx += 1
        record_id = f"pidstat:{sample_idx}"
        yield emit_raw_row(
            run_id=run_id,
            run_dir=run_dir,
            source_file=file_path,
            source_type="pidstat",
            record_id=record_id,
            line_no=line_no,
            raw=raw,
        )

        for key, value in zip(header, parts):
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type="pidstat",
                record_id=record_id,
                line_no=line_no,
                key=key,
                value=value,
                raw=raw,
            )


def parse_jcmd_file(run_id: str, run_dir: Path, file_path: Path, source_type: str) -> Iterator[Row]:
    current_section = "unknown"

    for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        raw = strip_ansi(line).rstrip()
        if not raw:
            continue

        section_match = JCMD_SECTION_RE.match(raw)
        if section_match:
            current_section = section_match.group("section")
            record_id = f"{source_type}:{line_no}"
            yield emit_raw_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                raw=raw,
            )
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                key="section",
                value=current_section,
                raw=raw,
                force_type="string",
            )
            continue

        record_id = f"{source_type}:{line_no}"
        yield emit_raw_row(
            run_id=run_id,
            run_dir=run_dir,
            source_file=file_path,
            source_type=source_type,
            record_id=record_id,
            line_no=line_no,
            raw=raw,
        )

        yield make_row(
            run_id=run_id,
            run_dir=run_dir,
            source_file=file_path,
            source_type=source_type,
            record_id=record_id,
            line_no=line_no,
            key="section",
            value=current_section,
            raw=raw,
            force_type="string",
        )

        if current_section == "VM.flags":
            # Example line: "-XX:MaxHeapSize=2147483648 -XX:+UseG1GC ..."
            for token in raw.split():
                if token.startswith("-XX:") or token.startswith("-X"):
                    yield make_row(
                        run_id=run_id,
                        run_dir=run_dir,
                        source_file=file_path,
                        source_type=source_type,
                        record_id=record_id,
                        line_no=line_no,
                        key="flag",
                        value=token,
                        raw=raw,
                        force_type="string",
                    )
            continue

        m = KV_RE.match(raw)
        if m:
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                key=m.group("key"),
                value=m.group("value"),
                raw=raw,
            )
        else:
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type=source_type,
                record_id=record_id,
                line_no=line_no,
                key="text",
                value=raw,
                raw=raw,
                force_type="string",
            )


def parse_file(run_id: str, run_dir: Path, file_path: Path) -> Iterator[Row]:
    name = file_path.name

    if name == "env.txt":
        yield from parse_simple_kv_file(run_id, run_dir, file_path, "env")
    elif name == "summary.txt":
        yield from parse_simple_kv_file(run_id, run_dir, file_path, "summary")
    elif name == "app.pid":
        yield from parse_scalar_file(run_id, run_dir, file_path, "app_pid", "pid")
    elif name == "app.out.log":
        yield from parse_app_out_log(run_id, run_dir, file_path)
    elif name == "gc.log":
        yield from parse_gc_log(run_id, run_dir, file_path)
    elif name == "jstat.log":
        yield from parse_space_table(run_id, run_dir, file_path, "jstat")
    elif name == "vmstat.log":
        yield from parse_space_table(
            run_id,
            run_dir,
            file_path,
            "vmstat",
            skip_prefixes=("procs ",),
            header_prefix="r ",
        )
    elif name == "pidstat.log":
        yield from parse_pidstat(run_id, run_dir, file_path)
    elif name == "jcmd-startup.txt":
        yield from parse_jcmd_file(run_id, run_dir, file_path, "jcmd_startup")
    elif name == "jcmd-shutdown.txt":
        yield from parse_jcmd_file(run_id, run_dir, file_path, "jcmd_shutdown")
    else:
        # Unknown file: still capture raw lines
        for line_no, line in enumerate(file_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            raw = strip_ansi(line).rstrip()
            if not raw:
                continue
            record_id = f"unknown:{line_no}"
            yield emit_raw_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type="unknown",
                record_id=record_id,
                line_no=line_no,
                raw=raw,
            )
            yield make_row(
                run_id=run_id,
                run_dir=run_dir,
                source_file=file_path,
                source_type="unknown",
                record_id=record_id,
                line_no=line_no,
                key="text",
                value=raw,
                raw=raw,
                force_type="string",
            )


def iter_run_dirs(base_dir: Path) -> Iterable[Path]:
    for child in sorted(base_dir.iterdir()):
        if child.is_dir() and RUN_DIR_RE.match(child.name):
            yield child


def iter_rows(base_dir: Path) -> Iterator[Row]:
    for run_dir in iter_run_dirs(base_dir):
        run_id = run_dir.name
        for file_path in sorted(p for p in run_dir.iterdir() if p.is_file()):
            yield from parse_file(run_id, run_dir, file_path)


def write_parquet(rows: Iterable[Row], output_path: Path, row_group_size: int = 100_000) -> None:
    schema = pa.schema([
        ("run_id", pa.string()),
        ("run_dir", pa.string()),
        ("source_file", pa.string()),
        ("source_type", pa.string()),
        ("record_id", pa.string()),
        ("line_no", pa.int64()),
        ("key", pa.string()),
        ("value", pa.string()),
        ("value_num", pa.float64()),
        ("value_type", pa.string()),
        ("raw", pa.string()),
    ])

    writer = None
    batch: list[dict] = []

    try:
        for row in rows:
            batch.append({
                "run_id": row.run_id,
                "run_dir": row.run_dir,
                "source_file": row.source_file,
                "source_type": row.source_type,
                "record_id": row.record_id,
                "line_no": row.line_no,
                "key": row.key,
                "value": row.value,
                "value_num": row.value_num,
                "value_type": row.value_type,
                "raw": row.raw,
            })

            if len(batch) >= row_group_size:
                table = pa.Table.from_pylist(batch, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, schema=schema, compression="zstd")
                writer.write_table(table)
                batch.clear()

        if batch:
            table = pa.Table.from_pylist(batch, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(output_path, schema=schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse all run log folders into one tall Parquet table."
    )
    parser.add_argument(
        "base_log_dir",
        help="Directory containing run folders like 2026-04-15_09-44-13_2481507",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output parquet file path",
    )

    args = parser.parse_args()

    base_dir = Path(args.base_log_dir)
    output_path = Path(args.output)

    if not base_dir.is_dir():
        raise SystemExit(f"Base log dir not found or not a directory: {base_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(iter_rows(base_dir), output_path)
    print(f"Wrote parquet to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())