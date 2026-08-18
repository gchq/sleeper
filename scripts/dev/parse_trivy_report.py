#!/usr/bin/env python3
"""Extract vulnerability tables from Trivy output or a CI job log.

Trivy prints its results as box-drawing tables where a value that repeats
down a column is rendered once and left blank underneath (a "rowspan").
This script reconstructs the real rows by tracking, per column, whether a
separator line between two physical rows shows dashes (the cell ends, a new
value starts next) or blank space (the previous value carries down).

Usage:
    parse_trivy_report.py <job-log-file> [--json|--ignores]

With no flags, prints a human-readable summary.
With --json, dumps the full parsed structure (one entry per section) as JSON.
With --ignores, writes entries for a Trivy ignore file.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field

TIMESTAMP_RE = re.compile(r"^\S+Z (.*)$")
URL_RE = re.compile(r"https?://\S+")


def strip_timestamp(line: str) -> str:
    line = line.rstrip("\n")
    m = TIMESTAMP_RE.match(line)
    return m.group(1) if m else line


@dataclass
class Section:
    name: str
    summary: str | None = None
    columns: list[str] = field(default_factory=list)
    entries: list[dict] = field(default_factory=list)


def boundary_positions(border_line: str) -> list[int]:
    return [i for i, ch in enumerate(border_line) if ch in "┌┬┐└┴┘"]


def slice_columns(line: str, bounds: list[int]) -> list[str]:
    cells = []
    for i in range(len(bounds) - 1):
        start, end = bounds[i] + 1, bounds[i + 1]
        cells.append(line[start:end] if end <= len(line) else line[start:])
    return cells


def is_separator_line(line: str, bounds: list[int]) -> bool:
    # A data row always has '│' at every boundary position; a separator/
    # border row has at least one junction character ('┼┤├┴┬') instead,
    # wherever some column's cell starts or ends at that row.
    return any(p < len(line) and line[p] != "│" for p in bounds)


def is_data_line(line: str) -> bool:
    return line.startswith("│")


def parse_table(lines: list[str], start: int) -> tuple[list[str], list[dict], int]:
    """Parse a box-drawing table starting at lines[start] (the '┌' border).

    Returns (column_names, rows, index_after_table).
    """
    bounds = boundary_positions(lines[start])
    ncols = len(bounds) - 1
    columns = [c.strip() for c in slice_columns(lines[start + 1], bounds)]

    idx = start + 3  # skip top border, header row, header separator
    current = [""] * ncols
    building: list[list[str]] = [[] for _ in range(ncols)]
    rows: list[dict] = []

    while idx < len(lines):
        line = lines[idx]
        if is_separator_line(line, bounds):
            cells = slice_columns(line, bounds)
            for i in range(ncols):
                text = " ".join(t for t in building[i] if t).strip()
                if text:
                    current[i] = text
            rows.append(dict(zip(columns, current)))
            for i in range(ncols):
                if "─" in cells[i]:
                    current[i] = ""
            building = [[] for _ in range(ncols)]
            idx += 1
            if "┘" in line:
                break
        elif is_data_line(line):
            cells = slice_columns(line, bounds)
            for i in range(ncols):
                text = cells[i].strip()
                if text:
                    building[i].append(text)
            idx += 1
        else:
            # GitHub Actions log capture sometimes interleaves unrelated
            # lines (e.g. Trivy's own update-notice banner) mid-table, since
            # it's written to stderr concurrently with the table on stdout.
            # Skip past it and keep looking for the table's real close.
            idx += 1

    return columns, rows, idx


def split_references(value: str) -> tuple[str, list[str]]:
    urls = URL_RE.findall(value)
    text = URL_RE.sub("", value).strip()
    text = re.sub(r"\s+", " ", text)
    return text, urls


def parse_sections(path: str) -> list[Section]:
    with open(path, encoding="utf-8", errors="replace") as f:
        content = [strip_timestamp(l) for l in f]

    sections: list[Section] = []
    i = 1
    while i < len(content):
        if (
            re.fullmatch(r"=+", content[i])
            and content[i - 1].strip()
            and not content[i - 1].startswith(("│", "├", "└", "┌"))
        ):
            section = Section(name=content[i - 1].strip())
            j = i + 1
            while j < len(content) and not content[j].strip():
                j += 1
            if j < len(content) and content[j].startswith("Total:"):
                section.summary = content[j].strip()
                j += 1
                while j < len(content) and not content[j].strip():
                    j += 1

            if j < len(content) and content[j].startswith("┌"):
                columns, rows, j = parse_table(content, j)
                section.columns = columns
                for row in rows:
                    entry = dict(row)
                    if columns:
                        title_col = columns[-1]
                        text, urls = split_references(entry.get(title_col, ""))
                        entry[title_col] = text
                        if urls:
                            entry["References"] = urls
                    section.entries.append(entry)

            sections.append(section)
            i = j
        else:
            i += 1

    return sections


def print_summary(sections: list[Section]) -> None:
    for section in sections:
        print(f"## {section.name}")
        if section.summary:
            print(section.summary)
        if not section.entries:
            print("  (no table found)")
            print()
            continue

        by_library: dict[str, list[dict]] = {}
        lib_col = section.columns[0] if section.columns else "Library"
        vuln_col = section.columns[1] if len(section.columns) > 1 else "Vulnerability"
        for entry in section.entries:
            by_library.setdefault(entry.get(lib_col, ""), []).append(entry)

        for library, entries in by_library.items():
            print(f"  {library}")
            for entry in entries:
                vuln = entry.get(vuln_col, "")
                extras = ", ".join(
                    f"{k}={v}"
                    for k, v in entry.items()
                    if k not in (lib_col, vuln_col, "References") and v
                )
                print(f"    - {vuln} {extras}".rstrip())
        print()


def print_ignores(sections: list[Section]) -> None:
    for section in sections:
        print(f"  ## {section.name}")
        if not section.entries:
            print("  ## (no table found)")
            print()
            continue

        by_lib: dict[str, list[dict]] = {}
        lib_col = section.columns[0] if section.columns else "Library"
        vuln_col = section.columns[1] if len(section.columns) > 1 else "Vulnerability"
        for entry in section.entries:
            library = entry.get(lib_col, "").split(" ")[0]
            by_lib.setdefault(library, []).append(entry)

        for library, entries in by_lib.items():
            vulnerabilities = list(
                dict.fromkeys([entry.get(vuln_col, "") for entry in entries])
            )
            files = []
            for entry in entries:
                for file in entry.get(lib_col, "").split(" ")[1:]:
                    files.append(file.strip("()"))
            files = list(dict.fromkeys(files))
            if not files:
                files_str = ""
            else:
                files_str = f" ({', '.join(files)})"
            print(f"  # {library}{files_str}")
            for vulnerability in vulnerabilities:
                print(f"  - id: {vulnerability}")
        print()


def main() -> None:
    args = sys.argv[1:]
    as_json = "--json" in args
    as_ignores = "--ignores" in args
    args = [a for a in args if a not in ["--json", "--ignores"]]
    if len(args) != 1:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    sections = parse_sections(args[0])
    if as_json:
        json.dump(
            [
                {
                    "name": s.name,
                    "summary": s.summary,
                    "columns": s.columns,
                    "entries": s.entries,
                }
                for s in sections
            ],
            sys.stdout,
            indent=2,
        )
        print()
    elif as_ignores:
        print_ignores(sections)
    else:
        print_summary(sections)


if __name__ == "__main__":
    main()
