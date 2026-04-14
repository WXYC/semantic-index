"""Parse MySQL dump files into Python tuples.

Uses a Rust extension (wxyc_etl.parser) for ~1000x faster parsing when available,
with a pure-Python fallback for environments where the Rust extension isn't built.
"""

import logging
import os
import re
from collections.abc import Iterator

logger = logging.getLogger(__name__)

_HAS_RUST = False
if not os.environ.get("WXYC_ETL_NO_RUST"):
    try:
        from wxyc_etl.parser import iter_table_rows as _rust_iter  # type: ignore[import-not-found]
        from wxyc_etl.parser import load_table_rows as _rust_load  # type: ignore[import-not-found]

        _HAS_RUST = True
    except ImportError:
        try:
            import sql_parser_rs as _rust  # type: ignore[import-untyped,import-not-found]

            _rust_iter = _rust.iter_table_rows
            _rust_load = _rust.load_table_rows
            _HAS_RUST = True
        except ImportError:
            pass

if not _HAS_RUST:
    logger.info("Rust SQL parser not available, using pure-Python fallback")


def parse_sql_values(line: str) -> list[tuple]:
    """Parse a MySQL INSERT INTO ... VALUES line into a list of row tuples.

    Handles strings (with escaped quotes/backslashes), ints, floats, and NULLs.
    Works with or without an explicit column list before VALUES.
    """
    values_match = re.search(r"\)\s*VALUES\s*\(", line, re.IGNORECASE)
    if values_match:
        values_start = values_match.end() - 1
    else:
        values_match = re.search(r"VALUES\s*\(", line, re.IGNORECASE)
        if not values_match:
            return []
        values_start = values_match.end() - 1

    data = line[values_start:]
    rows: list[tuple] = []
    current_row: list = []
    current_val: list[str] = []
    in_string = False
    i = 0

    while i < len(data):
        ch = data[i]
        if not in_string:
            if ch == "(":
                current_row = []
                current_val = []
                i += 1
            elif ch == ")":
                val_str = "".join(current_val).strip()
                current_row.append(_parse_value(val_str))
                rows.append(tuple(current_row))
                current_row = []
                current_val = []
                i += 1
            elif ch == ",":
                if current_val:
                    val_str = "".join(current_val).strip()
                    current_row.append(_parse_value(val_str))
                    current_val = []
                i += 1
            elif ch == "'":
                in_string = True
                current_val.append(ch)
                i += 1
            elif ch == ";":
                break
            else:
                current_val.append(ch)
                i += 1
        else:
            if ch == "\\" and i + 1 < len(data):
                current_val.append(ch)
                current_val.append(data[i + 1])
                i += 2
            elif ch == "'":
                current_val.append(ch)
                in_string = False
                i += 1
            else:
                current_val.append(ch)
                i += 1

    return rows


def _parse_value(val_str: str):
    """Convert a raw SQL value string to a Python object."""
    if val_str == "NULL" or val_str == "":
        return None
    if val_str.startswith("'") and val_str.endswith("'"):
        s = val_str[1:-1]
        s = s.replace("\\'", "'").replace("\\\\", "\\")
        return s
    try:
        return int(val_str)
    except ValueError:
        try:
            return float(val_str)
        except ValueError:
            return val_str


def iter_table_rows(path: str, table_name: str) -> Iterator[tuple]:
    """Yield parsed rows for a specific table from a MySQL dump file.

    Uses the Rust extension when available (~1000x faster). Falls back to
    pure-Python line-by-line parsing.
    """
    if _HAS_RUST:
        yield from _rust_iter(path, table_name)
    else:
        pattern = re.compile(r"INSERT INTO `" + re.escape(table_name) + r"`")
        with open(path, encoding="latin-1") as f:
            for line in f:
                if pattern.search(line):
                    yield from parse_sql_values(line)


def load_table_rows(path: str, table_name: str) -> list[tuple]:
    """Load all rows for a table into a list.

    Uses the Rust extension when available (~1000x faster).
    """
    if _HAS_RUST:
        return _rust_load(path, table_name)  # type: ignore[no-any-return]
    return list(iter_table_rows(path, table_name))
