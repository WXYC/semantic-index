"""Parse MySQL dump files into Python tuples.

The parser is extracted from tubafrenzy/scripts/analysis/cross_genre_transitions.py
and extended with a streaming interface for large dump files (400MB+).
"""

import re
from collections.abc import Iterator


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

    Reads one line at a time to handle large dump files (400MB+) where each
    INSERT statement can be ~1MB.
    """
    pattern = re.compile(r"INSERT INTO `" + re.escape(table_name) + r"`")
    with open(path, encoding="latin-1") as f:
        for line in f:
            if pattern.search(line):
                yield from parse_sql_values(line)


def load_table_rows(path: str, table_name: str) -> list[tuple]:
    """Load all rows for a table into a list. Use for small tables only."""
    return list(iter_table_rows(path, table_name))
