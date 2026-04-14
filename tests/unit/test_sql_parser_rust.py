"""Tests for wxyc_etl.parser integration with sql_parser.py.

Verifies that the wxyc_etl Rust parser produces identical output to the
pure-Python fallback, and that the import priority chain works correctly.
"""

import os
import re
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

# --- Fixtures ---

DUMP_SQL = (
    "-- MySQL dump\n"
    "\n"
    "INSERT INTO `GENRE` VALUES (1,'Rock'),(2,'Jazz'),(3,'Electronic');\n"
    "INSERT INTO `FLOWSHEET_ENTRY_PROD` VALUES "
    "(100,'Autechre',NULL,'VI Scose Poise','Confield',NULL,50,NULL,'Warp',NULL,"
    "1710000000000,NULL,10,1,NULL,'S',NULL,NULL,0);\n"
    "INSERT INTO `FLOWSHEET_ENTRY_PROD` VALUES "
    "(101,'Stereolab',NULL,NULL,'Aluminum Tunes',NULL,51,NULL,'Duophonic',NULL,"
    "1710000001000,NULL,10,2,NULL,'S',NULL,NULL,1);\n"
    "INSERT INTO `LIBRARY_CODE` VALUES (50,1,NULL,NULL,NULL,NULL,NULL,'Autechre');\n"
    "INSERT INTO `LIBRARY_CODE` VALUES (51,1,NULL,NULL,NULL,NULL,NULL,'Stereolab');\n"
)


def _write_dump(content: str) -> Path:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, encoding="latin-1")
    f.write(content)
    f.close()
    return Path(f.name)


# --- Parity tests ---


class TestWxycEtlParserParity:
    """Verify wxyc_etl Rust parser produces identical output to pure-Python."""

    @pytest.fixture(autouse=True)
    def dump_file(self):
        self.path = _write_dump(DUMP_SQL)
        yield
        self.path.unlink(missing_ok=True)

    def _python_rows(self, table_name: str) -> list[tuple]:
        """Parse using pure-Python fallback only."""
        from semantic_index.sql_parser import parse_sql_values

        pattern = re.compile(r"INSERT INTO `" + re.escape(table_name) + r"`")
        rows = []
        with open(self.path, encoding="latin-1") as f:
            for line in f:
                if pattern.search(line):
                    rows.extend(parse_sql_values(line))
        return rows

    def _rust_rows(self, table_name: str) -> list[tuple]:
        """Parse using wxyc_etl Rust parser directly."""
        from wxyc_etl.parser import load_table_rows

        return [tuple(row) for row in load_table_rows(str(self.path), table_name)]

    def test_genre_parity(self):
        py = self._python_rows("GENRE")
        rs = self._rust_rows("GENRE")
        assert py == rs

    def test_flowsheet_parity(self):
        py = self._python_rows("FLOWSHEET_ENTRY_PROD")
        rs = self._rust_rows("FLOWSHEET_ENTRY_PROD")
        assert len(py) == len(rs) == 2
        assert py == rs

    def test_library_code_parity(self):
        py = self._python_rows("LIBRARY_CODE")
        rs = self._rust_rows("LIBRARY_CODE")
        assert py == rs

    def test_null_handling(self):
        """NULL values produce None in both parsers."""
        py = self._python_rows("FLOWSHEET_ENTRY_PROD")
        rs = self._rust_rows("FLOWSHEET_ENTRY_PROD")
        # Column 2 is NULL in both rows
        for py_row, rs_row in zip(py, rs, strict=True):
            assert py_row[2] is None
            assert rs_row[2] is None

    def test_escaped_quotes(self):
        sql = r"INSERT INTO `T` VALUES (1,'HONEST JON\'S/ASTRALWERKS');" + "\n"
        path = _write_dump(sql)
        try:
            from wxyc_etl.parser import load_table_rows

            from semantic_index.sql_parser import parse_sql_values

            rs = [tuple(r) for r in load_table_rows(str(path), "T")]
            py = parse_sql_values(sql.strip())
            assert rs == py == [(1, "HONEST JON'S/ASTRALWERKS")]
        finally:
            path.unlink(missing_ok=True)

    def test_empty_string(self):
        sql = "INSERT INTO `T` VALUES (1,'','notempty');\n"
        path = _write_dump(sql)
        try:
            from wxyc_etl.parser import load_table_rows

            from semantic_index.sql_parser import parse_sql_values

            rs = [tuple(r) for r in load_table_rows(str(path), "T")]
            py = parse_sql_values(sql.strip())
            assert rs == py == [(1, "", "notempty")]
        finally:
            path.unlink(missing_ok=True)

    def test_bigint_values(self):
        """Timestamps are bigint(20) — both parsers return int."""
        py = self._python_rows("FLOWSHEET_ENTRY_PROD")
        rs = self._rust_rows("FLOWSHEET_ENTRY_PROD")
        # Column 10 is the timestamp
        assert py[0][10] == rs[0][10] == 1710000000000
        assert isinstance(py[0][10], int)
        assert isinstance(rs[0][10], int)


# --- Import priority tests ---


class TestImportPriority:
    """Verify that wxyc_etl.parser is preferred over sql_parser_rs."""

    def test_wxyc_etl_preferred_over_sql_parser_rs(self):
        """When wxyc_etl.parser is available, _HAS_RUST should be True
        and the module should use wxyc_etl, not sql_parser_rs."""
        # Force a fresh import of sql_parser
        if "semantic_index.sql_parser" in sys.modules:
            del sys.modules["semantic_index.sql_parser"]

        import semantic_index.sql_parser as sp

        assert sp._HAS_RUST is True

    def test_sql_parser_rs_not_in_modules_when_wxyc_etl_available(self):
        """sql_parser_rs should not be loaded when wxyc_etl.parser works."""
        # Remove any prior import
        for mod_name in list(sys.modules):
            if mod_name.startswith("sql_parser_rs"):
                del sys.modules[mod_name]
        if "semantic_index.sql_parser" in sys.modules:
            del sys.modules["semantic_index.sql_parser"]

        import semantic_index.sql_parser as _sp  # noqa: F811

        assert _sp._HAS_RUST is True
        assert "sql_parser_rs" not in sys.modules


# --- Pure-Python fallback test ---


class TestPurePythonFallback:
    """Verify WXYC_ETL_NO_RUST forces pure-Python path."""

    def test_no_rust_env_forces_fallback(self):
        """Setting WXYC_ETL_NO_RUST=1 forces _HAS_RUST=False."""
        if "semantic_index.sql_parser" in sys.modules:
            del sys.modules["semantic_index.sql_parser"]

        with mock.patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            import semantic_index.sql_parser as sp

            assert sp._HAS_RUST is False

        # Clean up
        del sys.modules["semantic_index.sql_parser"]

    def test_fallback_produces_correct_output(self):
        """Pure-Python path produces correct results."""
        if "semantic_index.sql_parser" in sys.modules:
            del sys.modules["semantic_index.sql_parser"]

        path = _write_dump("INSERT INTO `GENRE` VALUES (1,'Rock'),(2,'Jazz');\n")
        try:
            with mock.patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
                import semantic_index.sql_parser as sp

                assert sp._HAS_RUST is False
                rows = list(sp.iter_table_rows(str(path), "GENRE"))
                assert rows == [(1, "Rock"), (2, "Jazz")]
        finally:
            path.unlink(missing_ok=True)
            if "semantic_index.sql_parser" in sys.modules:
                del sys.modules["semantic_index.sql_parser"]
