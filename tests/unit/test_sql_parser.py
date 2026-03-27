"""Tests for the MySQL dump SQL parser."""

import tempfile
from pathlib import Path

from semantic_index.sql_parser import iter_table_rows, load_table_rows, parse_sql_values


class TestParseValues:
    """Tests for parse_sql_values — the character-by-character state machine."""

    def test_single_row_with_ints(self):
        line = "INSERT INTO `FOO` VALUES (1,2,3);"
        rows = parse_sql_values(line)
        assert rows == [(1, 2, 3)]

    def test_single_row_with_strings(self):
        line = "INSERT INTO `FOO` VALUES (1,'hello','world');"
        rows = parse_sql_values(line)
        assert rows == [(1, "hello", "world")]

    def test_multiple_rows(self):
        line = "INSERT INTO `FOO` VALUES (1,'a'),(2,'b'),(3,'c');"
        rows = parse_sql_values(line)
        assert rows == [(1, "a"), (2, "b"), (3, "c")]

    def test_null_values(self):
        line = "INSERT INTO `FOO` VALUES (1,NULL,'text',NULL);"
        rows = parse_sql_values(line)
        assert rows == [(1, None, "text", None)]

    def test_escaped_single_quote(self):
        line = r"INSERT INTO `FOO` VALUES (1,'HONEST JON\'S/ASTRALWERKS');"
        rows = parse_sql_values(line)
        assert rows == [(1, "HONEST JON'S/ASTRALWERKS")]

    def test_escaped_backslash(self):
        line = "INSERT INTO `FOO` VALUES (1,'back\\\\slash');"
        rows = parse_sql_values(line)
        assert rows == [(1, "back\\slash")]

    def test_empty_string(self):
        line = "INSERT INTO `FOO` VALUES (1,'','notempty');"
        rows = parse_sql_values(line)
        assert rows == [(1, "", "notempty")]

    def test_float_value(self):
        line = "INSERT INTO `FOO` VALUES (1,3.14,'pi');"
        rows = parse_sql_values(line)
        assert rows == [(1, 3.14, "pi")]

    def test_negative_int(self):
        line = "INSERT INTO `FOO` VALUES (-1,'neg');"
        rows = parse_sql_values(line)
        assert rows == [(-1, "neg")]

    def test_with_column_list(self):
        """INSERT statements can include an explicit column list before VALUES."""
        line = "INSERT INTO `FOO` (`id`, `name`) VALUES (1,'bar');"
        rows = parse_sql_values(line)
        assert rows == [(1, "bar")]

    def test_no_values_returns_empty(self):
        line = "CREATE TABLE `FOO` (id INT);"
        rows = parse_sql_values(line)
        assert rows == []

    def test_string_with_comma(self):
        line = "INSERT INTO `FOO` VALUES (1,'hello, world');"
        rows = parse_sql_values(line)
        assert rows == [(1, "hello, world")]

    def test_string_with_parentheses(self):
        line = "INSERT INTO `FOO` VALUES (1,'(remix)');"
        rows = parse_sql_values(line)
        assert rows == [(1, "(remix)")]

    def test_bigint_value(self):
        """Flowsheet timestamps are bigint(20) — milliseconds since epoch."""
        line = "INSERT INTO `FOO` VALUES (1,1710000000000);"
        rows = parse_sql_values(line)
        assert rows == [(1, 1710000000000)]


class TestIterTableRows:
    """Tests for the streaming file reader."""

    def _write_dump(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, encoding="latin-1")
        f.write(content)
        f.close()
        return Path(f.name)

    def test_yields_rows_for_matching_table(self):
        path = self._write_dump(
            "INSERT INTO `GENRE` VALUES (1,'Rock'),(2,'Jazz');\n"
            "INSERT INTO `OTHER` VALUES (99,'skip');\n"
        )
        rows = list(iter_table_rows(str(path), "GENRE"))
        assert rows == [(1, "Rock"), (2, "Jazz")]

    def test_skips_non_matching_tables(self):
        path = self._write_dump(
            "INSERT INTO `OTHER` VALUES (1,'skip');\n" "INSERT INTO `GENRE` VALUES (1,'Rock');\n"
        )
        rows = list(iter_table_rows(str(path), "GENRE"))
        assert rows == [(1, "Rock")]

    def test_multiple_insert_lines_for_same_table(self):
        path = self._write_dump(
            "INSERT INTO `GENRE` VALUES (1,'Rock');\n" "INSERT INTO `GENRE` VALUES (2,'Jazz');\n"
        )
        rows = list(iter_table_rows(str(path), "GENRE"))
        assert rows == [(1, "Rock"), (2, "Jazz")]

    def test_skips_comments_and_blanks(self):
        path = self._write_dump("-- MySQL dump\n" "\n" "INSERT INTO `GENRE` VALUES (1,'Rock');\n")
        rows = list(iter_table_rows(str(path), "GENRE"))
        assert rows == [(1, "Rock")]

    def test_empty_file(self):
        path = self._write_dump("")
        rows = list(iter_table_rows(str(path), "GENRE"))
        assert rows == []


class TestLoadTableRows:
    """Tests for the convenience wrapper."""

    def test_collects_into_list(self):
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False, encoding="latin-1")
        f.write("INSERT INTO `GENRE` VALUES (1,'Rock'),(2,'Jazz');\n")
        f.close()
        rows = load_table_rows(f.name, "GENRE")
        assert isinstance(rows, list)
        assert len(rows) == 2
