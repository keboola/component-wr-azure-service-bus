import json
from collections import OrderedDict

import pytest
from keboola.component.dao import BaseType, ColumnDefinition, DataType, TableDefinition
from keboola.component.exceptions import UserException

from configuration import Configuration, JsonValueType
from message_builder import build_message
from typed_values import ValueConversionError, resolve_column_types, serialize_row

BASE = {
    "auth_type": "connection_string",
    "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=s",
    "destination_type": "queue",
    "entity_name": "q1",
}

INT, NUM, BOOL = JsonValueType.INTEGER, JsonValueType.NUMBER, JsonValueType.BOOLEAN


def _table(types: dict[str, str | None]) -> TableDefinition:
    schema = OrderedDict()
    for name, dtype in types.items():
        schema[name] = ColumnDefinition(data_types={"base": DataType(dtype=dtype)} if dtype else {})
    table = TableDefinition(name="t.csv", full_path="/tmp/t.csv")
    table.schema = schema
    return table


def _body(row, column_types=None, empty_as_null=False, row_number=1, **cfg):
    config = Configuration(**{**BASE, "empty_as_null": empty_as_null, **cfg})
    return str(build_message(row, config, column_types, row_number))


class TestResolveColumnTypes:
    def test_string_typing_resolves_nothing(self):
        config = Configuration(**BASE)
        assert resolve_column_types(_table({"a": "INTEGER"}), config) == {}

    def test_maps_every_base_type(self):
        table = _table(
            {
                "i": "INTEGER",
                "n": "NUMERIC",
                "f": "FLOAT",
                "b": "BOOLEAN",
                "s": "STRING",
                "d": "DATE",
                "t": "TIMESTAMP",
                "u": None,
            }
        )
        config = Configuration(**BASE, value_typing="typed")
        assert resolve_column_types(table, config) == {"i": INT, "n": NUM, "f": NUM, "b": BOOL}

    def test_base_type_is_case_insensitive(self):
        config = Configuration(**BASE, value_typing="typed")
        assert resolve_column_types(_table({"a": "integer"}), config) == {"a": INT}

    def test_backend_only_type_is_not_guessed(self):
        table = TableDefinition(name="t.csv", full_path="/tmp/t.csv")
        table.schema = OrderedDict(a=ColumnDefinition(data_types={"snowflake": DataType(dtype="NUMBER")}))
        config = Configuration(**BASE, value_typing="typed")
        assert resolve_column_types(table, config) == {}

    def test_basetype_helper_objects_are_read(self):
        table = TableDefinition(name="t.csv", full_path="/tmp/t.csv")
        table.schema = OrderedDict(a=ColumnDefinition(data_types=BaseType.integer()))
        config = Configuration(**BASE, value_typing="typed")
        assert resolve_column_types(table, config) == {"a": INT}

    def test_override_wins_over_table_type(self):
        config = Configuration(
            **BASE,
            value_typing="typed",
            column_types=[{"column": "a", "type": "string"}, {"column": "b", "type": "boolean"}],
        )
        table = _table({"a": "INTEGER", "b": "STRING", "c": "FLOAT"})
        assert resolve_column_types(table, config) == {"b": BOOL, "c": NUM}

    def test_override_on_untyped_table(self):
        config = Configuration(**BASE, value_typing="typed", column_types=[{"column": "a", "type": "integer"}])
        assert resolve_column_types(_table({"a": None}), config) == {"a": INT}


class TestSerialization:
    def test_all_types(self):
        row = {"i": "2", "n": "1.89", "f": "0.5", "b": "true", "s": "x"}
        types = {"i": INT, "n": NUM, "f": NUM, "b": BOOL}
        assert serialize_row(row, types, False, 1) == '{"i": 2, "n": 1.89, "f": 0.5, "b": true, "s": "x"}'

    def test_matches_json_dumps_for_strings(self):
        row = {"a": "Košík", "b": 'q"uote', "c": ""}
        assert serialize_row(row, {}, False, 1) == json.dumps(row, ensure_ascii=False)

    def test_default_path_is_byte_identical(self):
        row = {"a": "1", "b": "", "c": "Košík"}
        assert _body(row) == json.dumps(row, ensure_ascii=False)

    def test_typed_output_is_valid_json(self):
        body = serialize_row({"i": "-7", "n": "-0.25", "b": "0"}, {"i": INT, "n": NUM, "b": BOOL}, False, 1)
        assert json.loads(body) == {"i": -7, "n": -0.25, "b": False}

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("true", "true"),
            ("TRUE", "true"),
            ("T", "true"),
            ("1", "true"),
            ("yes", "true"),
            ("false", "false"),
            ("False", "false"),
            ("f", "false"),
            ("0", "false"),
            ("no", "false"),
            (" true ", "true"),
        ],
    )
    def test_boolean_spellings(self, raw, expected):
        assert serialize_row({"b": raw}, {"b": BOOL}, False, 1) == f'{{"b": {expected}}}'

    @pytest.mark.parametrize(("raw", "expected"), [("007", "7"), ("+5", "5"), (" 12 ", "12"), ("-0", "0")])
    def test_integer_normalisation(self, raw, expected):
        assert serialize_row({"i": raw}, {"i": INT}, False, 1) == f'{{"i": {expected}}}'

    def test_integer_beyond_int64_is_exact(self):
        big = "123456789012345678901234567890"
        assert serialize_row({"i": big}, {"i": INT}, False, 1) == f'{{"i": {big}}}'


class TestDecimalPrecision:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("12345678901234567890.123456789012345678", "12345678901234567890.123456789012345678"),
            ("0.1", "0.1"),
            ("1.50", "1.50"),
            ("0.30000000000000004", "0.30000000000000004"),
            (".5", "0.5"),
            ("5.", "5"),
            ("+1.5", "1.5"),
            ("1e-7", "0.0000001"),
            ("1.5E+3", "1500"),
            ("-0.000", "-0.000"),
        ],
    )
    def test_text_is_kept_exact_and_valid_json(self, raw, expected):
        body = serialize_row({"n": raw}, {"n": NUM}, False, 1)
        assert body == f'{{"n": {expected}}}'
        json.loads(body)

    def test_not_rounded_through_float(self):
        body = serialize_row({"n": "9007199254740993.5"}, {"n": NUM}, False, 1)
        assert "9007199254740993.5" in body
        assert json.loads(body, parse_float=str)["n"] == "9007199254740993.5"


class TestEmptyHandling:
    def test_empty_as_null_nulls_every_column(self):
        row = {"i": "", "s": "", "b": ""}
        types = {"i": INT, "b": BOOL}
        assert serialize_row(row, types, True, 1) == '{"i": null, "s": null, "b": null}'

    def test_empty_as_null_with_string_typing(self):
        assert _body({"a": "", "b": "x"}, empty_as_null=True) == '{"a": null, "b": "x"}'

    def test_empty_string_column_stays_empty_string_when_typing_on(self):
        assert serialize_row({"i": "1", "s": ""}, {"i": INT}, False, 1) == '{"i": 1, "s": ""}'

    def test_empty_non_string_column_without_flag_fails(self):
        with pytest.raises(ValueConversionError) as exc:
            serialize_row({"i": ""}, {"i": INT}, False, 5)
        message = str(exc.value)
        assert "'i'" in message
        assert "row 5" in message
        assert "Empty cells as null" in message

    def test_ragged_missing_cell_in_non_string_column_fails(self):
        with pytest.raises(ValueConversionError):
            serialize_row({"i": None}, {"i": INT}, False, 2)

    def test_ragged_missing_cell_in_string_column_is_null_as_before(self):
        assert serialize_row({"a": "1", "b": None}, {"a": INT}, False, 1) == '{"a": 1, "b": null}'

    def test_extra_cells_key_is_serialised_like_json_dumps(self):
        row = {"a": "1", None: ["x", "y"]}
        assert serialize_row(row, {"a": INT}, False, 1) == '{"a": 1, "null": ["x", "y"]}'


class TestConversionErrors:
    @pytest.mark.parametrize(
        ("target", "raw"),
        [
            (INT, "abc"),
            (INT, "1.5"),
            (INT, "1,5"),
            (INT, "1_000"),
            (INT, "9" * 5000),
            (NUM, "abc"),
            (NUM, "NaN"),
            (NUM, "Infinity"),
            (NUM, "1_0.5"),
            (NUM, "1e99999999"),
            (NUM, "1.2.3"),
            (BOOL, "maybe"),
            (BOOL, "2"),
        ],
    )
    def test_unconvertible_value_is_a_user_exception(self, target, raw):
        with pytest.raises(UserException):
            serialize_row({"c": raw}, {"c": target}, False, 3)

    def test_error_names_column_value_row_and_type(self):
        with pytest.raises(ValueConversionError) as exc:
            serialize_row({"price": "abc"}, {"price": NUM}, False, 12)
        message = str(exc.value)
        assert "'abc'" in message
        assert "column 'price'" in message
        assert "number" in message
        assert "row 12" in message

    def test_long_value_is_truncated(self):
        with pytest.raises(ValueConversionError) as exc:
            serialize_row({"c": "x" * 500}, {"c": INT}, False, 1)
        assert "x" * 500 not in str(exc.value)
        assert "..." in str(exc.value)
