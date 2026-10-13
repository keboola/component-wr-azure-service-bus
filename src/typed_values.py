import json
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from keboola.component.dao import ColumnDefinition, TableDefinition
from keboola.component.exceptions import UserException

from configuration import Configuration, JsonValueType, ValueTyping

_BASE_TYPE_TO_JSON = {
    "INTEGER": JsonValueType.INTEGER,
    "NUMERIC": JsonValueType.NUMBER,
    "FLOAT": JsonValueType.NUMBER,
    "BOOLEAN": JsonValueType.BOOLEAN,
}

_INTEGER_RE = re.compile(r"[+-]?[0-9]+")
_NUMBER_RE = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?")
_TRUE_VALUES = frozenset({"true", "t", "1", "yes"})
_FALSE_VALUES = frozenset({"false", "f", "0", "no"})

_MAX_ADJUSTED_EXPONENT = 1000

_MAX_VALUE_PREVIEW = 40


class ValueConversionError(UserException):
    pass


class _Unconvertible(ValueError):
    pass


def resolve_column_types(table: TableDefinition, config: Configuration) -> dict[str, JsonValueType]:
    if config.value_typing != ValueTyping.TYPED:
        return {}
    types: dict[str, JsonValueType] = {}
    for name, column in table.schema.items():
        json_type = _BASE_TYPE_TO_JSON.get(_base_type_name(column))
        if json_type:
            types[name] = json_type
    for override in config.column_types:
        if override.type == JsonValueType.STRING:
            types.pop(override.column, None)
        else:
            types[override.column] = override.type
    return types


def _base_type_name(column: ColumnDefinition) -> str:
    base = (column.data_types or {}).get("base")
    return str(getattr(base, "dtype", "") or "").upper()


def serialize_row(
    row: Mapping[Any, Any],
    column_types: Mapping[str, JsonValueType],
    empty_as_null: bool,
    row_number: int | None,
) -> str:
    parts = []
    for key, value in row.items():
        parts.append(f"{_key_text(key)}: {_json_fragment(key, value, column_types, empty_as_null, row_number)}")
    return "{" + ", ".join(parts) + "}"


def _key_text(key: str | None) -> str:
    return json.dumps("null" if key is None else key, ensure_ascii=False)


def _json_fragment(
    key: str | None,
    value: Any,
    column_types: Mapping[str, JsonValueType],
    empty_as_null: bool,
    row_number: int | None,
) -> str:
    target = column_types.get(key) if key is not None else None
    if value is None or value == "":
        if empty_as_null:
            return "null"
        if target is None:
            return json.dumps(value, ensure_ascii=False)
        raise ValueConversionError(
            f"Empty value in {target} column '{key}' (row {row_number}). Enable 'Empty cells as null' "
            f"to send null, or set the column's type override to string."
        )
    if target is None or not isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    try:
        return _CONVERTERS[target](value)
    except _Unconvertible as e:
        raise ValueConversionError(
            f"Cannot convert value {_preview(value)} in column '{key}' to {target} (row {row_number}): {e}"
        ) from e


def _to_integer(text: str) -> str:
    stripped = text.strip()
    if not _INTEGER_RE.fullmatch(stripped):
        raise _Unconvertible("not a whole number")
    try:
        return str(int(stripped))
    except ValueError as e:
        raise _Unconvertible("too many digits") from e


def _to_number(text: str) -> str:
    stripped = text.strip()
    if not _NUMBER_RE.fullmatch(stripped):
        raise _Unconvertible("not a number")
    number = Decimal(stripped)
    if abs(number.adjusted()) > _MAX_ADJUSTED_EXPONENT:
        raise _Unconvertible("magnitude out of the supported range")
    return format(number, "f")


def _to_boolean(text: str) -> str:
    lowered = text.strip().lower()
    if lowered in _TRUE_VALUES:
        return "true"
    if lowered in _FALSE_VALUES:
        return "false"
    raise _Unconvertible("expected true/false, t/f, 1/0 or yes/no")


_CONVERTERS = {
    JsonValueType.INTEGER: _to_integer,
    JsonValueType.NUMBER: _to_number,
    JsonValueType.BOOLEAN: _to_boolean,
}


def _preview(value: str) -> str:
    shown = value if len(value) <= _MAX_VALUE_PREVIEW else value[:_MAX_VALUE_PREVIEW] + "..."
    return repr(shown)
