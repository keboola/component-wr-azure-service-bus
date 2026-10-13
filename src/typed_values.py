"""Typed JSON serialisation of one input row for ``row_as_json`` mode.

The JSON type of a column comes from the input table's base data type (read through
``keboola.component``'s ``TableDefinition.schema``, which also normalises legacy
``KBC.datatype.basetype`` column metadata) and can be overridden per column in the
configuration. Only non-string columns are tracked: a column without an entry is
written as a JSON string, exactly as before.

Numbers are never routed through ``float``: the exact decimal text is validated,
normalised to JSON number syntax and written into the body verbatim, so a
``NUMERIC(38,10)`` value keeps every digit.
"""

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

# Bounds the adjusted exponent so a value such as "1e999999999" cannot expand into a
# gigantic digit string. FLOAT tops out near 1e308, so this never rejects real data.
_MAX_ADJUSTED_EXPONENT = 1000

_MAX_VALUE_PREVIEW = 40


class ValueConversionError(UserException):
    """A cell cannot be written as its column's JSON type (user-fixable, exit 1)."""


class _Unconvertible(ValueError):
    """A converter's rejection reason; wrapped into a :class:`ValueConversionError` with column and row."""


def resolve_column_types(table: TableDefinition, config: Configuration) -> dict[str, JsonValueType]:
    """Non-string JSON type per column; empty unless ``value_typing`` is ``typed``.

    Table base types are applied first, then the configured overrides, so an override
    always wins (a ``string`` override removes the column from the result).
    """
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
    """Serialise a CSV row to a JSON object, formatted like ``json.dumps(row, ensure_ascii=False)``.

    Raises :class:`ValueConversionError` naming the column and row when a cell cannot be
    converted, or when it is empty in a non-string column while ``empty_as_null`` is off.
    """
    parts = []
    for key, value in row.items():
        key_text = json.dumps("null" if key is None else key, ensure_ascii=False)
        parts.append(f"{key_text}: {_json_fragment(key, value, column_types, empty_as_null, row_number)}")
    return "{" + ", ".join(parts) + "}"


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
