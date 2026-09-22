"""Build a ServiceBusMessage from one input-table row.

The body is produced per ``mode`` (``row_as_json`` serialises the whole row;
``column_value`` takes one column, passing it through when it is valid JSON and
otherwise wrapping it as ``{"data": <raw>}``). Configured ``message_properties``
column mappings are applied onto the matching ServiceBusMessage attributes; a
row-level mapping failure raises :class:`MessageMappingError` (exit 1).
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from azure.servicebus import ServiceBusMessage
from keboola.component.exceptions import UserException

from configuration import BodyMode, Configuration, MessagePropertyMap

logger = logging.getLogger(__name__)

# message_properties field -> ServiceBusMessage attribute for the plain string mappings.
_SIMPLE_PROPS = {
    "message_id_column": "message_id",
    "session_id_column": "session_id",
    "subject_column": "subject",
    "correlation_id_column": "correlation_id",
    "partition_key_column": "partition_key",
    "reply_to_column": "reply_to",
    "reply_to_session_id_column": "reply_to_session_id",
}


class MessageMappingError(UserException):
    """A row could not be mapped onto a ServiceBusMessage (user-fixable, exit 1)."""


def required_columns(config: Configuration) -> list[str]:
    """Every input column the configuration references (body + property mappings).

    Single source of truth for the up-front header validation in ``component.py``,
    kept next to the mapping logic so a new mapping cannot drift out of the check.
    """
    columns: list[str] = []
    if config.mode == BodyMode.COLUMN_VALUE and config.column:
        columns.append(config.column)
    props = config.message_properties
    for field in _SIMPLE_PROPS:
        column = getattr(props, field)
        if column:
            columns.append(column)
    if props.application_properties_column:
        columns.append(props.application_properties_column)
    if props.scheduled_enqueue_time_column:
        columns.append(props.scheduled_enqueue_time_column)
    return columns


def build_message(row: dict[str, str], config: Configuration) -> ServiceBusMessage:
    """Map a CSV DictReader row onto a ServiceBusMessage per the row configuration."""
    kwargs: dict[str, Any] = {"content_type": config.content_type}

    if config.time_to_live_seconds is not None:
        kwargs["time_to_live"] = timedelta(seconds=config.time_to_live_seconds)

    _apply_property_mappings(kwargs, row, config.message_properties)

    return ServiceBusMessage(_build_body(row, config), **kwargs)


def _build_body(row: dict[str, str], config: Configuration) -> str:
    if config.mode == BodyMode.COLUMN_VALUE:
        # A ragged row (fewer cells than the header) leaves DictReader filling the
        # trailing column with None; treat a missing cell as an empty value.
        value = row.get(config.column or "") or ""
        try:
            json.loads(value)
        except json.JSONDecodeError:
            return json.dumps({"data": value})
        return value
    return json.dumps(row)


def _apply_property_mappings(kwargs: dict[str, Any], row: dict[str, str], props: MessagePropertyMap) -> None:
    for field, attr in _SIMPLE_PROPS.items():
        column = getattr(props, field)
        if column and column in row:
            kwargs[attr] = row[column]

    if props.application_properties_column and props.application_properties_column in row:
        kwargs["application_properties"] = _parse_application_properties(
            props.application_properties_column, row[props.application_properties_column]
        )

    if props.scheduled_enqueue_time_column and props.scheduled_enqueue_time_column in row:
        kwargs["scheduled_enqueue_time_utc"] = _parse_scheduled_time(
            props.scheduled_enqueue_time_column, row[props.scheduled_enqueue_time_column]
        )


def _parse_application_properties(column: str, raw: str) -> dict[str, Any]:
    # TypeError covers a ragged row where DictReader filled a missing trailing cell
    # with None, which json.loads(None) would otherwise raise uncaught (exit 2).
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        raise MessageMappingError(f"Invalid JSON in application_properties column '{column}': {e}") from e
    if not isinstance(parsed, dict):
        raise MessageMappingError(f"The application_properties column '{column}' must contain a JSON object.")
    return parsed


def _parse_scheduled_time(column: str, raw: str) -> datetime:
    # TypeError covers a ragged row where DictReader filled a missing trailing cell
    # with None, which datetime.fromisoformat(None) would otherwise raise uncaught.
    try:
        parsed = datetime.fromisoformat(raw)
    except (ValueError, TypeError) as e:
        raise MessageMappingError(
            f"Invalid ISO-8601 timestamp in scheduled_enqueue_time column '{column}': '{raw}'."
        ) from e
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
