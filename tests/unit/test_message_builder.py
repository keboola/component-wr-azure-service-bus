import json
from datetime import UTC, datetime
from typing import cast

import pytest
from keboola.component.exceptions import UserException

from configuration import Configuration
from message_builder import build_message, required_columns

BASE = {
    "auth_type": "connection_string",
    "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=s",
    "destination_type": "queue",
    "entity_name": "q1",
}


def test_row_as_json_body():
    msg = build_message({"a": "1", "b": "x"}, Configuration(**BASE))
    assert json.loads(str(msg)) == {"a": "1", "b": "x"}
    assert msg.content_type == "application/json"


def test_row_as_json_keeps_non_ascii_raw():
    # ensure_ascii=False -> accented text stays raw UTF-8 in the body, not \uXXXX escapes.
    msg = build_message({"name": "Košík"}, Configuration(**BASE))
    body = str(msg)
    assert "Košík" in body
    assert "\\u" not in body
    assert json.loads(body) == {"name": "Košík"}


def test_column_value_json_string_sent_unchanged():
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": '{"k": 1}'}, cfg)
    assert str(msg) == '{"k": 1}'


def test_column_value_non_json_scalar_sent_raw():
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": "raw text"}, cfg)
    assert str(msg) == "raw text"


def test_column_value_empty_string_sent_raw():
    # An empty cell is not parsed or coerced; it goes out as an empty body untouched.
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": ""}, cfg)
    assert str(msg) == ""


def test_property_mappings():
    cfg = Configuration(**{**BASE, "message_properties": {"session_id_column": "sid", "message_id_column": "mid"}})
    msg = build_message({"a": "1", "sid": "s1", "mid": "m1"}, cfg)
    assert msg.session_id == "s1"
    assert msg.message_id == "m1"


def test_all_simple_property_mappings():
    cfg = Configuration(
        **{
            **BASE,
            "message_properties": {
                "subject_column": "subj",
                "correlation_id_column": "corr",
                "partition_key_column": "pk",
                "reply_to_column": "rt",
                "reply_to_session_id_column": "rtsid",
            },
        }
    )
    msg = build_message({"subj": "s", "corr": "c", "pk": "p", "rt": "r", "rtsid": "rs"}, cfg)
    assert msg.subject == "s"
    assert msg.correlation_id == "c"
    assert msg.partition_key == "p"
    assert msg.reply_to == "r"
    assert msg.reply_to_session_id == "rs"


def test_missing_mapped_column_is_skipped():
    cfg = Configuration(**{**BASE, "message_properties": {"session_id_column": "sid"}})
    msg = build_message({"a": "1"}, cfg)  # 'sid' column absent from the row
    assert msg.session_id is None


def test_application_properties_parsed():
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    msg = build_message({"props": '{"k": "v", "n": 3}'}, cfg)
    assert msg.application_properties == {"k": "v", "n": 3}


def test_bad_application_properties_json_raises():
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    with pytest.raises(UserException):
        build_message({"a": "1", "props": "{not json"}, cfg)


def test_application_properties_non_object_raises():
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    with pytest.raises(UserException):
        build_message({"props": "[1, 2, 3]"}, cfg)


def test_application_properties_nested_value_raises():
    # Service Bus application properties accept only scalar values; a nested object/array
    # must be rejected up front with a clear error, not deep inside the SDK at send time.
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    with pytest.raises(UserException):
        build_message({"props": '{"meta": {"nested": 1}}'}, cfg)


def test_row_as_json_content_type_override_ignored():
    # row_as_json always serializes with json.dumps(), so content_type is forced to
    # application/json even when config sets a stale/custom value.
    cfg = Configuration(**{**BASE, "content_type": "text/plain"})
    msg = build_message({"a": "1"}, cfg)
    assert msg.content_type == "application/json"


def test_column_value_custom_content_type_applied():
    # content_type only genuinely varies in column_value mode.
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload", "content_type": "text/plain"})
    msg = build_message({"payload": "raw text"}, cfg)
    assert msg.content_type == "text/plain"


def test_scheduled_enqueue_time_parsed_to_utc():
    cfg = Configuration(**{**BASE, "message_properties": {"scheduled_enqueue_time_column": "when"}})
    msg = build_message({"when": "2026-01-01T00:00:00+00:00"}, cfg)
    assert msg.scheduled_enqueue_time_utc == datetime(2026, 1, 1, tzinfo=UTC)


def test_bad_scheduled_enqueue_time_raises():
    cfg = Configuration(**{**BASE, "message_properties": {"scheduled_enqueue_time_column": "when"}})
    with pytest.raises(UserException):
        build_message({"when": "not-a-timestamp"}, cfg)


# --- I2: ragged rows (DictReader fills missing trailing cells with None) --------


def test_ragged_row_application_properties_none_raises():
    # A None cell must map to an exit-1 UserException, not an uncaught TypeError (exit 2).
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    with pytest.raises(UserException):
        build_message(cast("dict[str, str]", {"a": "1", "props": None}), cfg)


def test_ragged_row_scheduled_time_none_raises():
    cfg = Configuration(**{**BASE, "message_properties": {"scheduled_enqueue_time_column": "when"}})
    with pytest.raises(UserException):
        build_message(cast("dict[str, str]", {"when": None}), cfg)


def test_ragged_row_column_value_none_is_empty():
    # column_value body with a None cell must not crash; a missing cell is an empty value.
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message(cast("dict[str, str]", {"payload": None}), cfg)
    assert str(msg) == ""


# --- Blank cell in a mapped property column -> skip that property for the row ----


def test_blank_simple_property_cell_is_skipped():
    cfg = Configuration(**{**BASE, "message_properties": {"subject_column": "subj"}})
    msg = build_message({"subj": ""}, cfg)
    assert msg.subject is None  # blank cell -> property left unset, not set to ""


def test_blank_application_properties_cell_is_skipped():
    # A heterogeneous table where only some rows carry properties must not fail on the
    # blank rows: a blank application_properties cell skips the property (no JSON parse).
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    msg = build_message({"a": "1", "props": ""}, cfg)
    assert msg.application_properties is None


def test_blank_scheduled_enqueue_time_cell_is_skipped():
    cfg = Configuration(**{**BASE, "message_properties": {"scheduled_enqueue_time_column": "when"}})
    msg = build_message({"a": "1", "when": ""}, cfg)
    assert msg.scheduled_enqueue_time_utc is None


# --- B3: required_columns collects every referenced input column ----------------


def test_required_columns_collects_body_and_property_columns():
    cfg = Configuration(
        **{
            **BASE,
            "mode": "column_value",
            "column": "payload",
            "message_properties": {
                "message_id_column": "mid",
                "application_properties_column": "props",
                "scheduled_enqueue_time_column": "when",
            },
        }
    )
    assert set(required_columns(cfg)) == {"payload", "mid", "props", "when"}


def test_required_columns_empty_for_row_as_json_without_properties():
    assert required_columns(Configuration(**BASE)) == []
