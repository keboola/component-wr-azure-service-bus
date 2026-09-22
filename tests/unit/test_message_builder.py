import json
from datetime import UTC, datetime

import pytest
from keboola.component.exceptions import UserException

from configuration import Configuration
from message_builder import build_message

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


def test_column_value_json_decoded():
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": '{"k": 1}'}, cfg)
    assert json.loads(str(msg)) == {"k": 1}


def test_column_value_wraps_non_json():
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": "raw text"}, cfg)
    assert json.loads(str(msg)) == {"data": "raw text"}


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


def test_ttl_set_as_timedelta():
    cfg = Configuration(**{**BASE, "time_to_live_seconds": 60})
    msg = build_message({"a": "1"}, cfg)
    ttl = msg.time_to_live
    assert ttl is not None
    assert ttl.total_seconds() == 60


def test_scheduled_enqueue_time_parsed_to_utc():
    cfg = Configuration(**{**BASE, "message_properties": {"scheduled_enqueue_time_column": "when"}})
    msg = build_message({"when": "2026-01-01T00:00:00+00:00"}, cfg)
    assert msg.scheduled_enqueue_time_utc == datetime(2026, 1, 1, tzinfo=UTC)


def test_bad_scheduled_enqueue_time_raises():
    cfg = Configuration(**{**BASE, "message_properties": {"scheduled_enqueue_time_column": "when"}})
    with pytest.raises(UserException):
        build_message({"when": "not-a-timestamp"}, cfg)
