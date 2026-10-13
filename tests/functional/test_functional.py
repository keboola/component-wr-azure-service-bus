"""Functional (datadir) tests for keboola.wr-azure-service-bus.

Each case builds a real KBC_DATADIR (config.json + input tables copied from
``tests/setup/input_files/``, per the mappings in ``tests/setup/configs.json``)
and runs the component end-to-end via ``runpy.run_path(..., run_name="__main__")``
-- exercising the real ``__main__`` exit-code guard -- against the mocked Azure
SDK provided by the autouse ``mock_service_bus`` fixture (see conftest.py). No
network is touched and no cassettes are recorded (the send path is AMQP, not
HTTP, so vcrpy cannot record it).
"""

import json
import shutil
from pathlib import Path
from runpy import run_path

import pytest

TESTS_DIR = Path(__file__).resolve().parent.parent
SETUP_DIR = TESTS_DIR / "setup"
INPUT_FILES_DIR = SETUP_DIR / "input_files"
COMPONENT_SCRIPT = str(TESTS_DIR.parent / "src" / "component.py")

_CONFIGS = {case["name"]: case["config"] for case in json.loads((SETUP_DIR / "configs.json").read_text())}


def _build_datadir(case_name: str, tmp_path: Path) -> Path:
    """Materialize a KBC data folder for a case from configs.json + input_files/."""
    config = _CONFIGS[case_name]
    data_dir = tmp_path / "data"
    (data_dir / "in" / "tables").mkdir(parents=True)
    (data_dir / "out" / "tables").mkdir(parents=True)
    (data_dir / "out" / "files").mkdir(parents=True)
    (data_dir / "config.json").write_text(json.dumps(config))
    for table in config.get("storage", {}).get("input", {}).get("tables", []):
        dest = table["destination"]
        shutil.copy(INPUT_FILES_DIR / dest, data_dir / "in" / "tables" / dest)
        manifest = INPUT_FILES_DIR / f"{dest}.manifest"
        if manifest.exists():
            shutil.copy(manifest, data_dir / "in" / "tables" / manifest.name)
    return data_dir


def run_case(case_name: str, tmp_path: Path, monkeypatch) -> None:
    """Run the component end-to-end for a case against the mocked SDK."""
    data_dir = _build_datadir(case_name, tmp_path)
    monkeypatch.setenv("KBC_DATADIR", str(data_dir))
    run_path(COMPONENT_SCRIPT, run_name="__main__")


def _bodies(capture) -> list:
    return [str(m) for m in capture.messages]


# --- Sync action: testConnection (success + failure) ---------------------------


def test_01_test_connection_success(mock_service_bus, tmp_path, monkeypatch):
    run_case("01_testConnection", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert cap.opened_queue == "my-queue"
    assert cap.opened_topic is None
    assert cap.batches_created >= 1  # create_message_batch() connectivity probe
    assert cap.closed is True


def test_02_test_connection_bad_conn_string(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("02_testConnection_bad_conn_string", tmp_path, monkeypatch)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "SharedAccessKey=" not in err
    assert "connection string" in err.lower()


# --- Run: destination types + body modes ---------------------------------------


def test_03_run_queue_row_as_json(mock_service_bus, tmp_path, monkeypatch):
    run_case("03_run_queue_row_as_json", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert cap.opened_queue == "orders-queue"
    assert cap.opened_topic is None
    assert len(cap.messages) == 3
    assert cap.closed is True
    assert json.loads(_bodies(cap)[0]) == {"id": "1", "name": "alpha", "amount": "10"}


def test_04_run_topic_row_as_json(mock_service_bus, tmp_path, monkeypatch):
    run_case("04_run_topic_row_as_json", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert cap.opened_topic == "events-topic"
    assert cap.opened_queue is None
    assert len(cap.messages) == 3


def test_05_run_column_value(mock_service_bus, tmp_path, monkeypatch):
    run_case("05_run_column_value", tmp_path, monkeypatch)
    cap = mock_service_bus
    bodies = _bodies(cap)
    assert len(bodies) == 2
    assert bodies[0] == '{"k": 1}'  # JSON-looking cell sent unchanged, not re-parsed
    assert bodies[1] == "raw text"  # non-JSON cell sent raw, no wrapping


# --- Run: message property mappings --------------------------------------------


def test_06_run_message_properties(mock_service_bus, tmp_path, monkeypatch):
    run_case("06_run_message_properties", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert len(cap.messages) == 1
    msg = cap.messages[0]
    assert msg.message_id == "m-1"
    assert msg.session_id == "s-1"
    assert msg.subject == "greetings"
    assert msg.correlation_id == "c-1"
    assert msg.partition_key == "s-1"
    assert dict(msg.application_properties) == {"x": 1, "y": "z"}
    assert msg.scheduled_enqueue_time_utc is not None
    assert msg.scheduled_enqueue_time_utc.year == 2026


# --- Run: batching --------------------------------------------------------------


def test_07_run_batch_flush_by_count(mock_service_bus, tmp_path, monkeypatch):
    run_case("07_run_batch_flush_by_count", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert len(cap.messages) == 5
    assert len(cap.batches) >= 2  # batch_size=2 over 5 rows -> multiple flushes
    assert [len(b) for b in cap.batches] == [2, 2, 1]


# --- Run: failure paths (exit 1, UserException) --------------------------------


def test_08_run_oversized_row(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("08_run_oversized_row", tmp_path, monkeypatch)
    assert exc.value.code == 1
    combined = capsys.readouterr()
    assert "size" in (combined.out + combined.err).lower()


def test_09_run_missing_creds(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("09_run_missing_creds", tmp_path, monkeypatch)
    assert exc.value.code == 1
    combined = capsys.readouterr()
    assert "connection_string" in (combined.out + combined.err).lower()


def test_10_run_missing_entity_name(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("10_run_missing_entity_name", tmp_path, monkeypatch)
    assert exc.value.code == 1
    combined = capsys.readouterr()
    assert "entity_name" in (combined.out + combined.err).lower()


# --- Run: edge case (empty input) ----------------------------------------------


def test_11_run_empty_input(mock_service_bus, tmp_path, monkeypatch):
    run_case("11_run_empty_input", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert cap.opened_queue == "empty-queue"
    assert cap.messages == []
    assert cap.batches == []
    assert cap.closed is True
    # Lazy-batch behavior (commit e871396): create_message_batch() is the first real
    # broker round-trip, so zero input rows must never open the AMQP link.
    assert cap.batches_created == 0


# --- Run: second auth method (service principal) -------------------------------


def test_12_run_service_principal_auth(mock_service_bus, tmp_path, monkeypatch):
    run_case("12_run_service_principal_auth", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert cap.namespace == "example.servicebus.windows.net"
    assert cap.credential is not None
    assert cap.credential.kind == "client_secret"
    assert cap.credential.tenant_id == "00000000-0000-0000-0000-000000000000"
    assert len(cap.messages) == 3
    assert cap.opened_queue == "sp-queue"


# --- Run: bad application_properties JSON --------------------------------------


def test_13_run_bad_properties_json(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("13_run_bad_properties_json", tmp_path, monkeypatch)
    assert exc.value.code == 1  # invalid application_properties JSON -> UserException
    captured = capsys.readouterr()
    combined = (captured.out + captured.err).lower()
    assert "application_properties" in combined or "json" in combined


# --- Run: header validation (B3 -- mistyped column names fail fast) -------------


def test_14_run_missing_body_column(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("14_run_missing_body_column", tmp_path, monkeypatch)
    assert exc.value.code == 1  # column_value body column absent from header -> UserException
    combined = capsys.readouterr()
    assert "does_not_exist" in (combined.out + combined.err)


def test_15_run_missing_property_column(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("15_run_missing_property_column", tmp_path, monkeypatch)
    assert exc.value.code == 1  # message-property column absent from header -> UserException
    combined = capsys.readouterr()
    assert "does_not_exist" in (combined.out + combined.err)


# --- Run: ragged row (I2 -- missing trailing cell must not crash with exit 2) ----


def test_16_run_ragged_row(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("16_run_ragged_row", tmp_path, monkeypatch)
    assert exc.value.code == 1  # None cell -> exit-1 UserException, not exit-2 TypeError
    combined = capsys.readouterr()
    assert "application_properties" in (combined.out + combined.err).lower()


# --- Run: topic + column_value + properties (combined coverage) ----------------


def test_17_run_topic_column_value_with_properties(mock_service_bus, tmp_path, monkeypatch):
    run_case("17_run_topic_column_value_with_properties", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert cap.opened_topic == "alerts-topic"
    assert cap.opened_queue is None
    bodies = _bodies(cap)
    assert bodies == ['{"k": 1}', "raw text"]  # both cells sent raw, unchanged
    assert cap.messages[0].subject == "order-created"
    assert cap.messages[0].session_id == "sess-1"
    assert cap.messages[1].subject == "order-updated"
    assert cap.messages[1].session_id == "sess-2"


# --- Run: mode-aware content_type -----------------------------------------------


def test_18_run_row_as_json_content_type_override_ignored(mock_service_bus, tmp_path, monkeypatch):
    run_case("18_run_row_as_json_content_type_override_ignored", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert len(cap.messages) == 3
    assert all(m.content_type == "application/json" for m in cap.messages)


def test_19_run_column_value_custom_content_type(mock_service_bus, tmp_path, monkeypatch):
    run_case("19_run_column_value_custom_content_type", tmp_path, monkeypatch)
    cap = mock_service_bus
    assert len(cap.messages) == 2
    assert all(m.content_type == "text/plain" for m in cap.messages)


# --- Run: typed JSON values (CFTL-784) --------------------------------------------


def test_20_run_typed_from_table_types(mock_service_bus, tmp_path, monkeypatch):
    run_case("20_run_typed_from_table_types", tmp_path, monkeypatch)
    bodies = _bodies(mock_service_bus)
    assert bodies == [
        (
            '{"id": 1, "price": 1.89, "ratio": 0.5, "active": true, "name": "alpha", '
            '"created": "2026-01-01 10:00:00", "note": null}'
        ),
        (
            '{"id": 2, "price": 12345678901234567890.123456789012345678, "ratio": 0.0000001, "active": false, '
            '"name": "Košík", "created": "2026-01-02 11:30:00", "note": "hello"}'
        ),
        '{"id": null, "price": null, "ratio": null, "active": null, "name": null, "created": null, "note": null}',
    ]
    assert all(m.content_type == "application/json" for m in mock_service_bus.messages)


def test_21_run_typed_legacy_manifest(mock_service_bus, tmp_path, monkeypatch):
    run_case("21_run_typed_legacy_manifest", tmp_path, monkeypatch)
    assert _bodies(mock_service_bus) == ['{"id": 7, "price": 2.50, "active": true, "name": "gamma"}']


def test_22_run_typed_overrides_untyped_table(mock_service_bus, tmp_path, monkeypatch):
    run_case("22_run_typed_overrides_untyped_table", tmp_path, monkeypatch)
    assert _bodies(mock_service_bus) == [
        '{"id": 1, "price": 9.99, "zip": "01234", "flag": true}',
        '{"id": 2, "price": 10, "zip": "98765", "flag": false}',
    ]


def test_23_run_typed_bad_value(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("23_run_typed_bad_value", tmp_path, monkeypatch)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "'abc'" in err
    assert "column 'price'" in err
    assert "row 2" in err


def test_24_run_typed_empty_cell_without_null_flag(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("24_run_typed_empty_cell_without_null_flag", tmp_path, monkeypatch)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "Empty value" in err
    assert "column 'price'" in err
    assert "row 2" in err


def test_25_run_typed_override_missing_column(mock_service_bus, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        run_case("25_run_typed_override_missing_column", tmp_path, monkeypatch)
    assert exc.value.code == 1
    assert "does_not_exist" in capsys.readouterr().err
    assert mock_service_bus.messages == []


def test_26_run_string_typing_default_ignores_manifest(mock_service_bus, tmp_path, monkeypatch):
    run_case("26_run_string_typing_default_ignores_manifest", tmp_path, monkeypatch)
    assert _bodies(mock_service_bus)[0] == (
        '{"id": "1", "price": "1.89", "ratio": "0.5", "active": "true", "name": "alpha", '
        '"created": "2026-01-01 10:00:00", "note": ""}'
    )


def test_27_run_string_typing_empty_as_null(mock_service_bus, tmp_path, monkeypatch):
    run_case("27_run_string_typing_empty_as_null", tmp_path, monkeypatch)
    bodies = _bodies(mock_service_bus)
    assert json.loads(bodies[0])["note"] is None
    assert json.loads(bodies[0])["id"] == "1"
    assert json.loads(bodies[2]) == dict.fromkeys(["id", "price", "ratio", "active", "name", "created", "note"])
