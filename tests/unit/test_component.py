import csv
import json
import os
from unittest import mock

import pytest
from azure.servicebus.exceptions import ServiceBusConnectionError
from keboola.component.exceptions import UserException

import component as component_mod
from component import Component

BASE_PARAMS = {
    "auth_type": "connection_string",
    "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=s",
    "destination_type": "queue",
    "entity_name": "q1",
}


class FakeBatch:
    def __init__(self):
        self.msgs = []

    def add_message(self, m):
        self.msgs.append(m)


class FakeSender:
    def __init__(self, batch_error=None):
        self.sent = []
        self.closed = False
        self.batch_error = batch_error

    def create_message_batch(self):
        if self.batch_error is not None:
            raise self.batch_error
        return FakeBatch()

    def send_messages(self, x):
        if isinstance(x, FakeBatch):
            self.sent.extend(x.msgs)
        else:
            self.sent.append(x)

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class FakeClient:
    def __init__(self, sender=None):
        self.sender = sender or FakeSender()
        self.opened_queue = None
        self.opened_topic = None

    def get_queue_sender(self, queue_name):
        self.opened_queue = queue_name
        return self.sender

    def get_topic_sender(self, topic_name):
        self.opened_topic = topic_name
        return self.sender

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _write_datadir(tmp_path, params, rows, headers=("a", "b"), with_table=True, action=None):
    (tmp_path / "in" / "tables").mkdir(parents=True)
    config = {"parameters": params}
    if action is not None:
        config["action"] = action
    (tmp_path / "config.json").write_text(json.dumps(config))
    if with_table:
        with open(tmp_path / "in" / "tables" / "input.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(headers))
            writer.writeheader()
            writer.writerows(rows)


def _run(tmp_path, params, rows, with_table=True):
    _write_datadir(tmp_path, params, rows, with_table=with_table)
    fake_client = FakeClient()
    with (
        mock.patch.dict(os.environ, {"KBC_DATADIR": str(tmp_path)}),
        mock.patch.object(component_mod, "build_service_bus_client", return_value=fake_client),
    ):
        Component().run()
    return fake_client


def test_run_sends_all_rows_to_queue(tmp_path):
    client = _run(tmp_path, BASE_PARAMS, [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}])
    assert len(client.sender.sent) == 2
    assert client.opened_queue == "q1"
    assert client.opened_topic is None
    assert client.sender.closed is True


def test_run_opens_topic_sender(tmp_path):
    params = {**BASE_PARAMS, "destination_type": "topic", "entity_name": "t1"}
    client = _run(tmp_path, params, [{"a": "1", "b": "x"}])
    assert client.opened_topic == "t1"
    assert client.opened_queue is None
    assert len(client.sender.sent) == 1


def test_run_empty_input_sends_nothing(tmp_path):
    client = _run(tmp_path, BASE_PARAMS, [])  # header only, zero data rows
    assert client.sender.sent == []
    assert client.sender.closed is True


def test_run_missing_input_table_raises(tmp_path):
    with pytest.raises(UserException):
        _run(tmp_path, BASE_PARAMS, [], with_table=False)


def test_test_connection_opens_probes_and_closes(tmp_path):
    _write_datadir(tmp_path, BASE_PARAMS, [], with_table=False, action="testConnection")
    fake_client = FakeClient()
    with (
        mock.patch.dict(os.environ, {"KBC_DATADIR": str(tmp_path)}),
        mock.patch.object(component_mod, "build_service_bus_client", return_value=fake_client),
    ):
        result = Component().test_connection()
    assert fake_client.opened_queue == "q1"
    assert fake_client.sender.closed is True  # closed via the sender context manager
    assert "succeeded" in result.message.lower()


def test_test_connection_failure_exits_with_redacted_message(tmp_path, capsys):
    _write_datadir(tmp_path, BASE_PARAMS, [], with_table=False, action="testConnection")
    # The forced link-open probe (create_message_batch) fails with a broker error carrying a SAS key.
    error = ServiceBusConnectionError(message="down Endpoint=sb://x/;SharedAccessKey=SECRETKEY")
    failing = FakeClient(sender=FakeSender(batch_error=error))
    with (
        mock.patch.dict(os.environ, {"KBC_DATADIR": str(tmp_path)}),
        mock.patch.object(component_mod, "build_service_bus_client", return_value=failing),
        pytest.raises(SystemExit),  # sync-action failure -> exit(1)
    ):
        Component().test_connection()
    err = capsys.readouterr().err
    assert "SECRETKEY" not in err
    assert "connect" in err.lower()
