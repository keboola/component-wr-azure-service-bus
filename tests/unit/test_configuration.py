import pytest
from keboola.component.exceptions import UserException

from configuration import BodyMode, Configuration, DestinationType

BASE = {
    "auth_type": "connection_string",
    "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=s",
    "destination_type": "queue",
    "entity_name": "q1",
}


def test_defaults():
    c = Configuration(**BASE)
    assert c.mode is BodyMode.ROW_AS_JSON
    assert c.content_type == "application/json"
    assert c.batch_size == 1000
    assert c.destination_type is DestinationType.QUEUE


def test_column_value_requires_column():
    with pytest.raises(UserException):
        Configuration(**{**BASE, "mode": "column_value"})  # no `column`


def test_column_ignored_without_column_value():
    # I8: a leftover `column` with a non-column_value mode is tolerated (nulled), not rejected.
    c = Configuration(**{**BASE, "column": "payload"})  # mode defaults row_as_json
    assert c.column is None


def test_service_principal_requires_all_fields():
    with pytest.raises(UserException):
        Configuration(
            auth_type="service_principal",
            destination_type="queue",
            entity_name="q1",
            tenant_id="t",
            client_id="c",
        )  # missing #client_secret + namespace


def test_connection_string_auth_requires_secret():
    with pytest.raises(UserException):
        Configuration(auth_type="connection_string", destination_type="queue", entity_name="q1")


def test_managed_identity_is_rejected():
    # Managed identity was removed (maintainer decision 2026-09-22): it is no longer a valid auth_type.
    with pytest.raises(UserException):
        Configuration(
            auth_type="managed_identity",
            destination_type="queue",
            entity_name="q1",
            fully_qualified_namespace="ns.servicebus.windows.net",
        )


def test_batch_size_min():
    with pytest.raises(UserException):
        Configuration(**{**BASE, "batch_size": 0})


def test_message_properties_parsed():
    c = Configuration(**{**BASE, "message_properties": {"session_id_column": "sid", "message_id_column": "mid"}})
    assert c.message_properties.session_id_column == "sid"
    assert c.message_properties.message_id_column == "mid"
    assert c.message_properties.subject_column is None
