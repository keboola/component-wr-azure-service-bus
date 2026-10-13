import pytest
from keboola.component.exceptions import UserException

from configuration import BodyMode, Configuration, DestinationType, JsonValueType, ValueTyping

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


def test_value_typing_defaults_are_backward_compatible():
    c = Configuration(**BASE)
    assert c.value_typing is ValueTyping.STRING
    assert c.column_types == []
    assert c.empty_as_null is False


def test_typed_config_parses_overrides():
    c = Configuration(
        **BASE,
        value_typing="typed",
        empty_as_null=True,
        column_types=[{"column": "a", "type": "integer"}, {"column": "b", "type": "string"}],
    )
    assert c.value_typing is ValueTyping.TYPED
    assert c.empty_as_null is True
    assert [(o.column, o.type) for o in c.column_types] == [("a", JsonValueType.INTEGER), ("b", JsonValueType.STRING)]


def test_unknown_override_type_is_rejected():
    with pytest.raises(UserException):
        Configuration(**BASE, value_typing="typed", column_types=[{"column": "a", "type": "date"}])


def test_unknown_value_typing_is_rejected():
    with pytest.raises(UserException):
        Configuration(**BASE, value_typing="auto")


def test_duplicate_override_column_is_rejected():
    with pytest.raises(UserException, match="more than once"):
        Configuration(
            **BASE,
            value_typing="typed",
            column_types=[{"column": "a", "type": "integer"}, {"column": "a", "type": "number"}],
        )


def test_overrides_dropped_when_value_typing_is_string():
    c = Configuration(**BASE, column_types=[{"column": "a", "type": "integer"}])
    assert c.column_types == []


def test_typing_options_ignored_outside_row_as_json():
    c = Configuration(
        **BASE,
        mode="column_value",
        column="payload",
        value_typing="typed",
        empty_as_null=True,
        column_types=[{"column": "a", "type": "integer"}],
    )
    assert c.value_typing is ValueTyping.STRING
    assert c.empty_as_null is False
    assert c.column_types == []


def test_blank_override_column_is_rejected():
    with pytest.raises(UserException):
        Configuration(**BASE, value_typing="typed", column_types=[{"column": "", "type": "integer"}])
