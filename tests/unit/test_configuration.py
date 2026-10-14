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


ROW_KEY = "Endpoint=sb://x/;SharedAccessKeyName=rk;SharedAccessKey=ROWSECRET;EntityPath=q1"
ROW_ONLY = {"destination_type": "queue", "entity_name": "q1"}


def test_row_connection_string_defaults_empty():
    assert Configuration(**BASE).row_connection_string == ""


def test_row_connection_string_alone_satisfies_auth():
    c = Configuration(**ROW_ONLY, **{"#row_connection_string": ROW_KEY})
    assert c.auth_type is None
    assert c.row_connection_string == ROW_KEY


def test_row_connection_string_with_incomplete_service_principal_root():
    c = Configuration(
        auth_type="service_principal",
        tenant_id="t",
        **ROW_ONLY,
        **{"#row_connection_string": ROW_KEY},
    )
    assert c.row_connection_string == ROW_KEY


def test_row_connection_string_with_empty_root_connection_string():
    c = Configuration(auth_type="connection_string", **ROW_ONLY, **{"#row_connection_string": ROW_KEY})
    assert c.connection_string == ""


def test_row_connection_string_is_stripped_and_none_is_empty():
    assert Configuration(**BASE, **{"#row_connection_string": f"  {ROW_KEY}\n"}).row_connection_string == ROW_KEY
    assert Configuration(**BASE, **{"#row_connection_string": None}).row_connection_string == ""
    assert Configuration(**BASE, **{"#row_connection_string": "   "}).row_connection_string == ""


def test_blank_row_connection_string_does_not_replace_missing_root_auth():
    with pytest.raises(UserException, match="No credentials configured"):
        Configuration(**ROW_ONLY, **{"#row_connection_string": "  "})


def test_neither_root_auth_nor_row_key_is_a_clear_error():
    with pytest.raises(UserException) as exc:
        Configuration(**ROW_ONLY)
    assert "#row_connection_string" in str(exc.value)
    assert "auth_type" in str(exc.value)


def test_missing_root_connection_string_error_mentions_row_key():
    with pytest.raises(UserException) as exc:
        Configuration(auth_type="connection_string", **ROW_ONLY)
    assert "#connection_string" in str(exc.value)
    assert "#row_connection_string" in str(exc.value)


def test_invalid_auth_type_still_rejected_with_row_key():
    with pytest.raises(UserException):
        Configuration(auth_type="bogus", **ROW_ONLY, **{"#row_connection_string": ROW_KEY})


@pytest.mark.parametrize(
    "params",
    [
        {"#row_connection_string": ROW_KEY},
        {"auth_type": "connection_string", "#connection_string": "Endpoint=sb://x/"},
        {"auth_type": "service_principal", "tenant_id": "t"},
        {"auth_type": "service_principal", "#client_secret": "s"},
    ],
)
def test_has_credentials_true_when_any_credential_is_set(params):
    assert Configuration.has_credentials(params) is True


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"auth_type": "connection_string"},
        {"auth_type": "connection_string", "#connection_string": "", "#row_connection_string": "   "},
        {"auth_type": "service_principal", "tenant_id": "", "client_id": None, **ROW_ONLY},
    ],
)
def test_has_credentials_false_without_any_credential(params):
    assert Configuration.has_credentials(params) is False
