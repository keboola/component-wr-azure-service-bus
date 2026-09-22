from unittest import mock

import pytest
from azure.servicebus.exceptions import (
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusConnectionError,
    ServiceBusError,
)
from keboola.component.exceptions import UserException

import client as client_mod
from configuration import Configuration

BASE = {
    "auth_type": "connection_string",
    "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=SECRET",
    "destination_type": "queue",
    "entity_name": "q1",
}


def test_connection_string_path():
    with mock.patch.object(client_mod, "ServiceBusClient") as sb:
        client_mod.build_service_bus_client(Configuration(**BASE))
        sb.from_connection_string.assert_called_once()
        assert "uamqp_transport" not in sb.from_connection_string.call_args.kwargs


def test_service_principal_path():
    cfg = Configuration(
        auth_type="service_principal",
        tenant_id="t",
        client_id="c",
        **{"#client_secret": "sec"},
        fully_qualified_namespace="ns.servicebus.windows.net",
        destination_type="queue",
        entity_name="q1",
    )
    with (
        mock.patch.object(client_mod, "ServiceBusClient") as sb,
        mock.patch.object(client_mod, "ClientSecretCredential") as cred,
    ):
        client_mod.build_service_bus_client(cfg)
        cred.assert_called_once_with("t", "c", "sec")
        sb.assert_called_once()
        assert "uamqp_transport" not in sb.call_args.kwargs


def test_bad_connection_string_redacts_key():
    cfg = Configuration(**BASE)
    with mock.patch.object(client_mod, "ServiceBusClient") as sb:
        sb.from_connection_string.side_effect = ValueError("malformed Endpoint=sb://x/;SharedAccessKey=SECRET")
        with pytest.raises(UserException) as exc:
            client_mod.build_service_bus_client(cfg)
    assert "SECRET" not in str(exc.value)
    assert "SharedAccessKey=***" in str(exc.value)


def test_to_user_exception_entity_not_found_names_entity():
    exc = client_mod.to_user_exception(MessagingEntityNotFoundError(message="x"), "my-queue")
    assert "my-queue" in str(exc)
    assert "not found" in str(exc).lower()


def test_to_user_exception_auth_failure():
    exc = client_mod.to_user_exception(ServiceBusAuthenticationError(message="x"))
    text = str(exc).lower()
    assert "authentication" in text or "authorization" in text


def test_to_user_exception_connection_failure():
    exc = client_mod.to_user_exception(ServiceBusConnectionError(message="x"))
    assert "connect" in str(exc).lower()


def test_to_user_exception_redacts_sas_key():
    exc = client_mod.to_user_exception(ServiceBusError(message="boom Endpoint=sb://x/;SharedAccessKey=SEKRIT"))
    assert "SEKRIT" not in str(exc)
    assert "SharedAccessKey=***" in str(exc)
