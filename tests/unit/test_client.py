from unittest import mock

import pytest
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


def test_managed_identity_path():
    cfg = Configuration(
        auth_type="managed_identity",
        fully_qualified_namespace="ns.servicebus.windows.net",
        destination_type="queue",
        entity_name="q1",
    )
    with (
        mock.patch.object(client_mod, "ServiceBusClient") as sb,
        mock.patch.object(client_mod, "DefaultAzureCredential") as cred,
    ):
        client_mod.build_service_bus_client(cfg)
        cred.assert_called_once_with()
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
