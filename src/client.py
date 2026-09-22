"""Credential factory and Service Bus client builder.

This module is the pluggable auth seam. ``build_service_bus_client`` dispatches
on ``Configuration.auth_type`` to a concrete builder; a new auth method slots in
by adding a builder and registering it in ``_BUILDERS``. The transport is fixed
to pyamqp -- ``uamqp_transport`` is never set and no transport field is exposed.
"""

import logging
import re
from collections.abc import Callable

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.servicebus import ServiceBusClient
from keboola.component.exceptions import UserException

from configuration import AuthType, Configuration

logger = logging.getLogger(__name__)

_SAS_KEY_RE = re.compile(r"(SharedAccessKey=)[^;]+", re.IGNORECASE)


def redact_secrets(text: str) -> str:
    """Mask the SAS key so it never reaches a log or a surfaced error message."""
    return _SAS_KEY_RE.sub(r"\1***", text)


def _build_from_connection_string(config: Configuration) -> ServiceBusClient:
    try:
        return ServiceBusClient.from_connection_string(conn_str=config.connection_string)
    except ValueError as e:
        raise UserException(f"Invalid connection string: {redact_secrets(str(e))}")


def _build_from_service_principal(config: Configuration) -> ServiceBusClient:
    credential = ClientSecretCredential(config.tenant_id, config.client_id, config.client_secret)
    return ServiceBusClient(fully_qualified_namespace=config.fully_qualified_namespace, credential=credential)


def _build_from_managed_identity(config: Configuration) -> ServiceBusClient:
    return ServiceBusClient(
        fully_qualified_namespace=config.fully_qualified_namespace,
        credential=DefaultAzureCredential(),
    )


_BUILDERS: dict[AuthType, Callable[[Configuration], ServiceBusClient]] = {
    AuthType.CONNECTION_STRING: _build_from_connection_string,
    AuthType.SERVICE_PRINCIPAL: _build_from_service_principal,
    AuthType.MANAGED_IDENTITY: _build_from_managed_identity,
}


def build_service_bus_client(config: Configuration) -> ServiceBusClient:
    """Build a ServiceBusClient for the configured auth method (pyamqp; uamqp_transport never set)."""
    builder = _BUILDERS.get(config.auth_type)
    if builder is None:
        raise UserException(f"Unsupported auth_type: {config.auth_type}")
    return builder(config)
