"""Credential factory and Service Bus client builder.

This module is the pluggable auth seam. ``build_service_bus_client`` dispatches
on ``Configuration.auth_type`` to a concrete builder; a new auth method slots in
by adding a builder and registering it in ``_BUILDERS``. The transport is fixed
to pyamqp -- ``uamqp_transport`` is never set and no transport field is exposed.
"""

import logging
import re
from collections.abc import Callable

from azure.identity import ClientSecretCredential
from azure.servicebus import ServiceBusClient
from azure.servicebus.exceptions import (
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusCommunicationError,
    ServiceBusConnectionError,
    ServiceBusError,
)
from keboola.component.exceptions import UserException

from configuration import AuthType, Configuration

logger = logging.getLogger(__name__)

_SAS_KEY_RE = re.compile(r"(SharedAccessKey=)[^;]+", re.IGNORECASE)


def redact_secrets(text: str) -> str:
    """Mask the SAS key so it never reaches a log or a surfaced error message."""
    return _SAS_KEY_RE.sub(r"\1***", text)


def to_user_exception(error: ServiceBusError, entity_name: str | None = None) -> UserException:
    """Translate an Azure Service Bus error into an actionable, secret-free UserException (G3)."""
    detail = redact_secrets(str(error))
    target = f" '{entity_name}'" if entity_name else ""
    if isinstance(error, MessagingEntityNotFoundError):
        message = (
            f"The target topic or queue{target} was not found in the namespace. "
            "Check the entity name and that it exists."
        )
    elif isinstance(error, ServiceBusAuthenticationError | ServiceBusAuthorizationError):
        message = (
            "Authentication or authorization to Azure Service Bus failed. "
            "Check the credentials and that they grant Send rights on the entity."
        )
    elif isinstance(error, ServiceBusConnectionError | ServiceBusCommunicationError):
        message = "Could not connect to the Service Bus namespace. Check the namespace host name and network access."
    else:
        message = "Azure Service Bus reported an error."
    return UserException(f"{message} (details: {detail})")


def _build_from_connection_string(config: Configuration) -> ServiceBusClient:
    try:
        return ServiceBusClient.from_connection_string(conn_str=config.connection_string)
    except ValueError as e:
        raise UserException(f"Invalid connection string: {redact_secrets(str(e))}") from e


def _build_from_service_principal(config: Configuration) -> ServiceBusClient:
    try:
        credential = ClientSecretCredential(config.tenant_id, config.client_id, config.client_secret)
        return ServiceBusClient(fully_qualified_namespace=config.fully_qualified_namespace, credential=credential)
    except ValueError as e:
        raise UserException(f"Invalid service principal credentials: {redact_secrets(str(e))}") from e


_BUILDERS: dict[AuthType, Callable[[Configuration], ServiceBusClient]] = {
    AuthType.CONNECTION_STRING: _build_from_connection_string,
    AuthType.SERVICE_PRINCIPAL: _build_from_service_principal,
}


def build_service_bus_client(config: Configuration) -> ServiceBusClient:
    """Build a ServiceBusClient for the configured auth method (pyamqp; uamqp_transport never set)."""
    builder = _BUILDERS.get(config.auth_type)
    if builder is None:
        raise UserException(f"Unsupported auth_type: {config.auth_type}")
    return builder(config)
