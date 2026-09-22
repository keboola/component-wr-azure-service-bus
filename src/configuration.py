"""Typed configuration for the Azure Service Bus writer.

Keboola merges the config-root (auth) parameters and the config-row (destination
mapping) parameters into a single flat ``parameters`` dict, so ``Configuration``
is a flat model. Each auth field is optional at the schema level and required
only for its ``auth_type`` -- enforced by :meth:`Configuration._validate_auth`.
The pluggable credential factory lives in ``client.py``; this model only carries
the fields and validates them.
"""

import logging
from enum import StrEnum
from typing import Any

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

logger = logging.getLogger(__name__)


class AuthType(StrEnum):
    CONNECTION_STRING = "connection_string"
    SERVICE_PRINCIPAL = "service_principal"


class DestinationType(StrEnum):
    TOPIC = "topic"
    QUEUE = "queue"


class BodyMode(StrEnum):
    ROW_AS_JSON = "row_as_json"
    COLUMN_VALUE = "column_value"


class MessagePropertyMap(BaseModel):
    """Optional row-column -> ServiceBusMessage attribute mappings (each column name is free-text)."""

    model_config = ConfigDict(populate_by_name=True)

    message_id_column: str | None = None
    session_id_column: str | None = None
    subject_column: str | None = None
    correlation_id_column: str | None = None
    partition_key_column: str | None = None
    scheduled_enqueue_time_column: str | None = None
    application_properties_column: str | None = None
    reply_to_column: str | None = None
    reply_to_session_id_column: str | None = None


class Configuration(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    auth_type: AuthType

    # --- Auth block (config root). Required-per-auth_type, see _validate_auth. ---
    connection_string: str = Field(alias="#connection_string", default="")
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = Field(alias="#client_secret", default="")
    fully_qualified_namespace: str = ""

    # --- Destination mapping (config row). ---
    destination_type: DestinationType
    entity_name: str
    mode: BodyMode = BodyMode.ROW_AS_JSON
    column: str | None = None
    content_type: str = "application/json"
    batch_size: int = Field(default=1000, ge=1)
    time_to_live_seconds: int | None = None
    message_properties: MessagePropertyMap = Field(default_factory=MessagePropertyMap)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            messages = []
            for err in e.errors():
                loc = ".".join(str(part) for part in err["loc"]) if err["loc"] else "configuration"
                messages.append(f"{loc}: {err['msg']}")
            raise UserException(f"Validation Error: {', '.join(messages)}") from e

    @model_validator(mode="after")
    def _validate_body_mode(self):
        if self.mode == BodyMode.COLUMN_VALUE and not self.column:
            raise ValueError("`column` is required when `mode` is `column_value`.")
        if self.mode != BodyMode.COLUMN_VALUE and self.column:
            raise ValueError("`column` may only be set when `mode` is `column_value`.")
        return self

    @model_validator(mode="after")
    def _validate_auth(self):
        if self.auth_type == AuthType.CONNECTION_STRING and not self.connection_string:
            raise ValueError("`#connection_string` is required for connection_string auth.")
        if self.auth_type == AuthType.SERVICE_PRINCIPAL:
            required = {
                "tenant_id": self.tenant_id,
                "client_id": self.client_id,
                "#client_secret": self.client_secret,
                "fully_qualified_namespace": self.fully_qualified_namespace,
            }
            missing = [name for name, value in required.items() if not value]
            if missing:
                raise ValueError(f"Service principal auth requires: {', '.join(missing)}.")
        return self
