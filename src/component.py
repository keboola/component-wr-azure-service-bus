"""Azure Service Bus writer.

Reads one Storage input table per config row and sends each row as a message to
an Azure Service Bus topic or queue. ``run()`` is a thin orchestrator; the auth,
message-building and batching logic live in ``client.py``, ``message_builder.py``
and ``sender.py``.
"""

import csv
import logging
import sys
from collections.abc import Iterator

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender
from azure.servicebus.exceptions import ServiceBusError
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, ValidationResult

from client import build_service_bus_client, redact_secrets, to_user_exception
from configuration import Configuration, DestinationType
from message_builder import build_message, required_columns
from sender import MessageSender

logger = logging.getLogger(__name__)

# A single input cell can legitimately hold a large JSON payload well within the
# broker's per-message ceiling (Premium allows up to 100 MB). Raise the csv
# field-size limit so such cells parse; the real size enforcement is the sender's
# G2 oversized check. A fixed value is used deliberately -- csv.field_size_limit()
# with sys.maxsize raises OverflowError on some platforms.
csv.field_size_limit(128 * 1024 * 1024)  # 128 MB


class Component(ComponentBase):
    """Reads a Storage input table per config row and sends each row to Azure Service Bus."""

    def __init__(self) -> None:
        super().__init__()

    def run(self) -> None:
        """Send every row of the row's single input table to the configured entity."""
        config = Configuration(**self.configuration.parameters)
        table = self._resolve_input_table()
        self._validate_columns(table, config)
        with build_service_bus_client(config) as client:
            message_sender = MessageSender(self._open_sender(client, config), config.batch_size, config.entity_name)
            try:
                sent = message_sender.send(self._iter_messages(table, config))
            except Exception:
                # Some messages may already be on the broker; a Keboola re-run resends them.
                logger.warning(
                    "Send failed after %d message(s) were delivered; re-running this configuration may resend "
                    "them unless duplicate detection is enabled on the target entity.",
                    message_sender.sent_count,
                )
                raise
            finally:
                message_sender.close()
        logger.info("Sent %d message(s) to %s '%s'.", sent, config.destination_type, config.entity_name)

    @sync_action("testConnection")
    def test_connection(self) -> ValidationResult:
        """Open a sender for the configured auth method and force the AMQP link open as an auth/connectivity probe."""
        config = Configuration(**self.configuration.parameters)
        try:
            with build_service_bus_client(config) as client, self._open_sender(client, config) as sender:
                # Forces the real connection + auth round-trip. The SDK's create_message_batch()
                # exposes no timeout parameter, so this probe cannot be bounded here; the Keboola
                # sync-action/job timeout is the backstop against a stalled link. Do not "fix"
                # this by passing timeout= -- the SDK would raise TypeError.
                sender.create_message_batch()
        except ServiceBusError as e:
            raise to_user_exception(e, config.entity_name) from e
        return ValidationResult("Connection to Azure Service Bus succeeded.", MessageType.SUCCESS)

    def _resolve_input_table(self) -> TableDefinition:
        tables = self.get_input_tables_definitions()
        if len(tables) != 1:
            raise UserException(
                f"Exactly one input table is required in the row's input mapping, but found {len(tables)}."
            )
        return tables[0]

    @staticmethod
    def _validate_columns(table: TableDefinition, config: Configuration) -> None:
        """Fail fast (exit 1) if a configured body/property column is absent from the input header."""
        with open(table.full_path, newline="", encoding="utf-8") as f:
            header = csv.DictReader(f).fieldnames or []
        available = set(header)
        missing = sorted({name for name in required_columns(config) if name not in available})
        if missing:
            raise UserException(
                f"Configured column(s) not found in the input table '{table.name}': {', '.join(missing)}. "
                f"Available columns: {', '.join(header)}."
            )

    @staticmethod
    def _open_sender(client: ServiceBusClient, config: Configuration) -> ServiceBusSender:
        try:
            if config.destination_type == DestinationType.TOPIC:
                return client.get_topic_sender(topic_name=config.entity_name)
            return client.get_queue_sender(queue_name=config.entity_name)
        except ServiceBusError as e:
            raise to_user_exception(e, config.entity_name) from e
        except ValueError as e:
            # get_queue_sender/get_topic_sender raise a bare ValueError when the connection
            # string carries an EntityPath that differs from the configured entity_name.
            raise UserException(
                f"The connection string's entity (EntityPath) does not match the configured "
                f"entity_name '{config.entity_name}'. Use a namespace-level connection string "
                f"(no EntityPath), or set entity_name to the entity named in the connection string. "
                f"(details: {redact_secrets(str(e))})"
            ) from e

    @staticmethod
    def _iter_messages(table: TableDefinition, config: Configuration) -> Iterator[ServiceBusMessage]:
        with open(table.full_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            try:
                for row in reader:
                    yield build_message(row, config)
            except csv.Error as e:
                raise UserException(
                    f"Failed to parse the input table '{table.name}': {e}. A cell may exceed the CSV field size limit."
                ) from e


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as e:
        logger.error(redact_secrets(str(e)))
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
