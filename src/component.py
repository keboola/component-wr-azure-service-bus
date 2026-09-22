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

from client import build_service_bus_client, to_user_exception
from configuration import Configuration, DestinationType
from message_builder import build_message
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
        with build_service_bus_client(config) as client:
            message_sender = MessageSender(self._open_sender(client, config), config.batch_size, config.entity_name)
            try:
                sent = message_sender.send(self._iter_messages(table, config))
            finally:
                message_sender.close()
        logger.info("Sent %d message(s) to %s '%s'.", sent, config.destination_type, config.entity_name)

    @sync_action("testConnection")
    def test_connection(self) -> ValidationResult:
        """Open a sender for the configured auth method and force the AMQP link open as an auth/connectivity probe."""
        config = Configuration(**self.configuration.parameters)
        try:
            with build_service_bus_client(config) as client, self._open_sender(client, config) as sender:
                sender.create_message_batch()  # forces the real connection + auth round-trip
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
    def _open_sender(client: ServiceBusClient, config: Configuration) -> ServiceBusSender:
        try:
            if config.destination_type == DestinationType.TOPIC:
                return client.get_topic_sender(topic_name=config.entity_name)
            return client.get_queue_sender(queue_name=config.entity_name)
        except ServiceBusError as e:
            raise to_user_exception(e, config.entity_name) from e

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
        logger.error(str(e))
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
