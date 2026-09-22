"""Batching send orchestration around a single ServiceBusSender.

Fills a message batch until it is full -- by the SDK's size limit
(``MessageSizeExceededError``) or the configured ``batch_size`` count, whichever
comes first -- then flushes and re-adds the overflowing message. A message that
overflows an empty batch is sent unbatched; one that overflows even then exceeds
the entity's single-message cap and fails the row (:class:`UserException`).

The sender is injected (``ServiceBusSender``), so the Azure SDK boundary is
mockable in tests with a duck-typed fake -- Service Bus is AMQP, not HTTP, so
the send path cannot be recorded with vcrpy (see spec S7).
"""

import logging
from collections.abc import Iterable

from azure.servicebus import ServiceBusMessage, ServiceBusMessageBatch, ServiceBusSender
from azure.servicebus.exceptions import MessageSizeExceededError, ServiceBusError
from keboola.component.exceptions import UserException

from client import to_user_exception

logger = logging.getLogger(__name__)


class MessageSender:
    """Wrap one ServiceBusSender with size-and-count batching plus close-safety."""

    def __init__(self, sender: ServiceBusSender, batch_size: int, entity_name: str = ""):
        self._sender = sender
        self._batch_size = batch_size
        self._entity_name = entity_name

    def send(self, messages: Iterable[ServiceBusMessage]) -> int:
        """Send every message, batching by size and count; return the number sent."""
        try:
            return self._send_all(messages)
        except ServiceBusError as e:
            # Auth failure / missing entity / connection loss on a flush -> user-fixable (G3).
            raise to_user_exception(e, self._entity_name) from e

    def _send_all(self, messages: Iterable[ServiceBusMessage]) -> int:
        count = 0
        batch = self._sender.create_message_batch()
        batch_count = 0

        for message in messages:
            if batch_count >= self._batch_size:
                self._sender.send_messages(batch)
                batch = self._sender.create_message_batch()
                batch_count = 0

            if self._try_add(batch, message):
                batch_count += 1
            elif batch_count > 0:
                # The current (non-empty) batch is full: flush and start fresh.
                self._sender.send_messages(batch)
                batch = self._sender.create_message_batch()
                batch_count = 0
                if self._try_add(batch, message):
                    batch_count += 1
                else:
                    self._send_single(message)
            else:
                # Overflows an empty batch: too big to batch, send it on its own.
                self._send_single(message)

            count += 1

        if batch_count > 0:
            self._sender.send_messages(batch)

        return count

    @staticmethod
    def _try_add(batch: ServiceBusMessageBatch, message: ServiceBusMessage) -> bool:
        try:
            batch.add_message(message)
            return True
        except MessageSizeExceededError:
            return False

    def _send_single(self, message: ServiceBusMessage) -> None:
        try:
            self._sender.send_messages(message)
        except MessageSizeExceededError as e:
            raise UserException(
                "A message exceeds the target entity's single-message size limit and cannot be sent. "
                "Reduce the row size, or target a Premium-tier entity with a higher cap."
            ) from e

    def close(self) -> None:
        """Close the sender, swallowing a post-delivery close error as a warning (G4)."""
        try:
            self._sender.close()
        except Exception as e:  # noqa: BLE001 - G4: messages are already delivered; surfacing a
            # post-delivery close error would trigger a Keboola re-run and resend, so swallow it.
            logger.warning("Ignoring Service Bus sender close error after delivery: %s", e)
