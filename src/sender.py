"""Batching send orchestration around a single ServiceBusSender.

Fills a message batch until it is full -- by the SDK's size limit
(``MessageSizeExceededError``) or the configured ``batch_size`` count, whichever
comes first -- then flushes and re-adds the overflowing message. A message that
overflows an empty batch is sent unbatched; one that overflows even then exceeds
the entity's single-message cap and fails the row (:class:`UserException`).

The sender is injected (``ServiceBusSender``), so the Azure SDK boundary is
mockable in tests with a duck-typed fake -- Service Bus is AMQP, not HTTP, so
the send path cannot be recorded with vcrpy.
"""

import logging
from collections.abc import Iterable

from azure.servicebus import ServiceBusMessage, ServiceBusMessageBatch, ServiceBusSender
from azure.servicebus.exceptions import MessageSizeExceededError, ServiceBusError
from keboola.component.exceptions import UserException

from client import redact_secrets, to_user_exception

logger = logging.getLogger(__name__)

# Bound each broker send so a stalled AMQP link fails the job with an actionable
# timeout error instead of hanging. A fixed value is used deliberately -- this is
# a transport safety net, not a user-tunable knob, so it is not a config field.
SEND_TIMEOUT_SECONDS = 60

# The SDK's ServiceBusMessageBatch.size_in_bytes (what add_message enforces) under-counts
# the true AMQP wire encoding of the batch envelope by a few dozen bytes, and
# create_message_batch() caps a batch at exactly the link's max message size
# (MAX_BATCH_SIZE_STANDARD / _PREMIUM) with no headroom. A batch packed right up to that cap
# therefore encodes slightly OVER the broker limit and is rejected on send. Reserve this
# margin below the link max so a full batch still fits on the wire. 1 KiB is ~16x the observed
# overshoot and a negligible fraction of the 256 KB / 1 MB caps.
BATCH_SIZE_SAFETY_MARGIN_BYTES = 1024


class MessageSender:
    """Wrap one ServiceBusSender with size-and-count batching plus close-safety."""

    def __init__(self, sender: ServiceBusSender, batch_size: int, entity_name: str = "") -> None:
        self._sender = sender
        self._batch_size = batch_size
        self._entity_name = entity_name
        self._batch_max_size: int | None = None  # link max minus the safety margin; resolved on first batch
        self.sent_count = 0  # messages confirmed delivered so far (for partial-failure reporting)

    def _new_batch(self) -> ServiceBusMessageBatch:
        """Create a batch capped a safety margin below the link's max message size.

        The first call probes the link-negotiated max via a throwaway batch (also the
        first real broker round-trip), so it is only reached once there is a message to
        send -- an empty input opens no link.
        """
        if self._batch_max_size is None:
            link_max = self._sender.create_message_batch().max_size_in_bytes
            self._batch_max_size = max(link_max - BATCH_SIZE_SAFETY_MARGIN_BYTES, BATCH_SIZE_SAFETY_MARGIN_BYTES)
        return self._sender.create_message_batch(max_size_in_bytes=self._batch_max_size)

    def send(self, messages: Iterable[ServiceBusMessage]) -> int:
        """Send every message, batching by size and count; return the number sent."""
        try:
            return self._send_all(messages)
        except ServiceBusError as e:
            # Auth failure / missing entity / connection loss on a flush -> user-fixable (G3).
            raise to_user_exception(e, self._entity_name) from e

    def _send_all(self, messages: Iterable[ServiceBusMessage]) -> int:
        # Created lazily on the first message. create_message_batch() is the first
        # real broker round-trip -- it opens the AMQP link to read the entity's max
        # message size -- so an empty input table must send nothing without it.
        batch: ServiceBusMessageBatch | None = None
        batch_count = 0

        for message in messages:
            if batch is None:
                batch = self._new_batch()
            if batch_count >= self._batch_size:
                self._flush(batch, batch_count)
                batch = self._new_batch()
                batch_count = 0

            if self._try_add(batch, message):
                batch_count += 1
            elif batch_count > 0:
                # The current (non-empty) batch is full: flush and start fresh.
                self._flush(batch, batch_count)
                batch = self._new_batch()
                batch_count = 0
                if self._try_add(batch, message):
                    batch_count += 1
                else:
                    self._send_single(message)
            else:
                # Overflows an empty batch: too big to batch, send it on its own.
                self._send_single(message)

        if batch is not None and batch_count > 0:
            self._flush(batch, batch_count)

        return self.sent_count

    def _flush(self, batch: ServiceBusMessageBatch, batch_count: int) -> None:
        self._sender.send_messages(batch, timeout=SEND_TIMEOUT_SECONDS)
        self.sent_count += batch_count

    @staticmethod
    def _try_add(batch: ServiceBusMessageBatch, message: ServiceBusMessage) -> bool:
        try:
            batch.add_message(message)
            return True
        except MessageSizeExceededError:
            return False

    def _send_single(self, message: ServiceBusMessage) -> None:
        try:
            self._sender.send_messages(message, timeout=SEND_TIMEOUT_SECONDS)
        except MessageSizeExceededError as e:
            raise UserException(
                "A message exceeds the target entity's single-message size limit and cannot be sent. "
                "Reduce the row size, or target a Premium-tier entity with a higher cap."
            ) from e
        self.sent_count += 1

    def close(self) -> None:
        """Close the sender, swallowing a post-delivery close error as a warning (G4)."""
        try:
            self._sender.close()
        except Exception as e:  # noqa: BLE001 - G4: messages are already delivered; surfacing a
            # post-delivery close error would trigger a Keboola re-run and resend, so swallow it.
            logger.warning("Ignoring Service Bus sender close error after delivery: %s", redact_secrets(str(e)))
