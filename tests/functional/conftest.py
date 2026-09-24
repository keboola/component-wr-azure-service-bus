"""SDK-mock harness for the Azure Service Bus writer functional tests.

Azure Service Bus's data plane is AMQP 1.0 over TLS, not HTTP, so ``vcrpy``
(the engine behind ``keboola.datadirtest``'s VCR recording) cannot record or
replay the send path -- a pure sender makes no HTTP calls. Instead of recording
cassettes, this autouse fixture patches the Azure SDK boundary inside
``src/client.py`` (``ServiceBusClient`` plus the ``ClientSecretCredential``
class) with message-capturing fakes, so the component's full ``run()`` /
``testConnection``
path executes end-to-end with NO network. The fake records every
``ServiceBusMessage`` and batch it is handed so the tests can assert on them.

The fake ``from_connection_string`` still delegates to the REAL SDK parser for
validation (offline -- no connection is opened at construction), so a malformed
connection string raises ``ValueError`` exactly as in production. The batch/
sender fakes model the Standard-tier 256 KB message cap so the oversized-row
fail-fast path is exercised for real.
"""

from typing import Self

import pytest
from azure.servicebus import ServiceBusClient as _RealServiceBusClient
from azure.servicebus.exceptions import MessageSizeExceededError

import client as client_mod

# Azure Standard-tier single-message / batch cap (256 KB). Modeled by the fakes
# so a row over the cap trips MessageSizeExceededError like a real broker would.
STANDARD_TIER_CAP_BYTES = 256 * 1024


def _message_size(message) -> int:
    """Approximate the serialized size of a ServiceBusMessage by its body bytes."""
    return len(str(message).encode("utf-8"))


class Capture:
    """Accumulates everything the mocked SDK was asked to send, for assertions."""

    def __init__(self) -> None:
        self.messages: list = []  # every message sent (batched + single)
        self.batches: list[list] = []  # one entry per flushed batch
        self.singles: list = []  # messages sent unbatched (C2 fallback)
        self.batches_created = 0
        self.opened_queue: str | None = None
        self.opened_topic: str | None = None
        self.closed = False
        self.namespace: str | None = None
        self.credential = None


class FakeBatch:
    """A message batch that rejects a message once the byte cap is reached."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self.max_size_in_bytes = max_bytes  # read by the sender to derive the safety-margin cap
        self.messages: list = []
        self._size = 0

    def add_message(self, message) -> None:
        size = _message_size(message)
        if self._size + size > self._max:
            raise MessageSizeExceededError(message="Batch size cap exceeded.")
        self.messages.append(message)
        self._size += size


class FakeSender:
    """Duck-typed ServiceBusSender that records batches / single sends."""

    def __init__(self, capture: Capture, max_bytes: int) -> None:
        self._capture = capture
        self._max = max_bytes

    def create_message_batch(self, max_size_in_bytes: int | None = None) -> FakeBatch:
        self._capture.batches_created += 1
        return FakeBatch(max_size_in_bytes or self._max)

    def send_messages(self, message_or_batch, **kwargs) -> None:
        if isinstance(message_or_batch, FakeBatch):
            msgs = list(message_or_batch.messages)
            self._capture.batches.append(msgs)
            self._capture.messages.extend(msgs)
        else:
            if _message_size(message_or_batch) > self._max:
                raise MessageSizeExceededError(message="Single message exceeds the entity size cap.")
            self._capture.singles.append(message_or_batch)
            self._capture.messages.append(message_or_batch)

    def close(self) -> None:
        self._capture.closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False


class FakeServiceBusClient:
    """Stand-in for azure.servicebus.ServiceBusClient (no network)."""

    capture: Capture  # assigned per test by the mock_service_bus fixture
    max_bytes: int = STANDARD_TIER_CAP_BYTES

    def __init__(self, fully_qualified_namespace: str | None = None, credential=None, **kwargs) -> None:
        cap = type(self).capture
        if fully_qualified_namespace is not None:
            cap.namespace = fully_qualified_namespace
        if credential is not None:
            cap.credential = credential

    @classmethod
    def from_connection_string(cls, conn_str: str, **kwargs) -> FakeServiceBusClient:
        # Validate with the real SDK parser (offline -- construction opens no
        # connection), so a malformed connection string raises ValueError just
        # as in production; then discard the real client and return the fake.
        _RealServiceBusClient.from_connection_string(conn_str=conn_str, **kwargs).close()
        return cls()

    def _sender(self) -> FakeSender:
        return FakeSender(type(self).capture, type(self).max_bytes)

    def get_queue_sender(self, queue_name: str) -> FakeSender:
        type(self).capture.opened_queue = queue_name
        return self._sender()

    def get_topic_sender(self, topic_name: str) -> FakeSender:
        type(self).capture.opened_topic = topic_name
        return self._sender()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> bool:
        return False


class FakeClientSecretCredential:
    """Records that the service-principal branch was selected (no token fetch)."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, **kwargs) -> None:
        self.kind = "client_secret"
        self.tenant_id = tenant_id
        self.client_id = client_id


@pytest.fixture(autouse=True)
def mock_service_bus(monkeypatch):
    """Patch the SDK boundary in client.py; yield the per-test capture sink."""
    capture = Capture()
    FakeServiceBusClient.capture = capture
    FakeServiceBusClient.max_bytes = STANDARD_TIER_CAP_BYTES
    monkeypatch.setattr(client_mod, "ServiceBusClient", FakeServiceBusClient)
    monkeypatch.setattr(client_mod, "ClientSecretCredential", FakeClientSecretCredential)
    yield capture
