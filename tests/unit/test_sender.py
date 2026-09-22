from typing import cast

import pytest
from azure.servicebus import ServiceBusMessage, ServiceBusSender
from azure.servicebus.exceptions import (
    MessageSizeExceededError,
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusConnectionError,
)
from keboola.component.exceptions import UserException

from sender import MessageSender


def _messages(*bodies: str) -> list[ServiceBusMessage]:
    return [ServiceBusMessage(b) for b in bodies]


class FakeBatch:
    def __init__(self, cap):
        self.cap, self.msgs = cap, []

    def add_message(self, m):
        if len(self.msgs) >= self.cap:
            raise MessageSizeExceededError(message="full")
        self.msgs.append(m)


class FakeSender:
    def __init__(self, cap=2):
        self.cap, self.sent_batches, self.sent_singles = cap, [], []
        self.closed = False

    def create_message_batch(self):
        return FakeBatch(self.cap)

    def send_messages(self, x, **kwargs):
        if isinstance(x, FakeBatch):
            self.sent_batches.append(list(x.msgs))
        else:
            self.sent_singles.append(x)

    def close(self):
        self.closed = True


def _sender(fake) -> ServiceBusSender:
    return cast(ServiceBusSender, fake)


def test_flush_by_size_overflow():
    fs = FakeSender(cap=2)
    n = MessageSender(_sender(fs), batch_size=1000).send(_messages("a", "b", "c"))  # cap 2 -> 2 batches
    assert n == 3
    assert len(fs.sent_batches) == 2


def test_flush_by_count():
    fs = FakeSender(cap=1000)
    MessageSender(_sender(fs), batch_size=2).send(_messages("a", "b", "c", "d"))
    assert [len(b) for b in fs.sent_batches] == [2, 2]


def test_single_send_fallback_when_message_overflows_empty_batch():
    fs = FakeSender(cap=0)  # every batch is immediately full -> empty-batch overflow
    msgs = _messages("x", "y")
    n = MessageSender(_sender(fs), batch_size=1000).send(msgs)
    assert n == 2
    assert fs.sent_singles == msgs
    assert fs.sent_batches == []


def test_oversized_message_raises_user_exception():
    class OversizedSender:
        def create_message_batch(self):
            return FakeBatch(cap=0)

        def send_messages(self, x, **kwargs):
            raise MessageSizeExceededError(message="too big")

        def close(self):
            pass

    with pytest.raises(UserException):
        MessageSender(_sender(OversizedSender()), batch_size=1000).send(_messages("huge"))


def test_empty_input_sends_nothing():
    fs = FakeSender(cap=1000)
    n = MessageSender(_sender(fs), batch_size=1000).send([])
    assert n == 0
    assert fs.sent_batches == []
    assert fs.sent_singles == []


def test_close_swallows_error_as_warning():
    class BadCloseSender(FakeSender):
        def close(self):
            raise RuntimeError("connection dropped after delivery")

    # close() must not raise (G4 no double-send)
    MessageSender(_sender(BadCloseSender()), batch_size=1000).close()


class RaisingOnBatchSender:
    def __init__(self, error):
        self.error = error

    def create_message_batch(self):
        raise self.error

    def send_messages(self, x, **kwargs):
        pass

    def close(self):
        pass


class RaisingOnSendSender:
    def __init__(self, error):
        self.error = error

    def create_message_batch(self):
        return FakeBatch(cap=1000)

    def send_messages(self, x, **kwargs):
        raise self.error

    def close(self):
        pass


@pytest.mark.parametrize(
    "error",
    [
        ServiceBusConnectionError(message="namespace unreachable"),
        ServiceBusAuthenticationError(message="bad credentials"),
        MessagingEntityNotFoundError(message="no such queue"),
    ],
)
def test_broker_error_on_create_batch_maps_to_user_exception(error):
    # A broker error is user-fixable -> UserException, not a bare ServiceBusError (which would exit 2).
    with pytest.raises(UserException):
        MessageSender(_sender(RaisingOnBatchSender(error)), batch_size=1000, entity_name="q1").send(_messages("a"))


def test_broker_error_on_flush_maps_to_user_exception():
    sender = RaisingOnSendSender(ServiceBusConnectionError(message="namespace unreachable"))
    with pytest.raises(UserException):
        # batch_size=1 forces a flush (send_messages) on the second message.
        MessageSender(_sender(sender), batch_size=1, entity_name="q1").send(_messages("a", "b"))


def test_sent_count_reflects_partial_delivery_before_failure():
    # I6: sent_count exposes how many messages were confirmed delivered before a mid-run flush failed.
    class PartialSender:
        def __init__(self):
            self.calls = 0

        def create_message_batch(self):
            return FakeBatch(cap=1000)

        def send_messages(self, x, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise ServiceBusConnectionError(message="dropped")

        def close(self):
            pass

    sender = MessageSender(_sender(PartialSender()), batch_size=1, entity_name="q1")
    with pytest.raises(UserException):
        sender.send(_messages("a", "b", "c"))
    assert sender.sent_count == 1  # first single-message batch flushed before the 2nd flush failed


def test_broker_error_message_redacts_sas_key():
    error = ServiceBusConnectionError(message="failed Endpoint=sb://x/;SharedAccessKey=SECRETKEY end")
    with pytest.raises(UserException) as exc:
        MessageSender(_sender(RaisingOnBatchSender(error)), batch_size=1000, entity_name="q1").send(_messages("a"))
    assert "SECRETKEY" not in str(exc.value)
    assert "SharedAccessKey=***" in str(exc.value)
