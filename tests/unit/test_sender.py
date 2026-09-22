from typing import cast

import pytest
from azure.servicebus import ServiceBusMessage, ServiceBusSender
from azure.servicebus.exceptions import MessageSizeExceededError
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

    def send_messages(self, x):
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

        def send_messages(self, x):
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
