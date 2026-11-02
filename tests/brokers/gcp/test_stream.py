from collections.abc import AsyncIterator
from typing import Any

import pytest
from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.pubsub_v1.types import ReceivedMessage, StreamingPullResponse

from faststream_gcp_pubsub.subscriber.settler import FlowControl, Settler
from faststream_gcp_pubsub.subscriber.stream import StreamingPull


class _Call:
    def __init__(self, outcome: Exception | list[str]) -> None:
        self._outcome = outcome

    def cancel(self) -> None: ...

    async def __aiter__(self) -> AsyncIterator[Any]:
        if isinstance(self._outcome, Exception):
            raise self._outcome
        for ack_id in self._outcome:
            yield StreamingPullResponse(
                received_messages=[ReceivedMessage(ack_id=ack_id)]
            )


class _Client:
    """A subscriber client whose streams end the way the given outcomes say."""

    def __init__(self, *outcomes: Exception | list[str]) -> None:
        self._outcomes = list(outcomes)
        self.opened = 0

    async def streaming_pull(self, requests: Any, metadata: Any) -> _Call:
        self.opened += 1
        return _Call(self._outcomes.pop(0))


async def _noop(*args: Any) -> None: ...


def _stream(client: _Client) -> StreamingPull:
    settler = Settler(
        flow=FlowControl(), send_ack=_noop, extend=_noop, log=lambda *a: None
    )
    return StreamingPull(client, "projects/p/subscriptions/s", settler, lambda *a: None)  # type: ignore[arg-type]


@pytest.mark.asyncio()
async def test_a_retryable_error_reopens_the_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("faststream_gcp_pubsub.subscriber.stream.MAX_BACKOFF", 0)
    client = _Client(ServiceUnavailable("down"), ["a", "b"])
    stream = _stream(client)

    received = []
    async for message in stream:
        received.append(message.ack_id)
        if len(received) == 2:
            stream.stop()

    assert received == ["a", "b"]
    assert client.opened == 2


@pytest.mark.asyncio()
async def test_a_terminal_error_ends_the_stream() -> None:
    client = _Client(NotFound("no subscription"))

    with pytest.raises(NotFound):
        async for _ in _stream(client):
            pass

    assert client.opened == 1
