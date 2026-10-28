from __future__ import annotations

import asyncio
import random

from collector.errors import ErrorClass, backoff_delay, classify, should_retry


class FloodWaitError(Exception):
    def __init__(self, seconds: int) -> None:
        super().__init__(f"A wait of {seconds} seconds is required")
        self.seconds = seconds


class ChatForwardsRestrictedError(Exception):
    pass


class ChannelPrivateError(Exception):
    pass


class RpcCallFailError(Exception):
    pass


class RPCError(Exception):
    def __init__(self, seconds: int | None = None) -> None:
        super().__init__("rpc")
        self.seconds = seconds


def test_flood_wait_is_rate_limited_with_delay():
    failure = classify(FloodWaitError(42))
    assert failure.error_class is ErrorClass.RATE_LIMIT
    assert failure.retry_after == 42
    assert failure.retryable is True


def test_protected_content_is_permanent():
    failure = classify(ChatForwardsRestrictedError("forwards restricted"))
    assert failure.error_class is ErrorClass.PERMANENT
    assert failure.retryable is False


def test_private_channel_is_inaccessible():
    assert classify(ChannelPrivateError("nope")).error_class is ErrorClass.INACCESSIBLE


def test_transient_rpc_errors_are_retried():
    assert classify(RpcCallFailError("boom")).error_class is ErrorClass.TRANSIENT
    assert classify(asyncio.TimeoutError()).error_class is ErrorClass.TRANSIENT
    assert classify(ConnectionResetError()).error_class is ErrorClass.TRANSIENT


def test_rpc_error_with_seconds_is_rate_limited():
    failure = classify(RPCError(seconds=17))
    assert failure.error_class is ErrorClass.RATE_LIMIT
    assert failure.retry_after == 17


def test_programming_errors_are_permanent():
    assert classify(ValueError("bad payload")).error_class is ErrorClass.PERMANENT


def test_backoff_grows_and_is_capped():
    rng = random.Random(0)
    delays = [backoff_delay(n, base=2, cap=60, rng=rng) for n in range(1, 8)]
    assert delays[0] < delays[1] < delays[2]
    assert all(delay <= 60 for delay in delays)
    assert all(delay >= 0.1 for delay in delays)


def test_backoff_jitter_stays_within_bounds():
    rng = random.Random(1)
    for _ in range(50):
        delay = backoff_delay(3, base=2, cap=900, jitter=0.25, rng=rng)
        assert 8 * 0.74 <= delay <= 8 * 1.26


def test_should_retry_respects_class_and_budget():
    transient = classify(RpcCallFailError("boom"))
    assert should_retry(transient, attempts=1, max_attempts=3) is True
    assert should_retry(transient, attempts=3, max_attempts=3) is False
    permanent = classify(ChatForwardsRestrictedError("no"))
    assert should_retry(permanent, attempts=1, max_attempts=3) is False
