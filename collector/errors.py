"""Failure classification, retry policy and backoff.

Telethon exception classes are matched by *name* so this module stays
importable (and unit-testable) without Telethon installed, and so that
subclasses of the same RPC error behave consistently.

Unknown exceptions are **not** silently treated as safe to retry: they get
their own ``UNKNOWN`` class with a much smaller retry budget, because retrying
a programming error or an unexpected Telegram response burns rate limit and
delays the honest verdict.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from enum import Enum


class ErrorClass(str, Enum):
    TRANSIENT = "transient"          # retry with backoff
    RATE_LIMIT = "rate_limit"        # retry after the server-specified delay
    PERMANENT = "permanent"          # never retry
    INACCESSIBLE = "inaccessible"    # we cannot read/forward it at all
    AMBIGUOUS = "ambiguous"          # request may have been applied
    UNKNOWN = "unknown"              # unrecognised: short retry budget, then ambiguous


RETRYABLE = {ErrorClass.TRANSIENT, ErrorClass.RATE_LIMIT, ErrorClass.UNKNOWN}

#: Unknown failures are retried at most this many times before being treated as
#: a possibly-delivered request.
UNKNOWN_RETRY_BUDGET = 2


@dataclass(slots=True)
class Failure:
    error_class: ErrorClass
    message: str
    retry_after: float | None = None

    @property
    def retryable(self) -> bool:
        return self.error_class in RETRYABLE


# Telegram RPC error names (Telethon raises subclasses of RPCError).
_RATE_LIMIT_ERRORS = {
    "FloodWaitError",
    "FloodTestPhoneWaitError",
    "SlowModeWaitError",
    "TakeoutInitDelayError",
    "FloodPremiumWaitError",
}
_PERMANENT_ERRORS = {
    "ChatForwardsRestrictedError",
    "ChatWriteForbiddenError",
    "MessageIdInvalidError",
    "MessageNotModifiedError",
    "MessageEmptyError",
    "MediaEmptyError",
    "MessageAuthorRequiredError",
    "ChannelTooLargeError",
    "ChatRestrictedError",
    "UserBannedInChannelError",
    "ChatSendMediaForbiddenError",
    "ChatSendStickersForbiddenError",
    "ChatSendGifsForbiddenError",
    "ChatSendPlainForbiddenError",
    "ChatSendPhotosForbiddenError",
    "ChatSendVideosForbiddenError",
    "ChatSendDocsForbiddenError",
    "ChatSendRoundVideosForbiddenError",
    "ChatSendAudioForbiddenError",
    "ChatSendPollForbiddenError",
    "MessageTooLongError",
    "PeerIdInvalidError",
}
_INACCESSIBLE_ERRORS = {
    "ChannelPrivateError",
    "ChatAdminRequiredError",
    "ChannelInvalidError",
    "ChatForbiddenError",
    "UserNotParticipantError",
    "InviteHashExpiredError",
    "InviteHashInvalidError",
}
_TRANSIENT_ERRORS = {
    "RpcCallFailError",
    "RpcMcgetFailError",
    "ServerError",
    "TimedOutError",
    "ServiceUnavailableError",
    "TimeoutError",
    "ConnectionError",
    "OSError",
    "AuthKeyDuplicatedError",   # session invalid: retrying cannot help, see below
}


def classify(exc: BaseException) -> Failure:
    """Map an exception to a :class:`Failure`."""

    name = type(exc).__name__
    text = f"{name}: {exc}".strip()
    seconds = getattr(exc, "seconds", None)

    if name == "AuthKeyDuplicatedError":
        # The session was used from two places; it is now dead.  Permanent.
        return Failure(ErrorClass.PERMANENT, text)

    if name in _RATE_LIMIT_ERRORS:
        delay = float(seconds) if isinstance(seconds, (int, float)) else None
        return Failure(ErrorClass.RATE_LIMIT, text, retry_after=delay)

    # Any RPC error carrying a wait duration is a rate limit signal.
    if seconds is not None and isinstance(seconds, (int, float)) and seconds > 0:
        return Failure(ErrorClass.RATE_LIMIT, text, retry_after=float(seconds))

    if name in _INACCESSIBLE_ERRORS:
        return Failure(ErrorClass.INACCESSIBLE, text)
    if name in _PERMANENT_ERRORS:
        return Failure(ErrorClass.PERMANENT, text)
    if name in _TRANSIENT_ERRORS:
        return Failure(ErrorClass.TRANSIENT, text)
    if isinstance(exc, (asyncio.TimeoutError, ConnectionError, TimeoutError, OSError)):
        return Failure(ErrorClass.TRANSIENT, text)
    if isinstance(exc, (ValueError, TypeError, KeyError, AttributeError, RuntimeError)):
        # A programming or data problem: retrying will not help.
        return Failure(ErrorClass.PERMANENT, text)
    return Failure(ErrorClass.UNKNOWN, text)


def retry_budget(failure: Failure, max_attempts: int) -> int:
    """Attempt budget for this failure class."""

    if failure.error_class is ErrorClass.UNKNOWN:
        return min(max_attempts, UNKNOWN_RETRY_BUDGET)
    if failure.error_class is ErrorClass.RATE_LIMIT:
        # FloodWait means "come back later", so it gets more room than a crash,
        # but still a hard ceiling.
        return max_attempts * 3
    return max_attempts


def backoff_delay(
    attempt: int,
    *,
    base: float = 2.0,
    cap: float = 900.0,
    jitter: float = 0.25,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with jitter.

    ``attempt`` is 1-based.  Result is bounded by ``cap`` and never below
    ``0.1`` s, so a huge attempt count cannot produce a negative sleep.
    """

    attempt = max(1, int(attempt))
    nominal = min(cap, base * (2 ** (attempt - 1)))
    source = rng or random
    # Uniform in [nominal * (1 - jitter), nominal * (1 + jitter)]
    delay = nominal * (1 + source.uniform(-jitter, jitter))
    return max(0.1, min(cap, delay))


def should_retry(failure: Failure, attempts: int, max_attempts: int) -> bool:
    if not failure.retryable:
        return False
    return attempts < retry_budget(failure, max_attempts)
