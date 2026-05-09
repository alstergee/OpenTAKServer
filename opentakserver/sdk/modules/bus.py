"""OpenTAK Plugin SDK v2 — in-process event bus (``OTS.bus``).

A small synchronous publish/subscribe primitive. Plugins register handlers
keyed on dot-namespaced event names (``cot.received``, ``eud.connected``,
``mesh.received``, …) and core OTS code emits to those names. ``fnmatch``
glob patterns let one subscription cover a whole namespace
(``cot.*``, ``*``).

Threading model
---------------

* Subscriber registry is guarded by a :class:`threading.RLock` so
  ``on``/``off``/``emit``/``subscribers``/``clear`` can be called from any
  thread (Flask request thread, apscheduler worker, AMQP consumer, etc.).
* :func:`emit` is synchronous: every matching sync handler runs in the
  caller's thread before ``emit`` returns. Async handlers are dispatched
  best-effort — see "Async handlers" below — and ``emit`` does NOT wait
  for them.
* No-cascade-failure: a handler raising an exception is logged at ERROR
  with ``extra={'event', 'subscription_id'}`` and swallowed; remaining
  handlers still run.

Async handlers
--------------

If :func:`emit` is called from inside a running event loop the async
handler is scheduled as a task on that loop (``asyncio.ensure_future``).
If called from a thread with no running loop, the SDK looks for a
process-wide loop via :func:`set_event_loop` — if one exists and is
running, ``asyncio.run_coroutine_threadsafe`` posts the coroutine onto
it. Otherwise the SDK falls back to ``asyncio.run`` in a daemon thread.

Trade-off: the daemon-thread fallback means an async handler may run on
a throwaway loop with no shared state with the rest of the app. Plugin
authors who need OTS's main loop should declare the loop via
:func:`set_event_loop` in their startup hook (Phase A.4 ``loader_v2`` is
expected to do this for the Flask app's loop). Sync handlers don't have
this caveat — prefer them unless you genuinely need ``await``.

Reserved namespaces
-------------------

Event names beginning with ``core.`` are reserved for core OTS emit
calls. A plugin (i.e. ``current_plugin() is not None``) attempting to
:func:`emit` such an event raises
:class:`OTSPluginError` with ``code='bus.reserved_namespace'``.
Subscribing to ``core.*`` is allowed for any caller.
"""

from __future__ import annotations

import asyncio
import fnmatch
import inspect
import logging
import os
import secrets
import threading
from collections.abc import Awaitable, Callable
from typing import Any, Union

from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.permissions import current_plugin

logger = logging.getLogger(__name__)


# Public type alias — a handler may be sync or async; either way it takes a
# dict payload and its return value is ignored.
Handler = Union[Callable[[dict[str, Any]], Any], Callable[[dict[str, Any]], Awaitable[Any]]]


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------


# Maps ``pattern -> list[(subscription_id, handler)]``. ``pattern`` is the
# raw event string passed to :func:`on` (may contain ``fnmatch`` globs).
_subscribers: dict[str, list[tuple[str, Handler]]] = {}
_lock = threading.RLock()
_event_loop: asyncio.AbstractEventLoop | None = None


# ---------------------------------------------------------------------------
# Async loop registration (optional, used to route async handlers to the
# main event loop when emit is called from a different thread).
# ---------------------------------------------------------------------------


def set_event_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    """Register the process's main event loop for cross-thread async dispatch.

    Optional. The Flask + apscheduler bootstrap (loader_v2) is expected to
    call this with the running loop so async handlers emitted from worker
    threads land on the same loop as the rest of OTS. Pass ``None`` to
    clear the registration (test teardown).
    """

    global _event_loop
    _event_loop = loop


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def on(event: str, handler: Handler) -> str:
    """Register ``handler`` for ``event`` (an exact name or ``fnmatch`` glob).

    Args
    ----
    event:
        Dot-namespaced event pattern. ``'cot.received'`` matches a single
        event; ``'cot.*'`` matches every ``cot.<x>``; ``'*'`` matches all
        events. Patterns are evaluated via :func:`fnmatch.fnmatch` at
        emit time.
    handler:
        Sync or async callable taking a single ``dict`` payload. Async
        handlers are detected via :func:`inspect.iscoroutinefunction` and
        dispatched onto an event loop (see module docstring).

    Returns
    -------
    str
        Subscription ID (8-byte url-safe token) for later :func:`off`.

    Notes
    -----
    No scope is required to subscribe — observation is read-only on the
    bus. Emission to reserved namespaces is gated separately, see
    :func:`emit`.
    """

    if not isinstance(event, str) or not event:
        raise OTSPluginError(
            code='bus.invalid_event',
            message=f'event must be a non-empty string, got {event!r}',
        )
    if not callable(handler):
        raise OTSPluginError(
            code='bus.invalid_handler',
            message=f'handler must be callable, got {type(handler).__name__}',
        )

    sub_id = secrets.token_urlsafe(8)
    with _lock:
        _subscribers.setdefault(event, []).append((sub_id, handler))
    logger.debug(
        'bus.on registered',
        extra={'event': event, 'subscription_id': sub_id},
    )
    return sub_id


def off(subscription_id: str) -> bool:
    """Remove the subscription with id ``subscription_id``.

    Returns ``True`` if a subscription was removed, ``False`` if no such
    id was registered (idempotent — never raises).
    """

    with _lock:
        for pattern, handlers in list(_subscribers.items()):
            for index, (sub_id, _handler) in enumerate(handlers):
                if sub_id == subscription_id:
                    handlers.pop(index)
                    if not handlers:
                        _subscribers.pop(pattern, None)
                    logger.debug(
                        'bus.off removed',
                        extra={'event': pattern, 'subscription_id': sub_id},
                    )
                    return True
    return False


def emit(event: str, payload: dict[str, Any]) -> int:
    """Fire ``event`` to every matching subscriber. Returns count invoked.

    Synchronous: every matching sync handler runs in the calling thread
    before this returns. Async handlers are scheduled (see module
    docstring) and counted as invoked even though their work is
    non-blocking.

    A handler raising any exception is logged at ERROR with
    ``extra={'event', 'subscription_id'}`` and swallowed; remaining
    handlers still run (no-cascade-failure principle).

    Reserved namespace: emitting an event whose name starts with
    ``'core.'`` from inside a plugin context raises
    :class:`OTSPluginError` with ``code='bus.reserved_namespace'``.
    Core OTS code (no active plugin context) may emit anything.
    """

    if not isinstance(event, str) or not event:
        raise OTSPluginError(
            code='bus.invalid_event',
            message=f'event must be a non-empty string, got {event!r}',
        )

    if event.startswith('core.') and current_plugin() is not None:
        raise OTSPluginError(
            code='bus.reserved_namespace',
            message=(
                f'plugin {current_plugin().slug!r} attempted to emit '
                f'reserved event {event!r}; only core OTS code may emit '
                f'core.* events.'
            ),
        )

    # Snapshot under the lock so handlers can call back into on/off without
    # deadlocking or mutating the iteration list mid-flight.
    with _lock:
        matched: list[tuple[str, str, Handler]] = []
        for pattern, handlers in _subscribers.items():
            if fnmatch.fnmatchcase(event, pattern):
                for sub_id, handler in handlers:
                    matched.append((pattern, sub_id, handler))

    invoked = 0
    for _pattern, sub_id, handler in matched:
        try:
            if inspect.iscoroutinefunction(handler):
                _dispatch_async(handler, payload, sub_id, event)
            else:
                handler(payload)
            invoked += 1
        except Exception:  # noqa: BLE001 — bus is no-cascade-failure
            logger.error(
                'bus handler raised; continuing',
                exc_info=True,
                extra={'event': event, 'subscription_id': sub_id},
            )

    return invoked


def subscribers(event: str | None = None) -> int:
    """Return count of registered subscribers (optionally filtered).

    With ``event=None`` returns the total. With a string, the string is
    matched against each registered *pattern* using :func:`fnmatch.fnmatchcase`.
    A query of ``'cot.*'`` will count subscriptions registered under
    ``'cot.*'`` exactly, not subscriptions registered under ``'cot.received'``
    — the diagnostic asks "how many subs are listening for this scope?",
    which is symmetric with the emit-side glob match.
    """

    with _lock:
        if event is None:
            return sum(len(handlers) for handlers in _subscribers.values())
        total = 0
        for pattern, handlers in _subscribers.items():
            if fnmatch.fnmatchcase(pattern, event) or fnmatch.fnmatchcase(
                event, pattern
            ):
                total += len(handlers)
        return total


def clear() -> None:
    """Remove every subscriber. Intended for test teardown.

    Logs a WARNING when called outside a test context (detected via the
    ``OTS_TESTING`` env var). Production code should use :func:`off`
    with an explicit subscription id instead.
    """

    if os.environ.get('OTS_TESTING') != '1':
        logger.warning(
            'bus.clear() called outside test context; this wipes ALL '
            'plugin subscriptions and should not happen in production.'
        )
    with _lock:
        _subscribers.clear()


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _dispatch_async(
    handler: Callable[[dict[str, Any]], Awaitable[Any]],
    payload: dict[str, Any],
    sub_id: str,
    event: str,
) -> None:
    """Schedule ``handler(payload)`` on a running loop, or a daemon thread."""

    coro = handler(payload)

    # 1. Caller thread already has a running loop → schedule on it.
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        loop.create_task(_wrap_async(coro, sub_id, event))
        return

    # 2. A process-wide loop has been registered → post to it.
    if _event_loop is not None and _event_loop.is_running():
        asyncio.run_coroutine_threadsafe(
            _wrap_async(coro, sub_id, event), _event_loop
        )
        return

    # 3. Fallback: spin a daemon thread with its own loop.
    def _run() -> None:
        try:
            asyncio.run(_wrap_async(coro, sub_id, event))
        except Exception:  # noqa: BLE001 — already logged inside _wrap_async
            pass

    thread = threading.Thread(
        target=_run,
        name=f'ots-bus-async-{sub_id}',
        daemon=True,
    )
    thread.start()


async def _wrap_async(
    coro: Awaitable[Any],
    sub_id: str,
    event: str,
) -> None:
    """Await ``coro`` and log+swallow any exception. Used by every dispatch path."""

    try:
        await coro
    except Exception:  # noqa: BLE001 — bus is no-cascade-failure
        logger.error(
            'bus async handler raised; continuing',
            exc_info=True,
            extra={'event': event, 'subscription_id': sub_id},
        )


__all__ = [
    'Handler',
    'clear',
    'emit',
    'off',
    'on',
    'set_event_loop',
    'subscribers',
]
