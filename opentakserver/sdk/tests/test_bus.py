"""Tests for :mod:`opentakserver.sdk.modules.bus`.

Run inside the OTS container:
    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_bus.py
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time
from collections.abc import Iterator

import pytest

from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import bus
from opentakserver.sdk.permissions import (
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _testing_env_and_clear() -> Iterator[None]:
    """Ensure each test starts with an empty bus and ``OTS_TESTING=1`` so
    :func:`bus.clear` doesn't emit the "called outside test context" warning.
    """

    prior = os.environ.get('OTS_TESTING')
    os.environ['OTS_TESTING'] = '1'
    bus.clear()
    try:
        yield
    finally:
        bus.clear()
        bus.set_event_loop(None)
        if prior is None:
            os.environ.pop('OTS_TESTING', None)
        else:
            os.environ['OTS_TESTING'] = prior


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


def _manifest(slug: str = 'test-plugin') -> PluginManifest:
    return PluginManifest(
        api_version=2,
        name='Test',
        slug=slug,
        version='0.0.0',
        author='test',
        license='MIT',
        description='test fixture',
        permissions=PluginPermissions(),
        mount=[{'kind': 'tab', 'label': 'Test', 'path': '/test'}],
    )


# ---------------------------------------------------------------------------
# on / emit / off
# ---------------------------------------------------------------------------


def test_on_emit_off_roundtrip() -> None:
    received: list[dict] = []

    sub_id = bus.on('cot.received', received.append)
    assert isinstance(sub_id, str) and sub_id

    invoked = bus.emit('cot.received', {'uid': 'abc'})
    assert invoked == 1
    assert received == [{'uid': 'abc'}]

    assert bus.off(sub_id) is True

    invoked = bus.emit('cot.received', {'uid': 'def'})
    assert invoked == 0
    assert received == [{'uid': 'abc'}]  # unchanged


def test_off_unknown_subscription_returns_false() -> None:
    assert bus.off('nonexistent') is False


# ---------------------------------------------------------------------------
# Glob matching
# ---------------------------------------------------------------------------


def test_glob_subscription_matches_namespace() -> None:
    received: list[tuple[str, dict]] = []

    def handler(payload: dict) -> None:
        received.append(('cot.*', payload))

    bus.on('cot.*', handler)

    bus.emit('cot.received', {'n': 1})
    bus.emit('cot.sent', {'n': 2})
    bus.emit('eud.connected', {'n': 3})  # must NOT match cot.*

    assert received == [('cot.*', {'n': 1}), ('cot.*', {'n': 2})]


def test_wildcard_subscription_matches_everything() -> None:
    received: list[str] = []

    bus.on('*', lambda p: received.append(p['name']))

    bus.emit('cot.received', {'name': 'a'})
    bus.emit('eud.connected', {'name': 'b'})
    bus.emit('mesh.received', {'name': 'c'})

    assert received == ['a', 'b', 'c']


# ---------------------------------------------------------------------------
# No-cascade-failure
# ---------------------------------------------------------------------------


def test_handler_exception_does_not_block_other_handlers() -> None:
    received: list[str] = []

    def bad(_payload: dict) -> None:
        raise RuntimeError('boom')

    def good(_payload: dict) -> None:
        received.append('ok')

    bus.on('cot.received', bad)
    bus.on('cot.received', good)

    invoked = bus.emit('cot.received', {})
    # bad handler raises and is logged+swallowed; good handler still runs.
    # invoked counts only successful invocations.
    assert invoked == 1
    assert received == ['ok']


# ---------------------------------------------------------------------------
# subscribers() diagnostic
# ---------------------------------------------------------------------------


def test_subscribers_count_total_and_filtered() -> None:
    bus.on('cot.received', lambda _p: None)
    bus.on('cot.sent', lambda _p: None)
    bus.on('eud.connected', lambda _p: None)

    assert bus.subscribers() == 3
    assert bus.subscribers('cot.*') == 2
    assert bus.subscribers('eud.connected') == 1
    assert bus.subscribers('mesh.*') == 0


# ---------------------------------------------------------------------------
# clear()
# ---------------------------------------------------------------------------


def test_clear_wipes_all_subscribers() -> None:
    bus.on('a', lambda _p: None)
    bus.on('b', lambda _p: None)
    bus.on('*', lambda _p: None)

    assert bus.subscribers() == 3
    bus.clear()
    assert bus.subscribers() == 0
    assert bus.emit('a', {}) == 0


# ---------------------------------------------------------------------------
# Async handler dispatch
# ---------------------------------------------------------------------------


def test_async_handler_invoked_from_sync_emit() -> None:
    """An async handler must run when emit is called from sync code.

    Side effect is observed via ``Queue.put_nowait`` inside the coroutine,
    confirming the daemon-thread fallback ran our coroutine to completion.
    """

    queue: asyncio.Queue[dict] = asyncio.Queue()

    async def handler(payload: dict) -> None:
        queue.put_nowait(payload)

    bus.on('eud.position', handler)

    invoked = bus.emit('eud.position', {'lat': 40.0, 'lon': -111.0})
    assert invoked == 1

    # Daemon thread runs the coroutine — give it a beat to deliver.
    deadline = time.monotonic() + 2.0
    while queue.empty() and time.monotonic() < deadline:
        time.sleep(0.01)

    assert not queue.empty(), 'async handler never delivered'
    assert queue.get_nowait() == {'lat': 40.0, 'lon': -111.0}


def test_async_handler_routed_to_registered_loop() -> None:
    """When a process-wide loop is registered via :func:`set_event_loop`,
    async handlers from a different thread land on that loop."""

    loop = asyncio.new_event_loop()
    delivered = threading.Event()
    captured: list[dict] = []

    async def handler(payload: dict) -> None:
        captured.append(payload)
        delivered.set()

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    runner = threading.Thread(target=run_loop, daemon=True)
    runner.start()
    try:
        # Wait for loop to be running before registering it
        deadline = time.monotonic() + 1.0
        while not loop.is_running() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert loop.is_running()

        bus.set_event_loop(loop)
        bus.on('mesh.received', handler)
        invoked = bus.emit('mesh.received', {'from': '!abc'})
        assert invoked == 1

        assert delivered.wait(timeout=2.0), 'async handler never ran on registered loop'
        assert captured == [{'from': '!abc'}]
    finally:
        bus.set_event_loop(None)
        loop.call_soon_threadsafe(loop.stop)
        runner.join(timeout=2.0)
        loop.close()


# ---------------------------------------------------------------------------
# Reserved namespace
# ---------------------------------------------------------------------------


def test_plugin_emitting_core_event_is_rejected() -> None:
    bus.on('core.shutdown', lambda _p: None)

    with plugin_context(_manifest()):
        with pytest.raises(OTSPluginError) as exc_info:
            bus.emit('core.shutdown', {})

    assert exc_info.value.code == 'bus.reserved_namespace'


def test_core_emit_of_core_event_allowed() -> None:
    """Core OTS code (no plugin context) may emit core.* events."""

    received: list[dict] = []
    bus.on('core.shutdown', received.append)

    invoked = bus.emit('core.shutdown', {'reason': 'test'})
    assert invoked == 1
    assert received == [{'reason': 'test'}]


def test_plugin_can_subscribe_to_core_events() -> None:
    """Plugins may subscribe to ``core.*`` even though they cannot emit them."""

    received: list[dict] = []
    with plugin_context(_manifest()):
        bus.on('core.*', received.append)

    # Core (no plugin ctx) emits — plugin's subscription should fire.
    invoked = bus.emit('core.shutdown', {'reason': 'test'})
    assert invoked == 1
    assert received == [{'reason': 'test'}]
