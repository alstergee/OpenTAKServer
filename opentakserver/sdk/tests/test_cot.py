"""Tests for :mod:`opentakserver.sdk.modules.cot`.

Run inside the OTS container::

    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_cot.py
"""

from __future__ import annotations

import contextlib
import json
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from unittest import mock

import pytest

from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import cot
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


def _manifest(
    *,
    read: list[str] | None = None,
    write: list[str] | None = None,
) -> PluginManifest:
    return PluginManifest(
        api_version=2,
        name='Test Cot',
        slug='test-cot',
        version='0.0.0',
        author='test',
        license='MIT',
        description='cot test fixture',
        permissions=PluginPermissions(
            read=read or [],
            write=write or [],
        ),
        mount=[{'kind': 'tab', 'label': 'Test', 'path': '/test'}],
    )


@pytest.fixture
def amqp() -> Iterator[mock.MagicMock]:
    """Mock the AMQP publisher so tests don't hit the real broker.

    The cot module imports ``publish`` from ``opentakserver.amqp_publisher``
    at module load time, so we patch the bound symbol on the cot module
    (``cot._amqp_publish``) rather than the source.
    """

    with mock.patch.object(cot, '_amqp_publish') as patched:
        yield patched


@pytest.fixture(autouse=True)
def _clear_subscriptions() -> Iterator[None]:
    """Each test starts with a clean subscription registry."""

    cot._subscriptions.clear()
    yield
    cot._subscriptions.clear()


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


def test_send_raises_when_plugin_lacks_write_cot(amqp: mock.MagicMock) -> None:
    with plugin_context(_manifest(write=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            cot.send(uid='x', type='a-f-G-U-C', lat=0.0, lon=0.0)

    assert exc_info.value.code == 'permission.write.cot'
    assert isinstance(exc_info.value, OTSPluginError)
    amqp.assert_not_called()


def test_broadcast_raises_when_plugin_lacks_write_cot(amqp: mock.MagicMock) -> None:
    with plugin_context(_manifest(write=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            cot.broadcast(type='b-a-o-tbl', lat=0.0, lon=0.0)

    assert exc_info.value.code == 'permission.write.cot'
    amqp.assert_not_called()


def test_subscribe_raises_when_plugin_lacks_read_cot() -> None:
    def _handler(event: cot.CoTEvent) -> None:
        del event

    with plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            cot.subscribe('a-f-*', _handler)

    assert exc_info.value.code == 'permission.read.cot'


def test_send_passes_when_plugin_has_write_cot(amqp: mock.MagicMock) -> None:
    with plugin_context(_manifest(write=['cot'])):
        result = cot.send(uid='evt-1', type='a-f-G-U-C', lat=0.0, lon=0.0)

    assert result == 'evt-1'
    amqp.assert_called_once()


def test_send_passes_for_core_caller_without_plugin_context(
    amqp: mock.MagicMock,
) -> None:
    """Bare core code (no plugin context) bypasses the scope gate."""

    result = cot.send(uid='evt-core', type='a-f-G-U-C', lat=1.0, lon=2.0)

    assert result == 'evt-core'
    amqp.assert_called_once()


# ---------------------------------------------------------------------------
# Happy path / routing
# ---------------------------------------------------------------------------


def test_broadcast_returns_a_valid_uid(amqp: mock.MagicMock) -> None:
    result = cot.broadcast(
        type='b-a-o-tbl', lat=40.0, lon=-111.0, callsign='Alert'
    )

    assert isinstance(result, str)
    assert len(result) >= 32  # uuid4 is 36 chars; sanity check
    amqp.assert_called_once()
    call = amqp.call_args
    assert call.kwargs['exchange'] == 'cot_controller'
    assert call.kwargs['routing_key'] == ''

    # Body is the JSON envelope
    body = json.loads(call.kwargs['body'])
    assert 'uid' in body and 'cot' in body
    # XML parses + carries the broadcast uid + correct type
    root = ET.fromstring(body['cot'])
    assert root.tag == 'event'
    assert root.attrib['uid'] == result
    assert root.attrib['type'] == 'b-a-o-tbl'


def test_send_directed_publishes_once_per_uid(amqp: mock.MagicMock) -> None:
    cot.send(
        uid='dm-1',
        type='a-f-G-U-C',
        lat=0.0,
        lon=0.0,
        to_uids=['ANDROID-aaa', 'ANDROID-bbb'],
    )

    assert amqp.call_count == 2
    routing_keys = [c.kwargs['routing_key'] for c in amqp.call_args_list]
    exchanges = {c.kwargs['exchange'] for c in amqp.call_args_list}
    assert routing_keys == ['ANDROID-aaa', 'ANDROID-bbb']
    assert exchanges == {'dms'}


def test_send_rejects_empty_to_uids_list(amqp: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.send(uid='x', type='a-f-G-U-C', lat=0.0, lon=0.0, to_uids=[])

    assert exc_info.value.code == 'cot.invalid_to_uids'
    amqp.assert_not_called()


def test_send_rejects_bad_to_uids_entry(amqp: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.send(
            uid='x',
            type='a-f-G-U-C',
            lat=0.0,
            lon=0.0,
            to_uids=['ok', ''],  # type: ignore[list-item]
        )

    assert exc_info.value.code == 'cot.invalid_to_uids'


def test_send_envelope_uid_is_plugin_slug_when_in_plugin_context(
    amqp: mock.MagicMock,
) -> None:
    with plugin_context(_manifest(write=['cot'])):
        cot.send(uid='evt', type='a-f-G-U-C', lat=0.0, lon=0.0)

    body = json.loads(amqp.call_args.kwargs['body'])
    assert body['uid'] == 'plugin:test-cot'


def test_send_envelope_uid_is_server_when_no_plugin_context(
    amqp: mock.MagicMock,
) -> None:
    cot.send(uid='evt', type='a-f-G-U-C', lat=0.0, lon=0.0)

    body = json.loads(amqp.call_args.kwargs['body'])
    assert body['uid'] == 'server'


# ---------------------------------------------------------------------------
# XML escaping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'callsign',
    [
        "Bravo & Charlie",
        '<script>alert(1)</script>',
        'Alpha "Quoted"',
        "Tic 'Tac' Toe",
        'A<>B&C\'D"E',
    ],
)
def test_xml_escapes_special_chars_in_callsign(
    amqp: mock.MagicMock, callsign: str
) -> None:
    cot.send(
        uid='evt-x',
        type='a-f-G-U-C',
        lat=0.0,
        lon=0.0,
        callsign=callsign,
    )

    body = json.loads(amqp.call_args.kwargs['body'])
    xml = body['cot']

    # Must parse cleanly (no malformed XML from raw special chars).
    root = ET.fromstring(xml)
    contact = root.find('./detail/contact')
    assert contact is not None
    # The parsed attribute equals the original callsign (round-trip).
    assert contact.attrib['callsign'] == callsign


def test_xml_escapes_remarks_special_chars(amqp: mock.MagicMock) -> None:
    cot.send(
        uid='evt-r',
        type='a-f-G-U-C',
        lat=0.0,
        lon=0.0,
        remarks='5 < 7 & 7 > 5',
    )

    body = json.loads(amqp.call_args.kwargs['body'])
    root = ET.fromstring(body['cot'])
    remarks = root.find('./detail/remarks')
    assert remarks is not None
    assert remarks.text == '5 < 7 & 7 > 5'


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


def test_subscribe_returns_id_and_unsubscribe_works() -> None:
    def _handler(event: cot.CoTEvent) -> None:
        del event

    sub_id = cot.subscribe('a-f-G-U-C', _handler)
    assert isinstance(sub_id, str) and sub_id

    assert cot.unsubscribe(sub_id) is True
    # Idempotent: unsubscribing twice returns False the second time.
    assert cot.unsubscribe(sub_id) is False


def test_subscribe_rejects_non_callable_handler() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.subscribe('a-*', 'not_callable')  # type: ignore[arg-type]
    assert exc_info.value.code == 'cot.invalid_handler'


def test_subscribe_rejects_empty_pattern() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.subscribe('', lambda e: None)
    assert exc_info.value.code == 'cot.invalid_pattern'


def test_dispatch_routes_to_matching_subscribers_only() -> None:
    """C.7 hook: ensure the parked dispatcher does fire correctly so the
    bus wiring will Just Work when it lands."""

    received: list[str] = []
    other: list[str] = []

    cot.subscribe('a-f-G-U-C', lambda e: received.append(e.uid))
    cot.subscribe('b-r-f-h-*', lambda e: other.append(e.uid))

    import datetime as _dt
    evt = cot.CoTEvent(
        uid='evt-1',
        type='a-f-G-U-C',
        lat=0.0,
        lon=0.0,
        callsign='Bravo',
        timestamp=_dt.datetime.now(_dt.timezone.utc),
        sender_uid='sender',
        raw_xml='<event/>',
    )
    cot._dispatch_incoming(evt)

    assert received == ['evt-1']
    assert other == []


def test_dispatch_isolates_failing_subscriber() -> None:
    """A handler that raises must not block other subscribers."""

    def boom(_: cot.CoTEvent) -> None:
        raise RuntimeError('handler exploded')

    seen: list[str] = []
    cot.subscribe('a-*', boom)
    cot.subscribe('a-*', lambda e: seen.append(e.uid))

    import datetime as _dt
    evt = cot.CoTEvent(
        uid='evt-2',
        type='a-f-G-U-C',
        lat=0.0,
        lon=0.0,
        callsign='',
        timestamp=_dt.datetime.now(_dt.timezone.utc),
        sender_uid='',
        raw_xml='',
    )
    cot._dispatch_incoming(evt)

    assert seen == ['evt-2']


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_send_rejects_empty_uid(amqp: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.send(uid='', type='a-f-G-U-C', lat=0.0, lon=0.0)
    assert exc_info.value.code == 'cot.invalid_uid'
    amqp.assert_not_called()


def test_send_rejects_empty_type(amqp: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.send(uid='x', type='', lat=0.0, lon=0.0)
    assert exc_info.value.code == 'cot.invalid_type'
    amqp.assert_not_called()


def test_broadcast_rejects_to_uids_kwarg(amqp: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        cot.broadcast(  # type: ignore[call-arg]
            type='b-a-o-tbl', lat=0.0, lon=0.0, to_uids=['x']
        )
    assert exc_info.value.code == 'cot.broadcast_no_to_uids'
    amqp.assert_not_called()
