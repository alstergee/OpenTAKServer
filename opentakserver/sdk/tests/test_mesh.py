"""Tests for :mod:`opentakserver.sdk.modules.mesh`.

Run inside the OTS container:
    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_mesh.py
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

import pytest

from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import mesh
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


def _manifest(*, mesh_enabled: bool) -> PluginManifest:
    return PluginManifest(
        api_version=2,
        name='Mesh Test',
        slug='mesh-test',
        version='0.0.1',
        author='test',
        license='MIT',
        description='mesh test fixture',
        permissions=PluginPermissions(mesh=mesh_enabled),
        mount=[{'kind': 'tab', 'label': 'Mesh', 'path': '/mesh'}],
    )


@pytest.fixture(autouse=True)
def _reset_subscriptions() -> Iterator[None]:
    """Make every test see an empty subscription registry."""

    mesh._clear_subscriptions_for_test()
    yield
    mesh._clear_subscriptions_for_test()


@pytest.fixture
def mesh_manifest() -> PluginManifest:
    return _manifest(mesh_enabled=True)


# ---------------------------------------------------------------------------
# publish — permission gate
# ---------------------------------------------------------------------------


def test_publish_raises_permission_denied_without_mesh_scope() -> None:
    with plugin_context(_manifest(mesh_enabled=False)):
        with pytest.raises(PermissionDeniedError) as exc_info:
            mesh.publish(channel='ALLCALL', message='hi')
    assert exc_info.value.code == 'permission.mesh'


# ---------------------------------------------------------------------------
# publish — happy path: routing key + AMQP shape
# ---------------------------------------------------------------------------


def test_publish_routing_key_and_exchange(mesh_manifest: PluginManifest) -> None:
    captured: dict[str, Any] = {}

    def fake_publish(*, exchange: str, routing_key: str, body: bytes,
                     properties: Any | None = None) -> None:
        captured['exchange'] = exchange
        captured['routing_key'] = routing_key
        captured['body'] = body

    with plugin_context(mesh_manifest), \
            patch('opentakserver.amqp_publisher.publish', fake_publish):
        pkt_id = mesh.publish(channel='SECURITY', message='radio check')

    assert captured['exchange'] == 'amq.topic'
    # Default topic is 'msh' (no Flask app context pushed in this test).
    assert captured['routing_key'] == 'msh.2.e.SECURITY.outgoing'
    assert isinstance(pkt_id, str)
    assert len(pkt_id) == 8
    int(pkt_id, 16)  # parses as hex


def test_publish_uses_configured_topic(mesh_manifest: PluginManifest) -> None:
    """If a Flask app context provides ``OTS_MESHTASTIC_TOPIC`` we honour it."""

    from flask import Flask

    captured: dict[str, Any] = {}

    def fake_publish(**kwargs: Any) -> None:
        captured.update(kwargs)

    app = Flask(__name__)
    app.config['OTS_MESHTASTIC_TOPIC'] = 'festival'

    with plugin_context(mesh_manifest), \
            patch('opentakserver.amqp_publisher.publish', fake_publish), \
            app.app_context():
        mesh.publish(channel='STAFF', message='show start')

    assert captured['routing_key'] == 'festival.2.e.STAFF.outgoing'


# ---------------------------------------------------------------------------
# publish — error paths
# ---------------------------------------------------------------------------


def test_publish_unknown_channel_raises(mesh_manifest: PluginManifest) -> None:
    with plugin_context(mesh_manifest):
        with pytest.raises(OTSPluginError) as exc_info:
            mesh.publish(channel='LEAGUE_OF_VILLAINS', message='oops')
    assert exc_info.value.code == 'mesh.unknown_channel'


def test_publish_bad_hop_limit_raises(mesh_manifest: PluginManifest) -> None:
    with plugin_context(mesh_manifest):
        with pytest.raises(OTSPluginError) as exc_info:
            mesh.publish(channel='ALLCALL', message='hi', hop_limit=0)
    assert exc_info.value.code == 'mesh.bad_hop_limit'

    with plugin_context(mesh_manifest):
        with pytest.raises(OTSPluginError) as exc_info:
            mesh.publish(channel='ALLCALL', message='hi', hop_limit=8)
    assert exc_info.value.code == 'mesh.bad_hop_limit'


# ---------------------------------------------------------------------------
# publish — 5 silent-drop gates verified by decoding the published bytes
# ---------------------------------------------------------------------------


def _publish_and_decode(
    manifest: PluginManifest,
    **publish_kwargs: Any,
):
    """Publish once and return the decoded ServiceEnvelope + MeshPacket."""

    from meshtastic.protobuf import mesh_pb2, mqtt_pb2

    captured: dict[str, Any] = {}

    def fake_publish(**kwargs: Any) -> None:
        captured.update(kwargs)

    with plugin_context(manifest), \
            patch('opentakserver.amqp_publisher.publish', fake_publish):
        pkt_id = mesh.publish(**publish_kwargs)

    envelope = mqtt_pb2.ServiceEnvelope()
    envelope.ParseFromString(captured['body'])
    packet = envelope.packet
    return envelope, packet, captured, pkt_id


def test_publish_defaults_satisfy_silent_drop_gates(
    mesh_manifest: PluginManifest,
) -> None:
    envelope, packet, captured, pkt_id = _publish_and_decode(
        mesh_manifest, channel='ALLCALL', message='broadcast'
    )

    # Gate 1: routing key carries the default topic 'msh'.
    assert captured['routing_key'].startswith('msh.2.e.')

    # Gate 3: synthetic gateway id, never a real chip's id.
    assert envelope.gateway_id == '!fffe0001'

    # Gate 4: packet.from sits in a reserved range that no chip can have.
    # No from_user supplied -> anonymous sentinel 0xFFFE0001.
    packet_from = getattr(packet, 'from')
    assert packet_from == mesh.ANONYMOUS_PACKET_FROM
    # The sentinel sits in the 0xFFFE0000/16 reserved range.
    assert packet_from >> 16 == 0xFFFE

    # Gate 5: hop_start == hop_limit, both > 0 and <= 7.
    assert packet.hop_start == packet.hop_limit
    assert 1 <= packet.hop_limit <= 7
    assert packet.hop_start == 7  # default

    # ServiceEnvelope.channel_id matches the requested channel name.
    assert envelope.channel_id == 'ALLCALL'

    # MeshPacket.channel matches the index map.
    assert packet.channel == mesh.CHANNEL_INDEX_MAP['ALLCALL']

    # Default destination: broadcast.
    assert packet.to == mesh.BROADCAST_NODE_ID

    # Packet id round-trips through hex.
    assert packet.id == int(pkt_id, 16)


def test_publish_with_from_user_uses_per_user_id(
    mesh_manifest: PluginManifest,
) -> None:
    _, packet_a, _, _ = _publish_and_decode(
        mesh_manifest, channel='STAFF', message='hi', from_user='alice'
    )
    _, packet_b, _, _ = _publish_and_decode(
        mesh_manifest, channel='STAFF', message='hi', from_user='alice'
    )
    _, packet_c, _, _ = _publish_and_decode(
        mesh_manifest, channel='STAFF', message='hi', from_user='bob'
    )

    a = getattr(packet_a, 'from')
    b = getattr(packet_b, 'from')
    c = getattr(packet_c, 'from')

    # Stable per-user across calls.
    assert a == b
    # Different users → different ids (vanishingly small chance of collision,
    # but sha256(alice) vs sha256(bob) low24 actually differ — verified once).
    assert a != c
    # Both stay in the FE000000/8 reserved range.
    assert a >> 24 == 0xFE
    assert c >> 24 == 0xFE
    # And neither is the anonymous sentinel.
    assert a != mesh.ANONYMOUS_PACKET_FROM
    assert c != mesh.ANONYMOUS_PACKET_FROM


def test_publish_to_node_targets_specific_chip(
    mesh_manifest: PluginManifest,
) -> None:
    _, packet, _, _ = _publish_and_decode(
        mesh_manifest, channel='PRODUCTION', message='dm', to_node=0x12345678
    )
    assert packet.to == 0x12345678


def test_publish_hop_limit_propagates(mesh_manifest: PluginManifest) -> None:
    _, packet, _, _ = _publish_and_decode(
        mesh_manifest, channel='ALLCALL', message='1hop', hop_limit=1
    )
    assert packet.hop_start == 1
    assert packet.hop_limit == 1


def test_publish_payload_round_trips(mesh_manifest: PluginManifest) -> None:
    from meshtastic.protobuf import portnums_pb2

    _, packet, _, _ = _publish_and_decode(
        mesh_manifest, channel='ALLCALL', message='hello world'
    )
    assert packet.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP
    assert packet.decoded.payload == b'hello world'


# ---------------------------------------------------------------------------
# on_message + unsubscribe
# ---------------------------------------------------------------------------


def test_on_message_requires_mesh_scope() -> None:
    with plugin_context(_manifest(mesh_enabled=False)):
        with pytest.raises(PermissionDeniedError) as exc_info:
            mesh.on_message('ALLCALL', lambda _msg: None)
    assert exc_info.value.code == 'permission.mesh'


def test_on_message_registers_and_unsubscribe_removes(
    mesh_manifest: PluginManifest,
) -> None:
    with plugin_context(mesh_manifest):
        sub_id = mesh.on_message('ALLCALL', lambda _msg: None)

    assert isinstance(sub_id, str) and len(sub_id) == 32
    assert mesh.unsubscribe(sub_id) is True
    # Idempotent: removing twice returns False.
    assert mesh.unsubscribe(sub_id) is False


def test_on_message_unknown_channel_raises(mesh_manifest: PluginManifest) -> None:
    with plugin_context(mesh_manifest):
        with pytest.raises(OTSPluginError) as exc_info:
            mesh.on_message('NOPE', lambda _msg: None)
    assert exc_info.value.code == 'mesh.unknown_channel'


def test_dispatch_routes_only_matching_channels(
    mesh_manifest: PluginManifest,
) -> None:
    from datetime import datetime

    received: list[mesh.MeshMessage] = []

    with plugin_context(mesh_manifest):
        sec_sub = mesh.on_message('SECURITY', lambda m: received.append(m))
        wildcard_sub = mesh.on_message('*', lambda m: received.append(m))

    msg = mesh.MeshMessage(
        channel='SECURITY',
        sender_id='deadbeef',
        sender_callsign='Alpha',
        text='radio check',
        received_at=datetime.now(),
        raw_packet=b'raw',
    )
    mesh._dispatch_inbound(msg)

    # SECURITY handler + wildcard = 2 deliveries.
    assert len(received) == 2

    # Cleanup.
    mesh.unsubscribe(sec_sub)
    mesh.unsubscribe(wildcard_sub)

    received.clear()
    # Different channel: only wildcard would match — but we just unsubscribed.
    mesh._dispatch_inbound(
        mesh.MeshMessage(
            channel='STAFF',
            sender_id='cafebabe',
            sender_callsign='Bravo',
            text='hi',
            received_at=datetime.now(),
            raw_packet=b'raw',
        )
    )
    assert received == []


def test_dispatch_swallows_handler_exceptions(
    mesh_manifest: PluginManifest,
) -> None:
    from datetime import datetime

    calls: list[str] = []

    def good(_m: mesh.MeshMessage) -> None:
        calls.append('good')

    def bad(_m: mesh.MeshMessage) -> None:
        raise RuntimeError('plugin crash')

    with plugin_context(mesh_manifest):
        mesh.on_message('ALLCALL', bad)
        mesh.on_message('ALLCALL', good)

    msg = mesh.MeshMessage(
        channel='ALLCALL',
        sender_id='1',
        sender_callsign='x',
        text='t',
        received_at=datetime.now(),
        raw_packet=b'',
    )
    # Must not raise even though bad() does.
    mesh._dispatch_inbound(msg)
    # The good handler still ran.
    assert 'good' in calls
