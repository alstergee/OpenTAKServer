"""``OTS.mesh`` — Meshtastic publish/subscribe surface for v2 plugins.

A v2 plugin that declares ``mesh = true`` in its manifest can publish text
messages to a Meshtastic channel and register handlers for inbound mesh
traffic. The publish path mirrors the well-tested fan-out in
:func:`opentakserver.blueprints.ots_api.api.send_geochat` — it builds a
``ServiceEnvelope`` containing a ``MeshPacket`` carrying a ``TEXT_MESSAGE_APP``
``Data`` payload and pushes it through :mod:`opentakserver.amqp_publisher`.

The module honors the **5 silent-drop gates** from the Meshtastic firmware
(``MQTT.cpp::onReceiveProto``). Any one of these failing means the chip
silently discards the packet — no log, no error — so the gates have to be
satisfied at *build* time. They are:

1. **Topic root** must equal the chip's ``mqtt.root`` (config-driven via
   ``OTS_MESHTASTIC_TOPIC``, default ``'msh'``).
2. **Channel downlink_enabled = true** on the chip. *Chip-side flag — the
   server cannot enforce it.* Documented for completeness only.
3. **Gateway id** must NOT match any real chip's id. We use the synthetic
   ``!fffe0001`` (32-bit space ``0xFFFE0001``) which is firmly outside the
   real-chip id space.
4. **Packet ``from``** must NOT match any chip's nodenum (``isFromUs``). We
   pin the top byte to ``0xFE`` and derive the lower 24 bits from the user
   identity (or another reserved id when no user is supplied), staying out
   of real-chip id space.
5. **``hop_start`` and ``hop_limit``** both > 0 and ≤ 7. We bind them
   together: the caller passes ``hop_limit`` and we set ``hop_start`` to the
   same value, validated to ``1 <= hop_limit <= 7``.

The handler-registration side of the API stores subscriptions in a
module-level dict. Phase C.7's ``OTS.bus`` will replace the storage with a
shared event bus and a bridge from
:class:`opentakserver.controllers.meshtastic_controller.MeshtasticController`
will deliver inbound packets here.
"""

from __future__ import annotations

import hashlib
import logging
import random
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from flask import current_app

from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.permissions import requires_mesh

if TYPE_CHECKING:  # pragma: no cover — typing only
    from datetime import datetime as _dt  # noqa: F401

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants — the channel index map and the gateway id are part of the
# stable cross-component contract with both the chip's USERPREFS bake order
# and the existing api.py fan-out. Changing either silently breaks every
# plugin and the dashboard's chat panel.
# ---------------------------------------------------------------------------


CHANNEL_INDEX_MAP: dict[str, int] = {
    'ALLCALL': 1,
    'SECURITY': 2,
    'PRODUCTION': 3,
    'STAFF': 4,
    'PKI': 5,
}
"""Channel name → index. MUST match the chip's USERPREFS channel order
(0=PRIMARY, 1=ALLCALL, …). Mirrored from
:func:`opentakserver.blueprints.ots_api.api.send_geochat`."""

GATEWAY_ID: str = '!fffe0001'
"""Synthetic gateway id. Gate 3 — must never collide with a real chip id."""

ANONYMOUS_PACKET_FROM: int = 0xFFFE0001
"""``packet.from`` for unauthenticated/anonymous publishes. Sits in the
``0xFFFE0000/16`` reserved range, separate from per-user ids in
``0xFE000000/8``."""

PACKET_FROM_USER_PREFIX: int = 0xFE000000
"""High byte of ``packet.from`` for per-user publishes (gate 4).
The lower 24 bits are derived from ``sha256(user)``."""

BROADCAST_NODE_ID: int = 0xFFFFFFFF
"""``packet.to`` for broadcast (no specific destination chip)."""

MAX_HOPS: int = 7

DEFAULT_TOPIC: str = 'msh'


# ---------------------------------------------------------------------------
# Data class for inbound messages
# ---------------------------------------------------------------------------


@dataclass
class MeshMessage:
    """A decoded inbound TEXT_MESSAGE_APP packet from a mesh channel."""

    channel: str
    sender_id: str
    sender_callsign: str
    text: str
    received_at: datetime
    raw_packet: bytes = field(repr=False)


# ---------------------------------------------------------------------------
# Subscription registry. Until ``OTS.bus`` lands (C.7) this is the only place
# inbound handlers live. The C.7 bridge will iterate
# :func:`_dispatch_inbound` from MeshtasticController.
# ---------------------------------------------------------------------------


_subscriptions_lock = threading.RLock()
_subscriptions: dict[str, tuple[str, Callable[[MeshMessage], None]]] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_protobufs():
    """Import the meshtastic protobuf modules, raising a clear plugin error.

    The ``meshtastic`` package is an optional runtime dep — present in the
    OTS image, absent in some unit-test environments. Failing here lets the
    caller surface ``mesh.protobuf_missing`` to the plugin author instead
    of an opaque ``ImportError``.
    """

    try:
        from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2
    except ImportError as exc:
        raise OTSPluginError(
            code='mesh.protobuf_missing',
            message=(
                'meshtastic.protobuf is not installed; OTS.mesh.publish '
                'cannot serialize a MeshPacket. Install the meshtastic pip '
                'package in the runtime environment.'
            ),
        ) from exc
    return mesh_pb2, mqtt_pb2, portnums_pb2


def _packet_from_for_user(from_user: str | None) -> int:
    """Return the gate-4-safe ``packet.from`` for ``from_user``.

    With ``from_user=None`` we use a flat reserved id so anonymous publishes
    are still distinguishable from any chip. With a username we set the top
    byte to ``0xFE`` and derive the low 24 bits from a SHA-256 prefix —
    stable per identity, never collides with a real chip's id space.
    """

    if from_user is None:
        return ANONYMOUS_PACKET_FROM
    digest = hashlib.sha256(from_user.encode('utf-8')).hexdigest()
    low24 = int(digest[:6], 16) & 0x00FFFFFF
    return PACKET_FROM_USER_PREFIX | low24


def _resolve_topic() -> str:
    """Read ``OTS_MESHTASTIC_TOPIC`` from the active Flask app, with default.

    Falls back to :data:`DEFAULT_TOPIC` outside an app context — keeps unit
    tests that don't push an app context (and the documentation examples)
    working without surprise side effects.
    """

    try:
        return current_app.config.get('OTS_MESHTASTIC_TOPIC', DEFAULT_TOPIC)
    except RuntimeError:
        # No application context — return the documented default.
        return DEFAULT_TOPIC


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@requires_mesh()
def publish(
    *,
    channel: str,
    message: str,
    to_node: int | None = None,
    from_user: str | None = None,
    hop_limit: int = 7,
) -> str:
    """Publish a ``TEXT_MESSAGE_APP`` packet to a Meshtastic channel.

    Returns the protobuf packet id as an 8-char lower-hex string (uint32).

    Parameters
    ----------
    channel:
        One of ``ALLCALL``, ``SECURITY``, ``PRODUCTION``, ``STAFF``, ``PKI``.
        Anything else raises :class:`OTSPluginError` with code
        ``mesh.unknown_channel``.
    message:
        UTF-8 text to deliver. Truncation/MTU is the chip's job — we publish
        whatever the caller sent.
    to_node:
        Destination chip nodenum. ``None`` (default) broadcasts to every
        chip on the channel.
    from_user:
        Identity to encode into ``packet.from``. ``None`` uses the
        anonymous id reserved at :data:`ANONYMOUS_PACKET_FROM`.
    hop_limit:
        Max hops, ``1 <= hop_limit <= 7``. ``hop_start`` is set to the same
        value (gate 5 demands both > 0 and ≤ 7).

    Required scope: ``mesh = true`` in the active plugin manifest.
    """

    if channel not in CHANNEL_INDEX_MAP:
        raise OTSPluginError(
            code='mesh.unknown_channel',
            message=(
                f'channel {channel!r} is not a valid Meshtastic channel. '
                f'Valid: {sorted(CHANNEL_INDEX_MAP)}.'
            ),
        )
    if not 1 <= hop_limit <= MAX_HOPS:
        raise OTSPluginError(
            code='mesh.bad_hop_limit',
            message=(
                f'hop_limit must be 1..{MAX_HOPS} (inclusive), got {hop_limit}. '
                'Gate 5 of the Meshtastic firmware drops anything outside.'
            ),
        )

    mesh_pb2, mqtt_pb2, portnums_pb2 = _load_protobufs()

    topic = _resolve_topic()
    channel_index = CHANNEL_INDEX_MAP[channel]
    packet_from = _packet_from_for_user(from_user)
    target = BROADCAST_NODE_ID if to_node is None else int(to_node)
    pkt_id = random.randint(1, 0xFFFFFFFF)

    pb_data = mesh_pb2.Data()
    pb_data.portnum = portnums_pb2.TEXT_MESSAGE_APP
    pb_data.payload = message.encode('utf-8')

    mesh_packet = mesh_pb2.MeshPacket()
    mesh_packet.decoded.CopyFrom(pb_data)
    mesh_packet.to = target
    mesh_packet.want_ack = False
    mesh_packet.id = pkt_id
    # 'from' is a Python reserved word — protobuf allows setattr access.
    setattr(mesh_packet, 'from', packet_from)
    mesh_packet.channel = channel_index
    mesh_packet.hop_start = hop_limit
    mesh_packet.hop_limit = hop_limit

    service_envelope = mqtt_pb2.ServiceEnvelope()
    service_envelope.packet.CopyFrom(mesh_packet)
    service_envelope.channel_id = channel
    service_envelope.gateway_id = GATEWAY_ID

    routing_key = f'{topic}.2.e.{channel}.outgoing'
    body = service_envelope.SerializeToString()

    # Imported lazily so a test environment without amqp-publisher import
    # side effects (or with a stub) still works.
    from opentakserver import amqp_publisher

    amqp_publisher.publish(
        exchange='amq.topic',
        routing_key=routing_key,
        body=body,
    )

    pkt_id_hex = f'{pkt_id:08x}'
    logger.info(
        'OTS.mesh.publish channel=%s routing_key=%s id=%s',
        channel,
        routing_key,
        pkt_id_hex,
        extra={'plugin': 'sdk.mesh'},
    )
    return pkt_id_hex


@requires_mesh()
def on_message(
    channel: str | Literal['*'],
    handler: Callable[[MeshMessage], None],
) -> str:
    """Register ``handler`` for inbound messages on ``channel`` (or ``'*'``).

    Returns a subscription id usable with :func:`unsubscribe`. The handler
    is called synchronously from whichever thread delivers the message —
    keep it short or hand off to a queue.

    Required scope: ``mesh = true``.
    """

    if channel != '*' and channel not in CHANNEL_INDEX_MAP:
        raise OTSPluginError(
            code='mesh.unknown_channel',
            message=(
                f'channel {channel!r} is not a valid Meshtastic channel. '
                f"Valid: {sorted(CHANNEL_INDEX_MAP)} or '*'."
            ),
        )
    if not callable(handler):
        raise OTSPluginError(
            code='mesh.bad_handler',
            message='handler must be callable',
        )

    sub_id = uuid.uuid4().hex
    with _subscriptions_lock:
        _subscriptions[sub_id] = (channel, handler)
    logger.debug(
        'OTS.mesh.on_message channel=%s sub_id=%s',
        channel,
        sub_id,
        extra={'plugin': 'sdk.mesh'},
    )
    return sub_id


def unsubscribe(subscription_id: str) -> bool:
    """Remove a handler previously registered via :func:`on_message`.

    Returns ``True`` if the subscription existed and was removed,
    ``False`` if no such id was registered.
    """

    with _subscriptions_lock:
        return _subscriptions.pop(subscription_id, None) is not None


def _dispatch_inbound(message: MeshMessage) -> None:
    """Dispatch ``message`` to every matching handler.

    Internal — :class:`MeshtasticController` will call this once C.7 lands
    the bridge. Errors raised by individual handlers are logged and
    swallowed so one misbehaving plugin can't poison the dispatch loop.
    """

    with _subscriptions_lock:
        snapshot = list(_subscriptions.items())

    for sub_id, (channel, handler) in snapshot:
        if channel != '*' and channel != message.channel:
            continue
        try:
            handler(message)
        except Exception:
            logger.exception(
                'OTS.mesh handler raised; subscription continues',
                extra={'plugin': 'sdk.mesh', 'sub_id': sub_id},
            )


def _clear_subscriptions_for_test() -> None:
    """Test-only helper. Production code never calls this."""

    with _subscriptions_lock:
        _subscriptions.clear()


__all__ = [
    'ANONYMOUS_PACKET_FROM',
    'BROADCAST_NODE_ID',
    'CHANNEL_INDEX_MAP',
    'GATEWAY_ID',
    'MAX_HOPS',
    'MeshMessage',
    'PACKET_FROM_USER_PREFIX',
    'on_message',
    'publish',
    'unsubscribe',
]
