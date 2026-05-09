"""``OTS.cot`` — Cursor on Target (CoT) helpers for v2 plugins.

Plugins import this module via ``from opentakserver.sdk.modules import cot``
(or, once :mod:`opentakserver.sdk.ots_namespace` lands in C.7, via
``OTS.cot``). Helpers here wrap the same RabbitMQ exchanges that
``cot_parser`` and the geochat blueprint already publish to, so a CoT
emitted via :func:`cot.send` is indistinguishable from one emitted by
core OTS code.

Three helpers
-------------

* :func:`send` — build a CoT XML event and publish it. Default routes to
  the ``cot_controller`` fan-out exchange (broadcast); pass
  ``to_uids=[uid, ...]`` to direct-message specific EUDs via the ``dms``
  exchange.
* :func:`broadcast` — convenience wrapper, ``send(to_uids='broadcast')``.
* :func:`subscribe` / :func:`unsubscribe` — register a glob-matching
  handler for incoming CoT events. Until the in-process event bus
  (:mod:`opentakserver.sdk.modules.bus`, Phase C.7) exists, handlers are
  parked in a module-level dict and never fire — see
  :data:`_DEPENDS_ON_C7` and the architecture map's "Open architectural
  decisions" section. Subscriptions still validate scope and return a
  stable subscription id, so plugin code written against this surface
  today will keep working when C.7 ships.

The XML builder mirrors the shape used by
:func:`opentakserver.blueprints.ots_api.api.send_geochat`. We did not
extract that function — it owns DB/Point/Chatroom persistence that has
nothing to do with the SDK surface — but :func:`_build_cot_xml` produces
a structurally equivalent ``<event><point/><detail/></event>`` document
so downstream consumers (cot_parser, ATAK clients) treat plugin-emitted
CoTs identically to web-UI ones.

Permissions
-----------

* :func:`send` and :func:`broadcast` are gated by
  ``permissions.write = ["cot"]``. Bare core callers (no plugin context)
  pass through — see :func:`opentakserver.sdk.permissions.current_plugin`.
* :func:`subscribe` is gated by ``permissions.read = ["cot"]``.
"""

from __future__ import annotations

import datetime as _dt
import fnmatch
import json
import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal
from xml.sax.saxutils import escape as xml_escape, quoteattr

from opentakserver.amqp_publisher import publish as _amqp_publish
from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.permissions import (
    current_plugin,
    requires_read,
    requires_write,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class CoTEvent:
    """A parsed inbound CoT event handed to subscribers.

    Constructed by the (future) event-bus dispatcher when a CoT lands on
    the server. Plugins should treat instances as read-only.
    """

    uid: str
    type: str
    lat: float
    lon: float
    callsign: str
    timestamp: _dt.datetime
    sender_uid: str
    raw_xml: str


# ---------------------------------------------------------------------------
# Subscription registry — parked until OTS.bus (C.7)
# ---------------------------------------------------------------------------


_DEPENDS_ON_C7 = (
    'cot.subscribe currently parks handlers in a module-level dict. When '
    'OTS.bus.on (Phase C.7) lands, _dispatch_incoming will be wired into '
    'the bus so subscriptions actually fire. Tracked in '
    '/docker/opentak/.claude/skills/ots-plugin-architecture/SKILL.md '
    'under "Open architectural decisions" as D-2.'
)


_subscriptions_lock = threading.Lock()
_subscriptions: dict[str, tuple[str, Callable[[CoTEvent], None]]] = {}


def _dispatch_incoming(event: CoTEvent) -> None:
    """Fan an incoming CoT out to every matching subscriber.

    Called by the bus once C.7 is wired. Until then this stays unused —
    keeping it here so the wiring is a one-line bus-side hook, not a
    rewrite.
    """

    # Snapshot under the lock so handlers may unsubscribe themselves.
    with _subscriptions_lock:
        snapshot = list(_subscriptions.items())

    for sub_id, (pattern, handler) in snapshot:
        if not fnmatch.fnmatchcase(event.type, pattern):
            continue
        try:
            handler(event)
        except Exception:  # noqa: BLE001 — handler is plugin code; isolate failures
            logger.exception(
                'cot subscriber raised; continuing with remaining subscribers',
                extra={'subscription_id': sub_id, 'pattern': pattern},
            )


# ---------------------------------------------------------------------------
# XML builder
# ---------------------------------------------------------------------------


_DEFAULT_STALE = _dt.timedelta(minutes=5)


def _utc_now() -> _dt.datetime:
    """Centralised so tests can monkeypatch the clock if needed."""

    return _dt.datetime.now(_dt.timezone.utc)


def _fmt_cot_time(value: _dt.datetime) -> str:
    """ATAK accepts both Z-suffix and offset-form; the rest of OTS uses Z."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)
    return value.astimezone(_dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _build_cot_xml(
    *,
    uid: str,
    type: str,  # noqa: A002 — public CoT field name
    lat: float,
    lon: float,
    callsign: str = '',
    hae: float = 0.0,
    ce: float = 9999999,
    le: float = 9999999,
    how: str = 'h-g-i-g-o',
    stale: _dt.timedelta = _DEFAULT_STALE,
    remarks: str = '',
    now: _dt.datetime | None = None,
) -> str:
    """Render a complete ``<event>…</event>`` CoT XML document.

    Mirrors the shape used in
    :func:`opentakserver.blueprints.ots_api.api.send_geochat`. All
    user-supplied strings are XML-escaped (``xml.sax.saxutils.escape``
    for text nodes; :func:`xml.sax.saxutils.quoteattr` for attribute
    values) so a callsign like ``"Bravo & Charlie"`` round-trips intact.
    """

    if not uid:
        raise OTSPluginError(
            code='cot.invalid_uid', message='cot.send: uid must be non-empty'
        )
    if not type:
        raise OTSPluginError(
            code='cot.invalid_type', message='cot.send: type must be non-empty'
        )

    moment = now or _utc_now()
    time_str = _fmt_cot_time(moment)
    stale_str = _fmt_cot_time(moment + stale)

    # Attributes: use quoteattr so quotes embedded in user values are
    # safely escaped (quoteattr returns the string with surrounding
    # quotes already in place).
    a_uid = quoteattr(uid)
    a_type = quoteattr(type)
    a_how = quoteattr(how)
    a_time = quoteattr(time_str)
    a_stale = quoteattr(stale_str)

    # Numeric attributes — formatted from floats so we never embed a user
    # string. ``repr`` would give us scientific notation in some edge
    # cases; ``str`` is fine for the precision ATAK consumes.
    point_attrs = (
        f'lat="{lat}" lon="{lon}" hae="{hae}" ce="{ce}" le="{le}"'
    )

    # Detail block
    detail_parts: list[str] = []
    if callsign:
        detail_parts.append(f'<contact callsign={quoteattr(callsign)}/>')
    if remarks:
        detail_parts.append(
            f'<remarks>{xml_escape(remarks)}</remarks>'
        )
    detail = ''.join(detail_parts)

    return (
        f'<event version="2.0" uid={a_uid} type={a_type} how={a_how} '
        f'time={a_time} start={a_time} stale={a_stale}>'
        f'<point {point_attrs}/>'
        f'<detail>{detail}</detail>'
        f'</event>'
    )


# ---------------------------------------------------------------------------
# Publish helpers
# ---------------------------------------------------------------------------


def _publish(*, exchange: str, routing_key: str, body: str) -> None:
    """Thin wrapper so tests can patch a single seam."""

    _amqp_publish(exchange=exchange, routing_key=routing_key, body=body)


def _sender_uid_for_context() -> str:
    """Pick a stable ``uid`` for the JSON envelope's outer ``uid`` field.

    Inside a plugin call, prefer the plugin slug (so cot_parser logs and
    audit trails attribute the message to the plugin, not to ``server``).
    Outside any plugin context, fall back to the conventional ``server``
    sender used by other server-originated CoTs.
    """

    manifest = current_plugin()
    if manifest is not None:
        return f'plugin:{manifest.slug}'
    return 'server'


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@requires_write('cot')
def send(
    *,
    uid: str,
    type: str,  # noqa: A002 — public CoT field name
    lat: float,
    lon: float,
    callsign: str = '',
    hae: float = 0.0,
    ce: float = 9999999,
    le: float = 9999999,
    to_uids: list[str] | Literal['broadcast'] = 'broadcast',
    stale: _dt.timedelta = _DEFAULT_STALE,
    remarks: str = '',
    how: str = 'h-g-i-g-o',
) -> str:
    """Build a CoT XML event and publish it to AMQP.

    Returns the event's ``uid`` (the same value passed in, after
    validation). Requires ``write = ["cot"]`` when called from a plugin;
    bare core callers bypass the scope check (``current_plugin()`` is
    ``None``).

    Routing
    -------
    * ``to_uids='broadcast'`` (default): publish once to
      ``exchange='cot_controller'`` with empty routing key — the same
      fan-out OTS uses for any server-originated CoT.
    * ``to_uids=[uid1, uid2, ...]``: publish once per uid to
      ``exchange='dms'`` with ``routing_key=uid``.

    The published body is a JSON envelope ``{"uid": <sender>, "cot":
    <xml>}`` matching the shape ``send_geochat`` and ``cot_parser``
    publish.
    """

    xml = _build_cot_xml(
        uid=uid,
        type=type,
        lat=lat,
        lon=lon,
        callsign=callsign,
        hae=hae,
        ce=ce,
        le=le,
        how=how,
        stale=stale,
        remarks=remarks,
    )
    sender = _sender_uid_for_context()
    body = json.dumps({'uid': sender, 'cot': xml})

    if to_uids == 'broadcast':
        _publish(exchange='cot_controller', routing_key='', body=body)
        logger.info(
            'cot.send broadcast',
            extra={'uid': uid, 'cot_type': type, 'sender': sender},
        )
        return uid

    if not isinstance(to_uids, list) or not to_uids:
        raise OTSPluginError(
            code='cot.invalid_to_uids',
            message=(
                'cot.send: to_uids must be the literal "broadcast" or a '
                'non-empty list of uid strings'
            ),
        )

    for target in to_uids:
        if not isinstance(target, str) or not target:
            raise OTSPluginError(
                code='cot.invalid_to_uids',
                message=f'cot.send: to_uids contains non-string entry: {target!r}',
            )
        _publish(exchange='dms', routing_key=target, body=body)
    logger.info(
        'cot.send dm',
        extra={
            'uid': uid,
            'cot_type': type,
            'sender': sender,
            'to_uids': to_uids,
        },
    )
    return uid


@requires_write('cot')
def broadcast(
    *,
    type: str,  # noqa: A002 — public CoT field name
    lat: float,
    lon: float,
    uid: str | None = None,
    **kwargs: Any,
) -> str:
    """Convenience wrapper: ``send(to_uids='broadcast', ...)``.

    Auto-generates ``uid`` (a UUID4) if the caller didn't supply one —
    most broadcast events are transient alerts that don't need a stable
    id. The decorator on :func:`send` runs again, but the scope check is
    idempotent and the second pass is harmless.
    """

    if uid is None:
        uid = str(uuid.uuid4())
    # Strip to_uids if a caller passed it through **kwargs — broadcast()
    # is broadcast-only by definition, and silently dropping it would
    # hide bugs.
    if 'to_uids' in kwargs:
        raise OTSPluginError(
            code='cot.broadcast_no_to_uids',
            message=(
                'cot.broadcast does not accept to_uids; use cot.send for '
                'directed messages'
            ),
        )
    return send(
        uid=uid,
        type=type,
        lat=lat,
        lon=lon,
        to_uids='broadcast',
        **kwargs,
    )


@requires_read('cot')
def subscribe(
    type_pattern: str,
    handler: Callable[[CoTEvent], None],
) -> str:
    """Register ``handler`` for incoming CoT whose ``type`` matches the glob.

    ``type_pattern`` follows :mod:`fnmatch` rules — e.g. ``'a-f-G-U-C'``
    for an exact match, ``'b-r-f-h-*'`` for "any reporter type starting
    b-r-f-h-". Returns a subscription id usable with :func:`unsubscribe`.

    See :data:`_DEPENDS_ON_C7` for the temporary parking behaviour.
    """

    if not callable(handler):
        raise OTSPluginError(
            code='cot.invalid_handler',
            message='cot.subscribe: handler must be callable',
        )
    if not isinstance(type_pattern, str) or not type_pattern:
        raise OTSPluginError(
            code='cot.invalid_pattern',
            message='cot.subscribe: type_pattern must be a non-empty string',
        )

    sub_id = str(uuid.uuid4())
    with _subscriptions_lock:
        _subscriptions[sub_id] = (type_pattern, handler)

    manifest = current_plugin()
    logger.info(
        'cot.subscribe registered',
        extra={
            'plugin': manifest.slug if manifest is not None else None,
            'pattern': type_pattern,
            'subscription_id': sub_id,
        },
    )
    return sub_id


def unsubscribe(subscription_id: str) -> bool:
    """Remove a subscription previously registered via :func:`subscribe`.

    Returns ``True`` if the subscription existed, ``False`` otherwise.
    Intentionally not gated by a permission scope — releasing a resource
    you already hold should never fail closed.
    """

    with _subscriptions_lock:
        return _subscriptions.pop(subscription_id, None) is not None


__all__ = [
    'CoTEvent',
    'broadcast',
    'send',
    'subscribe',
    'unsubscribe',
]
