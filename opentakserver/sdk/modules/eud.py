"""``OTS.eud`` — End-User Device (EUD) helpers for v2 plugins.

Plugins import via ``from opentakserver.sdk.modules import eud`` (or, once
:mod:`opentakserver.sdk.ots_namespace` lands, via ``OTS.eud``).

Surface
-------

* :func:`online` — list EUDs that have sent a CoT recently. Backed directly
  by the ``euds`` table — no caching layer between the SDK and the DB so a
  plugin always sees the same picture as the dashboard.
* :func:`find` — look up an EUD by ``uid`` xor ``callsign``.
* :func:`send_to` — direct-message a single EUD by publishing to the
  ``dms`` AMQP exchange (the same exchange that the EUD's per-uid queue is
  bound to in :mod:`opentakserver.eud_handler.EudHandler`).
* :func:`geofence` / :func:`ungeofence` — register a polygonal geofence
  whose ``on_enter`` / ``on_exit`` callbacks fire when an EUD's position
  crosses the polygon boundary. Until ``OTS.bus.on('eud.position')``
  exists (Phase C.7), incoming positions are not routed in — see
  :data:`_DEPENDS_ON_C7`. Subscriptions still validate inputs and return
  a stable id so plugin code written against this surface today keeps
  working when C.7 ships. Plugin tests can drive
  :func:`_handle_position_update` directly to exercise the polygon logic.

Permissions
-----------

* :func:`online` and :func:`find` require ``read = ["eud"]``.
* :func:`send_to` requires ``write = ["cot"]`` — it publishes a CoT.
* :func:`geofence` / :func:`ungeofence` are *not* gated. Both only listen
  to position events; per the spec, no scope check is needed for now.

Point-in-polygon
----------------

The boundary check uses the standard ray-casting algorithm. We deliberately
avoid pulling in ``shapely`` (~30 MB of C deps) for what is a 25-line
helper. ``matplotlib.path.Path`` is similarly heavy. See
:func:`_point_in_polygon` for the implementation.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from opentakserver.amqp_publisher import publish as _amqp_publish
from opentakserver.extensions import db
from opentakserver.models.EUD import EUD
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


@dataclass(frozen=True)
class EudInfo:
    """Snapshot of an :class:`opentakserver.models.EUD.EUD` row.

    Frozen because plugin code should treat it as a read-only view.
    Values are copied off the ORM object so the snapshot remains valid
    after the calling session has closed.
    """

    uid: str
    callsign: str
    last_lat: float
    last_lon: float
    last_event_time: datetime | None
    last_status: str
    device: str
    platform: str
    os: str  # noqa: A003 — public field name on the EUD record
    version: str


def _as_eud_info(row: EUD, *, lat: float = 0.0, lon: float = 0.0) -> EudInfo:
    """Convert an ORM ``EUD`` row to the public :class:`EudInfo` snapshot.

    The EUD model stores positions on the ``points`` relationship, not on
    the row itself. Callers that have a Point in hand pass ``lat`` /
    ``lon`` explicitly; everything else gets ``0.0`` (the dashboard does
    the same for EUDs whose last point has been pruned).
    """

    return EudInfo(
        uid=row.uid or '',
        callsign=row.callsign or '',
        last_lat=float(lat),
        last_lon=float(lon),
        last_event_time=row.last_event_time,
        last_status=row.last_status or '',
        device=row.device or '',
        platform=row.platform or '',
        os=row.os or '',
        version=row.version or '',
    )


# ---------------------------------------------------------------------------
# online / find
# ---------------------------------------------------------------------------


_DEFAULT_WINDOW = timedelta(minutes=5)


def _utc_now() -> datetime:
    """Centralised so tests can monkeypatch the clock if needed."""

    return datetime.now(timezone.utc)


@requires_read('eud')
def online(within: timedelta | None = _DEFAULT_WINDOW) -> list[EudInfo]:
    """Return EUDs that have produced a CoT within ``within``.

    ``within=None`` lifts the recency filter and returns every EUD on
    record. ``within`` is interpreted relative to *now* in UTC; the
    ``EUD.last_event_time`` column is naive UTC so we strip the tzinfo
    on the cutoff before comparing.

    Returns an empty list if no EUD matches.
    """

    query = db.session.query(EUD)
    if within is not None:
        if not isinstance(within, timedelta):
            raise OTSPluginError(
                code='eud.invalid_within',
                message='eud.online: within must be a timedelta or None',
            )
        # ``last_event_time`` is stored naive (UTC) so we hand SQLAlchemy
        # a naive cutoff. Round-tripping through .replace(tzinfo=None)
        # avoids "can't compare aware to naive" errors on engines that
        # honour tz-awareness (e.g. PostgreSQL with TIMESTAMP WITHOUT TZ).
        cutoff = (_utc_now() - within).replace(tzinfo=None)
        query = query.filter(EUD.last_event_time.isnot(None)).filter(
            EUD.last_event_time >= cutoff
        )

    rows = query.all()
    return [_as_eud_info(row) for row in rows]


@requires_read('eud')
def find(
    *,
    uid: str | None = None,
    callsign: str | None = None,
) -> EudInfo | None:
    """Look up an EUD by ``uid`` or ``callsign`` (exactly one).

    Raises :class:`ValueError` if both arguments are given or both are
    ``None``. Returns ``None`` if no row matches.
    """

    if (uid is None) == (callsign is None):
        raise ValueError(
            'eud.find: pass exactly one of uid= or callsign=, not both/neither'
        )

    query = db.session.query(EUD)
    if uid is not None:
        if not isinstance(uid, str) or not uid:
            raise OTSPluginError(
                code='eud.invalid_uid',
                message='eud.find: uid must be a non-empty string',
            )
        query = query.filter(EUD.uid == uid)
    else:
        if not isinstance(callsign, str) or not callsign:
            raise OTSPluginError(
                code='eud.invalid_callsign',
                message='eud.find: callsign must be a non-empty string',
            )
        query = query.filter(EUD.callsign == callsign)

    row = query.one_or_none()
    if row is None:
        return None
    return _as_eud_info(row)


# ---------------------------------------------------------------------------
# send_to
# ---------------------------------------------------------------------------


def _publish_dm(*, routing_key: str, body: str) -> None:
    """Thin wrapper so tests can patch a single seam (mirrors cot._publish)."""

    _amqp_publish(exchange='dms', routing_key=routing_key, body=body)


def _sender_uid_for_context() -> str:
    """Pick the JSON envelope's outer ``uid`` field for an outgoing DM.

    Mirrors :func:`opentakserver.sdk.modules.cot._sender_uid_for_context`.
    """

    manifest = current_plugin()
    if manifest is not None:
        return f'plugin:{manifest.slug}'
    return 'server'


@requires_write('cot')
def send_to(uid: str, cot_event_xml: str) -> bool:
    """Direct-message a single EUD via the ``dms`` exchange.

    The body is the same JSON envelope the rest of OTS publishes —
    ``{"uid": <sender>, "cot": <xml>}`` — so cot_parser and the EUD's
    queue handle the message identically to a server-originated DM.

    Returns ``True`` if the publish call returned without raising.
    Propagates AMQP errors out of :func:`opentakserver.amqp_publisher.publish`
    after its retry budget is exhausted; callers can catch them or let
    them bubble.
    """

    if not isinstance(uid, str) or not uid:
        raise OTSPluginError(
            code='eud.invalid_uid',
            message='eud.send_to: uid must be a non-empty string',
        )
    if not isinstance(cot_event_xml, str) or not cot_event_xml.strip():
        raise OTSPluginError(
            code='eud.invalid_cot_xml',
            message='eud.send_to: cot_event_xml must be a non-empty string',
        )

    body = json.dumps({'uid': _sender_uid_for_context(), 'cot': cot_event_xml})
    _publish_dm(routing_key=uid, body=body)
    logger.info(
        'eud.send_to dm',
        extra={'to_uid': uid, 'sender': _sender_uid_for_context()},
    )
    return True


# ---------------------------------------------------------------------------
# Geofence registry — parked until OTS.bus (C.7)
# ---------------------------------------------------------------------------


_DEPENDS_ON_C7 = (
    'eud.geofence currently parks callbacks in a module-level dict. When '
    'OTS.bus.on(\'eud.position\') (Phase C.7) lands, _handle_position_update '
    'will be wired into the bus so geofence callbacks actually fire on live '
    'EUD movement. Tracked in '
    '/docker/opentak/.claude/skills/ots-plugin-architecture/SKILL.md '
    'under "Open architectural decisions" as D-2.'
)


_geofences_lock = threading.Lock()
# geofence_id -> {name, polygon, on_enter, on_exit, inside (uid -> bool)}
_geofences: dict[str, dict] = {}


def _validate_polygon(polygon: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Validate the polygon vertices and return a normalised tuple list.

    Accepts any iterable of 2-element sequences (tuples or lists) and
    normalises to ``list[tuple[float, float]]``. Raises
    :class:`OTSPluginError` on bad shape or fewer than 3 vertices.
    """

    if not isinstance(polygon, (list, tuple)):
        raise OTSPluginError(
            code='eud.invalid_polygon',
            message='eud.geofence: polygon must be a list of (lat, lon) pairs',
        )
    if len(polygon) < 3:
        raise OTSPluginError(
            code='eud.invalid_polygon',
            message='eud.geofence: polygon needs at least 3 vertices',
        )
    out: list[tuple[float, float]] = []
    for i, vertex in enumerate(polygon):
        if not isinstance(vertex, (list, tuple)) or len(vertex) != 2:
            raise OTSPluginError(
                code='eud.invalid_polygon',
                message=(
                    f'eud.geofence: vertex {i} must be a (lat, lon) pair, '
                    f'got {vertex!r}'
                ),
            )
        try:
            lat = float(vertex[0])
            lon = float(vertex[1])
        except (TypeError, ValueError) as exc:
            raise OTSPluginError(
                code='eud.invalid_polygon',
                message=(
                    f'eud.geofence: vertex {i} contains non-numeric value '
                    f'{vertex!r}'
                ),
            ) from exc
        out.append((lat, lon))
    return out


def _point_in_polygon(
    lat: float, lon: float, polygon: list[tuple[float, float]]
) -> bool:
    """Ray-casting point-in-polygon test.

    Polygon vertices are ``(lat, lon)`` pairs. The algorithm casts a
    horizontal ray east from the point and counts intersections with
    polygon edges; an odd count means inside.

    For the small polygons a geofence describes (a tent, a perimeter, a
    festival footprint), treating lat/lon as planar coordinates is
    accurate enough — the curvature error over a few-km box is sub-metre.
    Plugins that need geodesic accuracy can build their own check.
    """

    n = len(polygon)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        lat_i, lon_i = polygon[i]
        lat_j, lon_j = polygon[j]
        # Edge crosses the horizontal line y = lat?
        crosses = (lat_i > lat) != (lat_j > lat)
        if crosses:
            # x-coordinate of intersection of edge with y = lat
            slope = (lon_j - lon_i) / (lat_j - lat_i) if lat_j != lat_i else 0.0
            x_intersect = lon_i + (lat - lat_i) * slope
            if lon < x_intersect:
                inside = not inside
        j = i
    return inside


def geofence(
    name: str,
    polygon: list[tuple[float, float]],
    on_enter: Callable[[EudInfo], None] | None = None,
    on_exit: Callable[[EudInfo], None] | None = None,
) -> str:
    """Register a polygonal geofence; return the geofence id.

    ``polygon`` is a list of at least 3 ``(lat, lon)`` pairs. Either or
    both callbacks may be ``None`` (a geofence with no callbacks is a
    no-op but still registered, which is occasionally useful for
    diagnostics or to reserve an id before wiring up callbacks).

    The returned id is the argument to :func:`ungeofence`.

    See :data:`_DEPENDS_ON_C7` for the temporary parking behaviour: the
    callbacks won't fire on live traffic until C.7 wires the bus, but
    :func:`_handle_position_update` can be driven directly from tests.
    """

    if not isinstance(name, str) or not name:
        raise OTSPluginError(
            code='eud.invalid_geofence_name',
            message='eud.geofence: name must be a non-empty string',
        )
    normalised = _validate_polygon(polygon)
    if on_enter is not None and not callable(on_enter):
        raise OTSPluginError(
            code='eud.invalid_callback',
            message='eud.geofence: on_enter must be callable or None',
        )
    if on_exit is not None and not callable(on_exit):
        raise OTSPluginError(
            code='eud.invalid_callback',
            message='eud.geofence: on_exit must be callable or None',
        )

    geofence_id = str(uuid.uuid4())
    with _geofences_lock:
        _geofences[geofence_id] = {
            'name': name,
            'polygon': normalised,
            'on_enter': on_enter,
            'on_exit': on_exit,
            'inside': {},  # uid -> bool
        }

    manifest = current_plugin()
    logger.info(
        'eud.geofence registered',
        extra={
            'plugin': manifest.slug if manifest is not None else None,
            'name': name,
            'geofence_id': geofence_id,
            'vertices': len(normalised),
        },
    )
    return geofence_id


def ungeofence(geofence_id: str) -> bool:
    """Remove a geofence previously registered via :func:`geofence`.

    Returns ``True`` if a geofence with that id existed, ``False``
    otherwise. Releasing a geofence you already hold should never fail
    closed, so this is intentionally not gated by a permission scope.
    """

    with _geofences_lock:
        return _geofences.pop(geofence_id, None) is not None


def _handle_position_update(info: EudInfo) -> None:
    """Dispatch a position update through every registered geofence.

    Called by the C.7 bus once it lands (see :data:`_DEPENDS_ON_C7`).
    Tests drive this function directly to exercise the polygon logic
    without an event bus.

    Per-geofence behaviour:

    * If the EUD is now inside the polygon and was *not* inside on the
      previous update, ``on_enter`` fires.
    * If the EUD is now outside and *was* inside, ``on_exit`` fires.
    * Otherwise (no transition), nothing fires.

    Handler exceptions are logged and swallowed — one bad plugin
    callback must not block other geofences.
    """

    with _geofences_lock:
        snapshot = list(_geofences.items())

    for gf_id, gf in snapshot:
        try:
            inside_now = _point_in_polygon(
                info.last_lat, info.last_lon, gf['polygon']
            )
        except Exception:  # noqa: BLE001 — algorithm bug, not handler bug
            logger.exception(
                'eud.geofence point-in-polygon failed',
                extra={'geofence_id': gf_id},
            )
            continue

        was_inside = gf['inside'].get(info.uid, False)
        gf['inside'][info.uid] = inside_now

        if inside_now and not was_inside and gf['on_enter'] is not None:
            try:
                gf['on_enter'](info)
            except Exception:  # noqa: BLE001 — handler is plugin code
                logger.exception(
                    'eud.geofence on_enter handler raised',
                    extra={'geofence_id': gf_id, 'uid': info.uid},
                )
        elif (
            (not inside_now) and was_inside and gf['on_exit'] is not None
        ):
            try:
                gf['on_exit'](info)
            except Exception:  # noqa: BLE001 — handler is plugin code
                logger.exception(
                    'eud.geofence on_exit handler raised',
                    extra={'geofence_id': gf_id, 'uid': info.uid},
                )


__all__ = [
    'EudInfo',
    'find',
    'geofence',
    'online',
    'send_to',
    'ungeofence',
]
