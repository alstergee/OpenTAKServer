"""Tests for :mod:`opentakserver.sdk.modules.eud`.

Run inside the OTS container:

    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_eud.py

The DB-backed tests build a minimal Flask app with an in-memory SQLite
database. We import every model under :mod:`opentakserver.models` so
SQLAlchemy can resolve the cross-table relationships before
``db.create_all()`` runs (the ORM otherwise fails resolving e.g.
``CasEvac.zmist`` -> ``ZMIST`` if those modules are not yet loaded).
We do *not* spin up the full :func:`opentakserver.app.create_app` —
that path needs RabbitMQ, the migrations, the scheduler, etc., which
are out of scope for an SDK unit test.
"""

from __future__ import annotations

import contextlib
import importlib
import json
import pkgutil
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from flask import Flask
from flask_security.models import fsqla_v3 as fsqla

from opentakserver.extensions import db
from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import eud as eud_mod
from opentakserver.sdk.modules.eud import (
    EudInfo,
    _geofences,
    _geofences_lock,
    _point_in_polygon,
    find,
    geofence,
    online,
    send_to,
    ungeofence,
)
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Manifest helper (reused across permission-scope tests)
# ---------------------------------------------------------------------------


def _manifest(
    *,
    read: list[str] | None = None,
    write: list[str] | None = None,
) -> PluginManifest:
    return PluginManifest(
        api_version=2,
        name='Test',
        slug='test-plugin',
        version='0.0.0',
        author='test',
        license='MIT',
        description='test fixture',
        permissions=PluginPermissions(
            read=read or [],
            write=write or [],
            mesh=False,
            mission='none',
        ),
        mount=[{'kind': 'tab', 'label': 'Test', 'path': '/test'}],
    )


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


# ---------------------------------------------------------------------------
# Flask + SQLite fixtures
# ---------------------------------------------------------------------------


_models_imported = False


def _import_all_models() -> None:
    """Import every module under :mod:`opentakserver.models`.

    SQLAlchemy's declarative registry only resolves string-form
    relationship targets after the referenced class is defined, so we
    walk the package once before ``db.create_all`` runs. Idempotent —
    repeated calls are no-ops because each model module declares its
    table on the shared ``db.metadata`` and re-importing would trigger
    "Table already defined" errors via :func:`fsqla.FsModels.set_db_info`.
    """

    global _models_imported
    if _models_imported:
        return
    fsqla.FsModels.set_db_info(db)
    import opentakserver.models as models_pkg

    for _, name, _ in pkgutil.iter_modules(models_pkg.__path__):
        importlib.import_module(f'opentakserver.models.{name}')
    _models_imported = True


@pytest.fixture
def app() -> Flask:
    """Bare Flask app + in-memory SQLite DB with every OTS table."""

    _import_all_models()

    app = Flask('eud-test')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    with app.app_context():
        db.create_all()
    return app


@pytest.fixture
def app_ctx(app: Flask) -> Iterator[Flask]:
    """Yield an active app context for tests that hit ``db.session``."""

    with app.app_context():
        yield app


@pytest.fixture(autouse=True)
def _clean_geofences() -> Iterator[None]:
    """Wipe the module-level geofence registry between tests."""

    with _geofences_lock:
        _geofences.clear()
    try:
        yield
    finally:
        with _geofences_lock:
            _geofences.clear()


def _make_eud(
    uid: str,
    *,
    callsign: str | None = None,
    last_event_time: datetime | None = None,
    last_status: str = 'Connected',
    device: str = 'Pixel',
    platform: str = 'ATAK',
    os: str = 'Android',
    version: str = '5.0',
):
    """Insert and return an :class:`opentakserver.models.EUD.EUD` row."""

    from opentakserver.models.EUD import EUD

    row = EUD(
        uid=uid,
        callsign=callsign,
        last_event_time=last_event_time,
        last_status=last_status,
        device=device,
        platform=platform,
        os=os,
        version=version,
    )
    db.session.add(row)
    db.session.commit()
    return row


# ---------------------------------------------------------------------------
# online()
# ---------------------------------------------------------------------------


def test_online_filters_stale_euds(app_ctx: Flask) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    _make_eud('fresh-uid', callsign='Fresh', last_event_time=now - timedelta(minutes=10))
    _make_eud('stale-uid', callsign='Stale', last_event_time=now - timedelta(hours=4))
    _make_eud('null-uid', callsign='Null', last_event_time=None)

    results = online(within=timedelta(hours=1))

    uids = {info.uid for info in results}
    assert uids == {'fresh-uid'}
    assert all(isinstance(info, EudInfo) for info in results)


def test_online_within_none_returns_all(app_ctx: Flask) -> None:
    _make_eud('one', callsign='One', last_event_time=datetime(2020, 1, 1))
    _make_eud('two', callsign='Two', last_event_time=None)

    results = online(within=None)

    assert {info.uid for info in results} == {'one', 'two'}


def test_online_requires_eud_read_scope(app_ctx: Flask) -> None:
    with plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            online()
    assert exc_info.value.code == 'permission.read.eud'


def test_online_passes_with_declared_scope(app_ctx: Flask) -> None:
    _make_eud(
        'plug-uid',
        callsign='Plug',
        last_event_time=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    with plugin_context(_manifest(read=['eud'])):
        results = online(within=timedelta(minutes=10))
    assert {r.uid for r in results} == {'plug-uid'}


def test_online_invalid_within_raises(app_ctx: Flask) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        online(within='one hour')  # type: ignore[arg-type]
    assert exc_info.value.code == 'eud.invalid_within'


# ---------------------------------------------------------------------------
# find()
# ---------------------------------------------------------------------------


def test_find_by_uid_returns_info(app_ctx: Flask) -> None:
    _make_eud('uid-A', callsign='Alpha')
    info = find(uid='uid-A')
    assert info is not None
    assert info.uid == 'uid-A'
    assert info.callsign == 'Alpha'


def test_find_by_callsign_returns_info(app_ctx: Flask) -> None:
    _make_eud('uid-B', callsign='Bravo')
    info = find(callsign='Bravo')
    assert info is not None
    assert info.uid == 'uid-B'


def test_find_with_both_args_raises_value_error(app_ctx: Flask) -> None:
    with pytest.raises(ValueError):
        find(uid='X', callsign='Y')


def test_find_with_neither_arg_raises_value_error(app_ctx: Flask) -> None:
    with pytest.raises(ValueError):
        find()


def test_find_missing_returns_none(app_ctx: Flask) -> None:
    assert find(uid='nope-not-here') is None
    assert find(callsign='nope-not-here') is None


def test_find_requires_eud_read_scope(app_ctx: Flask) -> None:
    _make_eud('uid-S', callsign='Scope')
    with plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            find(uid='uid-S')
    assert exc_info.value.code == 'permission.read.eud'


# ---------------------------------------------------------------------------
# send_to()
# ---------------------------------------------------------------------------


def test_send_to_publishes_to_dms_exchange() -> None:
    captured: dict[str, object] = {}

    def fake_publish(*, exchange: str, routing_key: str, body: str, properties=None) -> None:
        captured['exchange'] = exchange
        captured['routing_key'] = routing_key
        captured['body'] = body

    with mock.patch.object(eud_mod, '_amqp_publish', side_effect=fake_publish):
        ok = send_to('eud-uid-1', '<event uid="X" type="a-f-G"/>')

    assert ok is True
    assert captured['exchange'] == 'dms'
    assert captured['routing_key'] == 'eud-uid-1'
    envelope = json.loads(captured['body'])  # type: ignore[arg-type]
    assert envelope['cot'] == '<event uid="X" type="a-f-G"/>'
    assert envelope['uid'] == 'server'


def test_send_to_uses_plugin_slug_as_sender() -> None:
    with mock.patch.object(eud_mod, '_amqp_publish') as published:
        with plugin_context(_manifest(write=['cot'])):
            send_to('uid-1', '<event/>')

    body = published.call_args.kwargs['body']
    assert json.loads(body)['uid'] == 'plugin:test-plugin'


def test_send_to_requires_cot_write_scope() -> None:
    with mock.patch.object(eud_mod, '_amqp_publish'):
        with plugin_context(_manifest(write=[])):
            with pytest.raises(PermissionDeniedError) as exc_info:
                send_to('uid-1', '<event/>')
    assert exc_info.value.code == 'permission.write.cot'


def test_send_to_rejects_empty_uid() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        send_to('', '<event/>')
    assert exc_info.value.code == 'eud.invalid_uid'


def test_send_to_rejects_empty_xml() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        send_to('uid-1', '   ')
    assert exc_info.value.code == 'eud.invalid_cot_xml'


# ---------------------------------------------------------------------------
# Point-in-polygon
# ---------------------------------------------------------------------------


# A simple unit square in (lat, lon) space anchored at the origin.
SQUARE: list[tuple[float, float]] = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]


def test_point_in_polygon_inside_square() -> None:
    assert _point_in_polygon(0.5, 0.5, SQUARE) is True


def test_point_in_polygon_outside_square() -> None:
    assert _point_in_polygon(2.0, 2.0, SQUARE) is False
    assert _point_in_polygon(-0.1, 0.5, SQUARE) is False
    assert _point_in_polygon(0.5, 1.1, SQUARE) is False


def test_point_in_polygon_concave_shape() -> None:
    # A C-shape — point in the indentation must be outside.
    c_shape = [
        (0.0, 0.0),
        (0.0, 3.0),
        (3.0, 3.0),
        (3.0, 2.0),
        (1.0, 2.0),
        (1.0, 1.0),
        (3.0, 1.0),
        (3.0, 0.0),
    ]
    assert _point_in_polygon(0.5, 0.5, c_shape) is True   # inside body
    assert _point_in_polygon(2.0, 1.5, c_shape) is False  # in the bite


# ---------------------------------------------------------------------------
# geofence()
# ---------------------------------------------------------------------------


def _info(uid: str, lat: float, lon: float) -> EudInfo:
    return EudInfo(
        uid=uid,
        callsign=uid.upper(),
        last_lat=lat,
        last_lon=lon,
        last_event_time=None,
        last_status='Connected',
        device='Pixel',
        platform='ATAK',
        os='Android',
        version='5.0',
    )


def test_geofence_register_and_unregister() -> None:
    gf_id = geofence('zone-1', SQUARE)
    assert gf_id in _geofences
    assert ungeofence(gf_id) is True
    assert gf_id not in _geofences
    # Second call returns False — already gone.
    assert ungeofence(gf_id) is False


def test_geofence_on_enter_fires_on_first_entry() -> None:
    seen: list[EudInfo] = []
    gf_id = geofence('zone', SQUARE, on_enter=seen.append)
    try:
        # Outside first — no callback.
        eud_mod._handle_position_update(_info('eud-1', 5.0, 5.0))
        assert seen == []
        # Cross in — callback fires once.
        eud_mod._handle_position_update(_info('eud-1', 0.5, 0.5))
        assert len(seen) == 1
        assert seen[0].uid == 'eud-1'
        # Stay inside — no second callback.
        eud_mod._handle_position_update(_info('eud-1', 0.6, 0.6))
        assert len(seen) == 1
    finally:
        ungeofence(gf_id)


def test_geofence_on_exit_fires_on_leaving() -> None:
    enters: list[EudInfo] = []
    exits: list[EudInfo] = []
    gf_id = geofence('zone', SQUARE, on_enter=enters.append, on_exit=exits.append)
    try:
        eud_mod._handle_position_update(_info('eud-2', 0.5, 0.5))
        eud_mod._handle_position_update(_info('eud-2', 5.0, 5.0))
        assert len(enters) == 1
        assert len(exits) == 1
    finally:
        ungeofence(gf_id)


def test_geofence_handler_exception_isolated() -> None:
    def boom(_info: EudInfo) -> None:
        raise RuntimeError('plugin code blew up')

    seen: list[EudInfo] = []
    gf_bad = geofence('bad', SQUARE, on_enter=boom)
    gf_good = geofence('good', SQUARE, on_enter=seen.append)
    try:
        eud_mod._handle_position_update(_info('eud-3', 0.5, 0.5))
        # The good callback still fires even though the bad one raised.
        assert len(seen) == 1
    finally:
        ungeofence(gf_bad)
        ungeofence(gf_good)


def test_geofence_rejects_short_polygon() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        geofence('bad', [(0.0, 0.0), (1.0, 1.0)])
    assert exc_info.value.code == 'eud.invalid_polygon'


def test_geofence_rejects_non_callable() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        geofence('bad', SQUARE, on_enter='not a callable')  # type: ignore[arg-type]
    assert exc_info.value.code == 'eud.invalid_callback'


def test_geofence_rejects_bad_vertex_shape() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        geofence('bad', [(0.0, 0.0), (0.0, 1.0), (1.0,)])  # type: ignore[list-item]
    assert exc_info.value.code == 'eud.invalid_polygon'


def test_geofence_no_callbacks_is_legal() -> None:
    gf_id = geofence('quiet', SQUARE)
    try:
        # Should not raise even though both callbacks are None.
        eud_mod._handle_position_update(_info('eud-4', 0.5, 0.5))
        eud_mod._handle_position_update(_info('eud-4', 5.0, 5.0))
    finally:
        ungeofence(gf_id)
