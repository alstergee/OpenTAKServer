"""Tests for :mod:`opentakserver.sdk.modules.mission`.

Run inside the OTS container::

    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_mission.py

The fixtures here build a minimal Flask app bound to an in-memory SQLite
database and create all model tables. We do NOT use ``opentakserver.app.create_app``
because it pulls in the full server (RabbitMQ, plugins, scheduler, …) and
makes the unit suite slow + flaky. The mission SDK module itself only needs
``current_app`` config (``OTS_DATA_FOLDER``, ``ALLOWED_EXTENSIONS``) and
``opentakserver.extensions.db`` — both wired by these fixtures.
"""

from __future__ import annotations

import contextlib
import importlib
import pkgutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from flask import Flask
from flask_security.models import fsqla_v3 as fsqla

from opentakserver.extensions import db
from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import mission as mission_mod
from opentakserver.sdk.modules.mission import MissionContentInfo, MissionInfo, MissionSDKError
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Walk-once model importer (matches the pattern in test_eud.py).
# SQLAlchemy's declarative registry only resolves string-form relationship
# targets once the referenced class is defined, so we import every model
# module before ``db.create_all`` runs. ``fsqla.FsModels.set_db_info(db)``
# wires the User FK targets so ``user.py`` can import.
# ---------------------------------------------------------------------------


def _import_all_models() -> None:
    """Import every ORM module so SQLAlchemy can resolve string-form FKs.

    Idempotent across modules + across pytest sessions: ``set_db_info`` is
    only called when the ``roles_users`` join table is not already attached
    to ``db.metadata`` (an earlier test module — e.g. test_eud — may have
    wired it on the shared singleton). A second call would re-define the
    table and explode.
    """

    if 'roles_users' not in db.metadata.tables:
        fsqla.FsModels.set_db_info(db)

    import opentakserver.models as models_pkg

    for _, name, _ in pkgutil.iter_modules(models_pkg.__path__):
        importlib.import_module(f'opentakserver.models.{name}')


# Imported lazily inside fixtures/tests to avoid touching the ORM at
# collection time. The names below are the leaves we use directly.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Plugin-context manager
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


def _manifest(*, mission: str = 'write') -> PluginManifest:
    return PluginManifest(
        api_version=2,
        name='Test Mission',
        slug='test-mission',
        version='0.0.0',
        author='test',
        license='MIT',
        description='mission sdk test fixture',
        permissions=PluginPermissions(
            read=[],
            write=[],
            mesh=False,
            mission=mission,  # type: ignore[arg-type]
        ),
        mount=[{'kind': 'tab', 'label': 'Test', 'path': '/test'}],
    )


# ---------------------------------------------------------------------------
# Flask app + DB fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app(tmp_path: Path) -> Flask:
    """Minimal Flask app with a freshly-built in-memory SQLite DB.

    ``OTS_DATA_FOLDER`` is steered at ``tmp_path`` so ``add_content``
    writes its on-disk copy into the pytest sandbox. Mirrors the fixture
    pattern in :mod:`test_eud` so the SDK suite stays consistent.
    """

    _import_all_models()

    app = Flask('mission-test')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['OTS_DATA_FOLDER'] = str(tmp_path)
    app.config['ALLOWED_EXTENSIONS'] = (
        'zip,xml,txt,pdf,png,jpg,jpeg,gif,kml,kmz,p12,tif,sqlite'
    )
    db.init_app(app)
    with app.app_context():
        db.create_all()
    return app


@pytest.fixture
def app_ctx(app: Flask) -> Iterator[Flask]:
    """Yield an active app context for tests that hit ``db.session``."""

    with app.app_context():
        yield app


# ---------------------------------------------------------------------------
# Permission gating
# ---------------------------------------------------------------------------


def test_create_requires_mission_write(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='read')):
        with pytest.raises(PermissionDeniedError) as exc_info:
            mission_mod.create(name='m1', description='d')
    assert exc_info.value.code == 'permission.mission.write'
    assert isinstance(exc_info.value, OTSPluginError)


def test_list_requires_mission_read(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='none')):
        with pytest.raises(PermissionDeniedError) as exc_info:
            mission_mod.list()
    assert exc_info.value.code == 'permission.mission.read'


def test_get_requires_mission_read(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='none')):
        with pytest.raises(PermissionDeniedError) as exc_info:
            mission_mod.get('whatever')
    assert exc_info.value.code == 'permission.mission.read'


def test_invite_requires_mission_write(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='read')):
        with pytest.raises(PermissionDeniedError) as exc_info:
            mission_mod.invite('m', 'someone')
    assert exc_info.value.code == 'permission.mission.write'


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_create_persists_mission_and_returns_info(app_ctx: Flask) -> None:
    from opentakserver.models.MissionChange import MissionChange
    from opentakserver.models.MissionRole import MissionRole

    with plugin_context(_manifest(mission='write')):
        info = mission_mod.create(
            name='Festival Crew',
            description='Coordinating with stage ops',
            members=['alice', 'bob'],
        )

    assert isinstance(info, MissionInfo)
    assert info.name == 'Festival Crew'
    assert info.description == 'Coordinating with stage ops'
    assert sorted(info.members) == ['alice', 'bob']
    assert info.created_at is not None
    assert info.owner == 'test-mission'

    rows = db.session.query(MissionRole).filter_by(mission_name='Festival Crew').all()
    assert {r.username for r in rows} == {'alice', 'bob'}

    change = (
        db.session.query(MissionChange)
        .filter_by(mission_name='Festival Crew', change_type=MissionChange.CREATE_MISSION)
        .first()
    )
    assert change is not None
    assert change.creator_uid == 'test-mission'


def test_create_duplicate_name_raises(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='Dup', description='')
        with pytest.raises(MissionSDKError) as exc_info:
            mission_mod.create(name='Dup', description='')
    assert exc_info.value.code == 'mission.exists'


def test_list_returns_all_missions(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='A', description='a')
        mission_mod.create(name='B', description='b', members=['carol'])

    with plugin_context(_manifest(mission='read')):
        rows = mission_mod.list()

    by_name = {r.name: r for r in rows}
    assert set(by_name) == {'A', 'B'}
    assert by_name['B'].members == ['carol']
    assert by_name['A'].members == []


def test_get_returns_none_for_missing(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='read')):
        assert mission_mod.get('does-not-exist') is None


def test_get_returns_info_for_existing(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='G1', description='g')
    with plugin_context(_manifest(mission='read')):
        info = mission_mod.get('G1')
    assert info is not None
    assert info.name == 'G1'
    assert info.description == 'g'


# ---------------------------------------------------------------------------
# Invite — idempotency
# ---------------------------------------------------------------------------


def _seed_user(username: str = 'eve') -> None:
    """Persist a minimal :class:`User` row for username-invite paths."""

    from opentakserver.models.user import User

    user = User()
    user.username = username
    user.email = f'{username}@example.test'
    user.password = 'x'
    user.active = True
    user.fs_uniquifier = username  # required NOT NULL on the flask-security model
    db.session.add(user)
    db.session.commit()


def test_invite_creates_invitation(app_ctx: Flask) -> None:
    from opentakserver.models.MissionInvitation import MissionInvitation

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='Inv', description='')
        _seed_user('eve')
        result = mission_mod.invite('Inv', 'eve')

    assert result is True
    rows = db.session.query(MissionInvitation).filter_by(mission_name='Inv').all()
    assert len(rows) == 1
    assert rows[0].username == 'eve'


def test_invite_is_idempotent(app_ctx: Flask) -> None:
    """Re-inviting the same identifier does NOT duplicate the invitation row."""

    from opentakserver.models.MissionInvitation import MissionInvitation

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='Inv', description='')
        _seed_user('frank')
        assert mission_mod.invite('Inv', 'frank') is True
        assert mission_mod.invite('Inv', 'frank') is True
        assert mission_mod.invite('Inv', 'frank') is True

    rows = db.session.query(MissionInvitation).filter_by(mission_name='Inv').all()
    assert len(rows) == 1


def test_invite_falls_back_to_callsign_for_unknown_identifier(app_ctx: Flask) -> None:
    """Identifier with no matching User/EUD is staged as a callsign invite."""

    from opentakserver.models.MissionInvitation import MissionInvitation

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='InvFut', description='')
        # No EUD or User seeded — SDK should still pre-stage as callsign.
        assert mission_mod.invite('InvFut', 'future-crew') is True

    rows = db.session.query(MissionInvitation).filter_by(mission_name='InvFut').all()
    assert len(rows) == 1
    assert rows[0].callsign == 'future-crew'


def test_invite_unknown_mission_raises(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='write')):
        with pytest.raises(MissionSDKError) as exc_info:
            mission_mod.invite('no-such-mission', 'someone')
    assert exc_info.value.code == 'mission.not_found'


# ---------------------------------------------------------------------------
# Content add / list / remove
# ---------------------------------------------------------------------------


def test_add_content_attaches_file_and_returns_hash(
    app_ctx: Flask, tmp_path: Path
) -> None:
    from opentakserver.models.MissionContent import MissionContent
    from opentakserver.models.MissionContentMission import MissionContentMission

    src = tmp_path / 'briefing.txt'
    src.write_text('hello tak')

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='C1', description='')
        digest = mission_mod.add_content('C1', src)

    assert isinstance(digest, str) and len(digest) == 64

    content = db.session.query(MissionContent).filter_by(hash=digest).first()
    assert content is not None
    assert content.filename == 'briefing.txt'
    assert content.size == len(b'hello tak')

    link = (
        db.session.query(MissionContentMission)
        .filter_by(mission_name='C1', mission_content_id=content.id)
        .first()
    )
    assert link is not None

    # On-disk copy lands at OTS_DATA_FOLDER/missions/<filename>
    assert (Path(app_ctx.config['OTS_DATA_FOLDER']) / 'missions' / 'briefing.txt').is_file()


def test_add_content_idempotent_for_same_file(
    app_ctx: Flask, tmp_path: Path
) -> None:
    from opentakserver.models.MissionContentMission import MissionContentMission

    src = tmp_path / 'kml.kml'
    src.write_text('<kml/>')

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='C2', description='')
        d1 = mission_mod.add_content('C2', src)
        d2 = mission_mod.add_content('C2', src)

    assert d1 == d2
    links = (
        db.session.query(MissionContentMission)
        .filter_by(mission_name='C2')
        .all()
    )
    assert len(links) == 1


def test_add_content_rejects_disallowed_extension(
    app_ctx: Flask, tmp_path: Path
) -> None:
    bad = tmp_path / 'notes.exe'
    bad.write_bytes(b'\x00\x01')

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='C3', description='')
        with pytest.raises(MissionSDKError) as exc_info:
            mission_mod.add_content('C3', bad)
    assert exc_info.value.code == 'mission.bad_extension'


def test_list_content_and_remove_content_round_trip(
    app_ctx: Flask, tmp_path: Path
) -> None:
    src = tmp_path / 'plan.pdf'
    src.write_bytes(b'%PDF-1.4 stub')

    with plugin_context(_manifest(mission='write')):
        mission_mod.create(name='C4', description='')
        digest = mission_mod.add_content('C4', src)

    with plugin_context(_manifest(mission='read')):
        items = mission_mod.list_content('C4')
    assert len(items) == 1
    assert isinstance(items[0], MissionContentInfo)
    assert items[0].hash == digest

    with plugin_context(_manifest(mission='write')):
        assert mission_mod.remove_content('C4', digest) is True
        # Content row stays, link row is gone
        assert mission_mod.list_content('C4') == []
        with pytest.raises(MissionSDKError) as exc_info:
            mission_mod.remove_content('C4', digest)
    assert exc_info.value.code == 'mission.content_not_found'


def test_remove_content_unknown_mission_raises(app_ctx: Flask) -> None:
    with plugin_context(_manifest(mission='write')):
        with pytest.raises(MissionSDKError) as exc_info:
            mission_mod.remove_content('nope', 'a' * 64)
    assert exc_info.value.code == 'mission.not_found'
