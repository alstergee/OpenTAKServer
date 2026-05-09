"""``OTS.mission`` — plugin-friendly Mission API helpers.

A v2 plugin that declares ``mission = "read"`` or ``mission = "write"`` in
its ``plugin.toml`` can call these helpers to enumerate, create, invite to,
and attach content to TAK missions without speaking the raw Marti HTTP
contract.

Design
------
The HTTP routes in :mod:`opentakserver.blueprints.marti_api.mission_marti_api`
are tightly bound to ``flask.request`` (mTLS cert headers, query args, raw
body) — they are NOT cleanly callable from outside an HTTP request thread.
Rather than refactor all 2255 LOC of that module in one go, this SDK module
re-implements the **core ORM-level mission flow** against the same
:class:`Mission`/:class:`MissionContent`/:class:`MissionContentMission`/
:class:`MissionInvitation` models. The ATAK-side broadcast (RabbitMQ
``missions`` exchange) is intentionally NOT performed here — see
``Open architectural decisions`` D-6 in the SKILL doc. Plugins that need
the CoT change broadcast can opt in via ``OTS.cot.broadcast`` after the
DB write, once C.1 ships.

All helpers run inside the active plugin's context (set by the loader's
``_with_plugin_context`` wrapper), and therefore the
:func:`requires_mission` decorator gates each call against the manifest.

The Flask app context (``current_app``) must be available — the module
reads ``OTS_DATA_FOLDER`` and ``ALLOWED_EXTENSIONS`` from app config and
opens a SQLAlchemy session from :data:`opentakserver.extensions.db`.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import logging
import mimetypes
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from flask import current_app

from opentakserver.extensions import db
from opentakserver.models.Mission import Mission
from opentakserver.models.MissionChange import MissionChange
from opentakserver.models.MissionContent import MissionContent
from opentakserver.models.MissionContentMission import MissionContentMission
from opentakserver.models.MissionInvitation import InvitationTypeEnum, MissionInvitation
from opentakserver.models.MissionRole import MissionRole
from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.permissions import current_plugin, requires_mission

logger = logging.getLogger(__name__)


__all__ = [
    'MissionContentInfo',
    'MissionInfo',
    'MissionSDKError',
    'add_content',
    'create',
    'get',
    'invite',
    'list',
    'list_content',
    'remove_content',
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class MissionSDKError(OTSPluginError):
    """Mission helper failure. ``code`` is one of:

    * ``mission.not_found`` — name lookup miss.
    * ``mission.exists`` — :func:`create` race / duplicate name.
    * ``mission.bad_extension`` — :func:`add_content` with a path whose
      extension is not in ``ALLOWED_EXTENSIONS``.
    * ``mission.file_missing`` — :func:`add_content` source path is missing.
    * ``mission.content_not_found`` — :func:`remove_content` hash miss.
    """


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass
class MissionInfo:
    """Plugin-facing view of a TAK mission.

    ``members`` is the list of usernames currently holding a non-owner
    :class:`MissionRole` row plus the owner. It does NOT include EUD-only
    invitations (those are tracked separately).
    """

    name: str
    description: str
    members: list[str] = field(default_factory=list)
    created_at: _dt.datetime | None = None
    owner: str = ''


@dataclass
class MissionContentInfo:
    """Plugin-facing view of a single mission content attachment."""

    hash: str
    filename: str
    size: int
    uploaded_at: _dt.datetime | None
    uploader: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _plugin_slug() -> str:
    """Return the active plugin's slug, or ``'core'`` if running outside a plugin.

    Used as the ``submitter`` field on :class:`MissionContent` and as the
    ``creator_uid`` placeholder on :class:`MissionChange` rows so that
    audit trails reflect which plugin made the write.
    """

    manifest = current_plugin()
    return manifest.slug if manifest is not None else 'core'


def _to_info(mission: Mission) -> MissionInfo:
    members = sorted(
        {role.username for role in (mission.roles or []) if role.username}
    )
    owner_username = ''
    if mission.owner is not None and getattr(mission.owner, 'user', None):
        owner_username = mission.owner.user.username or ''
    elif mission.creator_uid:
        owner_username = mission.creator_uid
    return MissionInfo(
        name=mission.name,
        description=mission.description or '',
        members=members,
        created_at=mission.create_time,
        owner=owner_username,
    )


def _to_content_info(content: MissionContent) -> MissionContentInfo:
    return MissionContentInfo(
        hash=content.hash,
        filename=content.filename or '',
        size=content.size or 0,
        uploaded_at=content.submission_time,
        uploader=content.submitter or '',
    )


def _find_mission(name: str) -> Mission | None:
    return db.session.query(Mission).filter_by(name=name).first()


def _record_mission_change(
    mission_name: str,
    change_type: str,
    *,
    content_uid: str | None = None,
) -> None:
    """Append a :class:`MissionChange` row. Does NOT broadcast over RabbitMQ.

    The Marti HTTP routes additionally publish a CoT change-notification on
    the ``missions`` exchange. Plugins that need that behavior can call
    ``OTS.cot.broadcast`` after the helper returns — see SKILL D-6.
    """

    change = MissionChange()
    change.isFederatedChange = False
    change.change_type = change_type
    change.mission_name = mission_name
    change.timestamp = _now()
    change.creator_uid = _plugin_slug()
    change.server_time = change.timestamp
    if content_uid is not None:
        change.content_uid = content_uid
    db.session.add(change)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@requires_mission('read')
def list() -> 'list[MissionInfo]':  # noqa: A001 — module-level alias to builtin is intentional
    """Return all missions visible to the current plugin's role.

    The current implementation returns every mission row. A future
    enhancement (SKILL D-7) will narrow this to missions whose group set
    intersects with the plugin's declared group scopes.
    """

    rows = db.session.query(Mission).all()
    return [_to_info(m) for m in rows]


@requires_mission('read')
def get(name: str) -> MissionInfo | None:
    """Return the mission named ``name``, or ``None`` if absent."""

    if not name:
        return None
    mission = _find_mission(name)
    return _to_info(mission) if mission is not None else None


@requires_mission('write')
def create(
    *,
    name: str,
    description: str = '',
    members: 'list[str] | None' = None,
) -> MissionInfo:
    """Create a TAK mission.

    Parameters
    ----------
    name:
        Mission name. Must be unique server-wide.
    description:
        Free-form description, persisted on :class:`Mission`.
    members:
        Optional list of usernames to seed as :class:`MissionRole` subscribers.
        The plugin slug is recorded as the creator (``creator_uid``) on the
        ``CREATE_MISSION`` :class:`MissionChange` row.

    Raises
    ------
    MissionSDKError
        With ``code='mission.exists'`` when a mission of that name already
        exists. The DB transaction is rolled back before raising.
    """

    if not name or not name.strip():
        raise MissionSDKError(
            code='mission.invalid_name',
            message='Mission name cannot be empty.',
        )

    if _find_mission(name) is not None:
        raise MissionSDKError(
            code='mission.exists',
            message=f'Mission {name!r} already exists.',
        )

    plugin_slug = _plugin_slug()

    mission = Mission()
    mission.name = name
    mission.description = description or None
    mission.tool = 'public'
    mission.group = '__ANON__'
    mission.default_role = MissionRole.MISSION_SUBSCRIBER
    mission.guid = str(uuid.uuid4())
    mission.create_time = _now()
    mission.password_protected = False
    mission.expiration = -1
    mission.creator_uid = plugin_slug

    db.session.add(mission)
    # flush so MissionRole foreign-key constraint sees the mission
    db.session.flush()

    for username in (members or []):
        role = MissionRole()
        role.clientUid = plugin_slug
        role.username = username
        role.createTime = mission.create_time
        role.role_type = MissionRole.MISSION_SUBSCRIBER
        role.mission_name = mission.name
        db.session.add(role)

    _record_mission_change(mission.name, MissionChange.CREATE_MISSION)
    db.session.commit()

    logger.info(
        'mission.create slug=%s name=%s members=%d',
        plugin_slug,
        name,
        len(members or []),
        extra={'plugin': plugin_slug},
    )

    return _to_info(mission)


@requires_mission('write')
def invite(mission_name: str, identifier: str) -> bool:
    """Invite a callsign or username to a mission.

    The ``identifier`` is matched first against EUD callsigns (the common
    case from a plugin), then against User usernames. Idempotent: re-inviting
    the same identifier is a no-op and still returns ``True``.

    Returns ``True`` on success (whether newly-created or already present).
    Raises :class:`MissionSDKError` with ``code='mission.not_found'`` when
    the mission does not exist.
    """

    if not mission_name or not identifier:
        raise MissionSDKError(
            code='mission.invalid_invite',
            message='mission_name and identifier are required.',
        )

    mission = _find_mission(mission_name)
    if mission is None:
        raise MissionSDKError(
            code='mission.not_found',
            message=f'Mission {mission_name!r} not found.',
        )

    # Lazy imports — break a circular ORM-import otherwise.
    from opentakserver.models.EUD import EUD
    from opentakserver.models.user import User

    eud = db.session.query(EUD).filter_by(callsign=identifier).first()
    invite_type = InvitationTypeEnum.callsign
    field_kwargs: dict[str, str] = {}
    if eud is not None:
        field_kwargs = {'callsign': identifier}
    else:
        user = db.session.query(User).filter_by(username=identifier).first()
        if user is not None:
            invite_type = InvitationTypeEnum.userName
            field_kwargs = {'username': identifier}
        else:
            # Last-chance: accept the raw identifier as a callsign for plugins
            # whose target EUD has not yet enrolled. The Marti route would
            # 404 here; the SDK is more permissive on purpose so plugins can
            # pre-stage invites for crew that will join later.
            field_kwargs = {'callsign': identifier}

    existing = (
        db.session.query(MissionInvitation)
        .filter_by(mission_name=mission_name, type=invite_type, **field_kwargs)
        .first()
    )
    if existing is not None:
        logger.info(
            'mission.invite idempotent slug=%s mission=%s id=%s',
            _plugin_slug(),
            mission_name,
            identifier,
            extra={'plugin': _plugin_slug()},
        )
        return True

    invitation = MissionInvitation()
    invitation.mission_name = mission_name
    invitation.mission_guid = mission.guid
    invitation.type = invite_type
    invitation.creator_uid = _plugin_slug()
    invitation.role = MissionRole.MISSION_SUBSCRIBER
    for k, v in field_kwargs.items():
        setattr(invitation, k, v)

    db.session.add(invitation)
    db.session.commit()

    logger.info(
        'mission.invite created slug=%s mission=%s id=%s type=%s',
        _plugin_slug(),
        mission_name,
        identifier,
        invite_type.value,
        extra={'plugin': _plugin_slug()},
    )
    return True


def _attach_content_to_mission(
    mission: Mission,
    file_path: Path,
    content_type: str | None,
) -> str:
    """Hash + persist a file, link it to ``mission``, append a MissionChange.

    Pulled out as a standalone helper so future refactors can have the
    ``/Marti/sync/upload`` route call this same function — matching the
    design hint in the task brief. The Marti route is NOT yet wired to it
    (touching that file is out of scope for C.4 — see SKILL D-6).

    Returns the SHA-256 hex digest of the file content (the Marti API's
    canonical content identifier).
    """

    if not file_path.is_file():
        raise MissionSDKError(
            code='mission.file_missing',
            message=f'No file at {file_path!s}.',
        )

    allowed = current_app.config.get('ALLOWED_EXTENSIONS', '') or ''
    allowed_set = {ext.strip().lower() for ext in allowed.split(',') if ext.strip()}
    extension = file_path.suffix.lstrip('.').lower()
    if allowed_set and extension and extension not in allowed_set:
        raise MissionSDKError(
            code='mission.bad_extension',
            message=f'.{extension} is not in ALLOWED_EXTENSIONS.',
        )

    payload = file_path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()

    if content_type is None:
        guess, _ = mimetypes.guess_type(file_path.name)
        content_type = guess or 'application/octet-stream'

    plugin_slug = _plugin_slug()

    content = (
        db.session.query(MissionContent).filter_by(hash=digest).first()
    )
    if content is None:
        content = MissionContent()
        content.mime_type = content_type
        content.filename = file_path.name
        content.submission_time = _now()
        content.submitter = plugin_slug
        content.uid = str(uuid.uuid4())
        content.creator_uid = plugin_slug
        content.size = len(payload)
        content.expiration = -1
        content.keywords = []
        content.hash = digest
        db.session.add(content)
        db.session.flush()

    # Persist the file to the on-disk missions folder so the Marti
    # ``/Marti/sync/content`` GET route can serve it back to ATAK clients.
    data_folder = current_app.config.get('OTS_DATA_FOLDER')
    if data_folder:
        missions_dir = os.path.join(data_folder, 'missions')
        os.makedirs(missions_dir, exist_ok=True)
        target = os.path.join(missions_dir, file_path.name)
        Path(target).write_bytes(payload)

    link = (
        db.session.query(MissionContentMission)
        .filter_by(mission_content_id=content.id, mission_name=mission.name)
        .first()
    )
    if link is None:
        link = MissionContentMission()
        link.mission_content_id = content.id
        link.mission_name = mission.name
        db.session.add(link)
        _record_mission_change(
            mission.name, MissionChange.ADD_CONTENT, content_uid=content.uid
        )

    db.session.commit()
    return digest


@requires_mission('write')
def add_content(
    mission_name: str,
    file_path: 'str | Path',
    content_type: str | None = None,
) -> str:
    """Upload + attach a content file to a mission. Returns the SHA-256 hash.

    The file is hashed; if a :class:`MissionContent` row already exists for
    that hash the existing row is reused (matches Marti behavior). The
    ``MissionContentMission`` join row is created idempotently — re-attaching
    the same file is safe.
    """

    mission = _find_mission(mission_name)
    if mission is None:
        raise MissionSDKError(
            code='mission.not_found',
            message=f'Mission {mission_name!r} not found.',
        )

    path = Path(file_path) if not isinstance(file_path, Path) else file_path
    return _attach_content_to_mission(mission, path, content_type)


@requires_mission('read')
def list_content(mission_name: str) -> 'list[MissionContentInfo]':
    """List content currently attached to a mission.

    Raises :class:`MissionSDKError` with ``code='mission.not_found'`` when
    the mission does not exist.
    """

    mission = _find_mission(mission_name)
    if mission is None:
        raise MissionSDKError(
            code='mission.not_found',
            message=f'Mission {mission_name!r} not found.',
        )
    return [_to_content_info(c) for c in (mission.contents or [])]


@requires_mission('write')
def remove_content(mission_name: str, content_hash: str) -> bool:
    """Detach a content file from a mission. Returns ``True`` on success.

    The :class:`MissionContent` row itself is preserved so federated mission
    history stays auditable — only the ``mission_content_mission`` join row
    is dropped, matching the Marti DELETE semantics.

    Raises :class:`MissionSDKError` if the mission does not exist
    (``mission.not_found``) or the content is not attached to it
    (``mission.content_not_found``).
    """

    mission = _find_mission(mission_name)
    if mission is None:
        raise MissionSDKError(
            code='mission.not_found',
            message=f'Mission {mission_name!r} not found.',
        )

    content = (
        db.session.query(MissionContent).filter_by(hash=content_hash).first()
    )
    if content is None:
        raise MissionSDKError(
            code='mission.content_not_found',
            message=f'No content with hash {content_hash!r}.',
        )

    link = (
        db.session.query(MissionContentMission)
        .filter_by(mission_content_id=content.id, mission_name=mission.name)
        .first()
    )
    if link is None:
        raise MissionSDKError(
            code='mission.content_not_found',
            message=(
                f'Content {content_hash!r} is not attached to mission '
                f'{mission_name!r}.'
            ),
        )

    db.session.delete(link)
    _record_mission_change(
        mission.name, MissionChange.REMOVE_CONTENT, content_uid=content.uid
    )
    db.session.commit()

    logger.info(
        'mission.remove_content slug=%s mission=%s hash=%s',
        _plugin_slug(),
        mission_name,
        content_hash,
        extra={'plugin': _plugin_slug()},
    )
    return True
