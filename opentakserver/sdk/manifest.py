"""OpenTAK Plugin SDK v2 — manifest schema and loader.

Defines the Pydantic v2 models that validate a plugin's ``plugin.toml``. The
loader (:func:`load_manifest`) reads the file with stdlib ``tomllib``, hands
the parsed dict to :class:`PluginManifest`, and re-raises validation failures
as :class:`ManifestValidationError` (an :class:`OTSPluginError` subclass)
with the underlying pydantic detail preserved on ``.details``.

Design notes
------------
* Models opt in to extra-field strictness on a per-model basis. ``MountSpec``
  variants use ``model_config = ConfigDict(extra='ignore')`` because mount
  specs are a wide discriminated union and silently dropping unknown fields
  is friendlier to forward-compat plugin authors than rejecting them. Top
  level models (``PluginManifest``, ``PluginPermissions``, ``PluginMenu``)
  use ``extra='forbid'`` because typos in the top-level manifest are bugs.
* The ``mount`` list is validated as a discriminated union on ``kind``. Add
  a new variant by writing a new ``<Kind>Mount`` model and appending it to
  the :data:`MountSpec` Annotated union.
* :class:`OTSPluginError` lives here (vs a sibling ``errors.py``) until we
  have more than one error type — matches the YAGNI rule in the agent
  contract.
"""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class OTSPluginError(Exception):
    """Base SDK error. Every plugin-loader failure is one of these.

    Attributes
    ----------
    code:
        Stable machine-readable identifier (``'manifest.invalid'``,
        ``'permissions.denied'``, …). UIs key off this for translations.
    message:
        Human-readable detail. Safe to show in toasts.
    details:
        Optional structured payload (e.g. pydantic ``ValidationError.errors()``).
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def __str__(self) -> str:
        return f'[{self.code}] {self.message}'

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}'
            f'(code={self.code!r}, message={self.message!r})'
        )


class ManifestValidationError(OTSPluginError):
    """Raised when ``plugin.toml`` fails schema validation.

    ``code`` is always ``'manifest.invalid'``. ``details`` carries the
    pydantic ``ValidationError.errors()`` output so callers can render
    field-level UI feedback without re-parsing the message string.
    """

    def __init__(self, message: str, *, details: Any | None = None) -> None:
        super().__init__('manifest.invalid', message, details=details)


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


_VALID_SCOPES: frozenset[str] = frozenset(
    {'eud', 'geochat', 'kml', 'mission', 'cot', 'config', 'storage', 'dp'}
)


MissionLevel = Literal['none', 'read', 'write']


class PluginPermissions(BaseModel):
    """Declared scopes a plugin needs. Enforced by the SDK at call time."""

    model_config = ConfigDict(extra='forbid')

    read: list[str] = Field(default_factory=list)
    write: list[str] = Field(default_factory=list)
    mesh: bool = False
    mission: MissionLevel = 'none'
    admin_routes: bool = False

    @field_validator('read', 'write')
    @classmethod
    def _validate_scopes(cls, value: list[str]) -> list[str]:
        bad = [item for item in value if item not in _VALID_SCOPES]
        if bad:
            allowed = ', '.join(sorted(_VALID_SCOPES))
            raise ValueError(
                f'unknown permission scope(s): {bad!r}. allowed: [{allowed}]'
            )
        return value


# ---------------------------------------------------------------------------
# Mount specs (discriminated union by `kind`)
#
# Each variant maps 1:1 to a row in the "Mount points" table of the
# /docker/opentak/.claude/skills/ots-plugin-architecture/SKILL.md document.
# ---------------------------------------------------------------------------


class _MountBase(BaseModel):
    """Shared base for every mount kind.

    Mount models use ``extra='ignore'`` so a v2.1 manifest with a new field
    on ``kind = 'tab'`` still loads on a v2.0 server (with that field
    silently dropped). Top-level manifest fields remain strict via
    ``PluginManifest.model_config``. This is a deliberate forward-compat
    bias for plugin authors.
    """

    model_config = ConfigDict(extra='ignore')

    label: str
    roles: list[str] = Field(default_factory=list)


def _path_must_be_absolute(value: str) -> str:
    if not value.startswith('/'):
        raise ValueError(f'path must start with "/", got: {value!r}')
    return value


class _RoutedMountBase(_MountBase):
    """Common base for mounts that occupy a UI route slot."""

    path: str
    icon: str | None = None

    @field_validator('path')
    @classmethod
    def _check_path(cls, value: str) -> str:
        return _path_must_be_absolute(value)


class _EndpointMountBase(_MountBase):
    """Common base for mounts that hit a server endpoint."""

    endpoint: str

    @field_validator('endpoint')
    @classmethod
    def _check_endpoint(cls, value: str) -> str:
        return _path_must_be_absolute(value)


class _EventHandlerMountBase(_MountBase):
    """Common base for event-driven server-only mounts."""

    event: str
    handler: str  # python dotted path: my.module:func or my.module.func


# --- routed (path) mounts ---------------------------------------------------


class TabMount(_RoutedMountBase):
    kind: Literal['tab']


class SubTabMount(_RoutedMountBase):
    kind: Literal['subtab']
    parent: str  # host page slug (e.g. 'eud', 'map', 'settings')


class FrameMount(_RoutedMountBase):
    kind: Literal['frame']


class NavbarGroupItemMount(_RoutedMountBase):
    kind: Literal['navbar_group_item']


# --- endpoint mounts --------------------------------------------------------


class MapOverlayMount(_EndpointMountBase):
    kind: Literal['map_overlay']


class MapDrawerMount(_EndpointMountBase):
    kind: Literal['map_drawer']


class DashboardWidgetMount(_EndpointMountBase):
    kind: Literal['dashboard_widget']


class CotHandlerMount(_EndpointMountBase):
    kind: Literal['cot_handler']


class EudActionMount(_EndpointMountBase):
    kind: Literal['eud_action']


class EudQrActionMount(_EndpointMountBase):
    kind: Literal['eud_qr_action']


class DataPackageGeneratorMount(_EndpointMountBase):
    kind: Literal['data_package_generator']


class ModalMount(_EndpointMountBase):
    kind: Literal['modal']


class ToolbarButtonMount(_EndpointMountBase):
    kind: Literal['toolbar_button']


class SettingsSectionMount(_EndpointMountBase):
    kind: Literal['settings_section']


# --- event-handler (server-only) mounts -------------------------------------


class NotificationHandlerMount(_EventHandlerMountBase):
    kind: Literal['notification_handler']


class MissionContentProviderMount(_EventHandlerMountBase):
    kind: Literal['mission_content_provider']


class SocketEventMount(_EventHandlerMountBase):
    kind: Literal['socket_event']


class MeshChannelHandlerMount(_EventHandlerMountBase):
    kind: Literal['mesh_channel_handler']


# --- specials ---------------------------------------------------------------


class BackgroundWorkerMount(_MountBase):
    kind: Literal['background_worker']
    cron: str
    handler: str  # python dotted path


class WebhookMount(_MountBase):
    kind: Literal['webhook']
    path: str
    hmac_secret_env: str | None = None

    @field_validator('path')
    @classmethod
    def _check_path(cls, value: str) -> str:
        return _path_must_be_absolute(value)


class CliCommandMount(_MountBase):
    kind: Literal['cli_command']
    name: str
    handler: str  # python dotted path


class AuthBackendMount(_MountBase):
    kind: Literal['auth_backend']
    name: str
    handler: str  # python dotted path


# Discriminated union — Pydantic dispatches on the ``kind`` literal. Order
# inside the Union doesn't matter for correctness but mirrors the order in
# the architecture map for readability.
MountSpec = Annotated[
    Union[
        TabMount,
        SubTabMount,
        FrameMount,
        MapOverlayMount,
        MapDrawerMount,
        DashboardWidgetMount,
        NavbarGroupItemMount,
        ModalMount,
        NotificationHandlerMount,
        EudActionMount,
        CotHandlerMount,
        SettingsSectionMount,
        ToolbarButtonMount,
        BackgroundWorkerMount,
        WebhookMount,
        CliCommandMount,
        MeshChannelHandlerMount,
        EudQrActionMount,
        DataPackageGeneratorMount,
        MissionContentProviderMount,
        SocketEventMount,
        AuthBackendMount,
    ],
    Field(discriminator='kind'),
]
"""Discriminated union over every supported ``[[plugin.mount]]`` kind.

Use as a type alias (``list[MountSpec]``). To validate an arbitrary dict at
runtime, wrap in a ``TypeAdapter``::

    from pydantic import TypeAdapter
    spec = TypeAdapter(MountSpec).validate_python({'kind': 'tab', ...})
"""


# ---------------------------------------------------------------------------
# Top-level manifest
# ---------------------------------------------------------------------------


class PluginMenu(BaseModel):
    """Optional advanced menu integration.

    Kept for backwards-compat with the design doc's ``[plugin.menu]`` table.
    New plugins should prefer a ``MountSpec(kind='eud_action')`` with the
    same label + endpoint — it's strictly more flexible (supports ``roles``,
    etc.). We keep both so the design-doc-shaped manifest still validates.
    """

    model_config = ConfigDict(extra='forbid')

    eud_action_label: str | None = None
    eud_action_endpoint: str | None = None


class PluginManifest(BaseModel):
    """Top-level v2 plugin manifest.

    Validated against ``plugin.toml``'s ``[plugin]`` table (and its
    sub-tables). ``api_version`` is locked to 2 here because v1 plugins
    don't carry a manifest at all — they're loaded by the legacy
    :class:`opentakserver.plugins.PluginManager.PluginManager`.
    """

    model_config = ConfigDict(extra='forbid')

    api_version: Literal[2]
    name: str
    slug: str = Field(pattern=r'^[a-z][a-z0-9-]*$')
    version: str
    author: str
    license: str
    description: str
    docs_url: str | None = None
    icon: str | None = None
    permissions: PluginPermissions = Field(default_factory=PluginPermissions)
    mount: list[MountSpec] = Field(min_length=1)
    menu: PluginMenu | None = None


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_manifest(path: str | Path) -> PluginManifest:
    """Read and validate a plugin's ``plugin.toml``.

    The TOML file's ``[plugin]`` table is treated as the manifest root. If
    no ``[plugin]`` table exists the entire document is validated as the
    manifest — that lets dual-purpose ``pyproject.toml`` files work too.

    Raises
    ------
    ManifestValidationError
        If the file is missing, unreadable, or fails schema validation.
        ``details`` is the pydantic ``ValidationError.errors()`` list when
        the failure is schema-level; otherwise ``None``.
    """

    p = Path(path)
    try:
        raw = p.read_bytes()
    except OSError as exc:
        raise ManifestValidationError(
            f'could not read plugin manifest at {p}: {exc}'
        ) from exc

    try:
        data = tomllib.loads(raw.decode('utf-8'))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ManifestValidationError(
            f'plugin manifest at {p} is not valid TOML: {exc}'
        ) from exc

    plugin_section: dict[str, Any] = data.get('plugin', data)
    try:
        manifest = PluginManifest.model_validate(plugin_section)
    except ValidationError as exc:
        raise ManifestValidationError(
            f'plugin manifest at {p} is invalid:\n{exc}',
            details=exc.errors(),
        ) from exc

    logger.info(
        'loaded plugin manifest',
        extra={'plugin': manifest.slug, 'version': manifest.version},
    )
    return manifest


__all__ = [
    'OTSPluginError',
    'ManifestValidationError',
    'PluginPermissions',
    'PluginMenu',
    'PluginManifest',
    'MountSpec',
    'MissionLevel',
    'TabMount',
    'SubTabMount',
    'FrameMount',
    'NavbarGroupItemMount',
    'MapOverlayMount',
    'MapDrawerMount',
    'DashboardWidgetMount',
    'CotHandlerMount',
    'EudActionMount',
    'EudQrActionMount',
    'DataPackageGeneratorMount',
    'ModalMount',
    'ToolbarButtonMount',
    'SettingsSectionMount',
    'NotificationHandlerMount',
    'MissionContentProviderMount',
    'SocketEventMount',
    'MeshChannelHandlerMount',
    'BackgroundWorkerMount',
    'WebhookMount',
    'CliCommandMount',
    'AuthBackendMount',
    'load_manifest',
]
