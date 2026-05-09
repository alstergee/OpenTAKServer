"""``/api/plugins/v2/*`` admin endpoints for the new Plugins UI tab.

This blueprint is the read/write surface the Plugins SPA tab consumes. It
exposes:

* ``GET /api/plugins/v2/installed`` — merged list of vanilla (SDK v1) and
  v2 plugins with name, version, description, declared scopes, mounts.
* ``GET /api/plugins/v2/mounts`` — flat array of every registered v2 mount.
* ``GET /api/plugins/v2/<slug>/manifest`` — the raw v2 manifest as JSON.
* ``GET /api/plugins/v2/<slug>/log?lines=N`` — the most recent N log lines
  buffered for a single plugin.
* ``POST /api/plugins/v2/<slug>/enable|disable`` — flip the
  ``OTS_PLUGIN_DISABLED`` set in ``config.yml`` (admin only).
* ``POST /api/plugins/v2/install`` — pip-install a wheel/git/url/spec then
  re-discover (admin only).
* ``POST /api/plugins/v2/<slug>/uninstall`` — pip-uninstall + drop mounts
  (admin only).
* ``GET /api/plugins/v2/marketplace`` — proxied JSON from
  ``OTS_PLUGIN_MARKETPLACE_URL``, cached 5min in-memory.

Every endpoint is ``@auth_required()``. Mutating endpoints additionally
require the ``administrator`` role via ``@roles_accepted("administrator")``.

Failures inside route bodies should raise an :class:`OTSPluginError`
subclass; the blueprint's :func:`_handle_plugin_error` translates it to
``{"success": False, "error": e.code, "detail": str(e)}`` with HTTP 500.
"""

from __future__ import annotations

import importlib.metadata as imd
import logging
import os
import re
import subprocess
import time
import traceback
from collections.abc import Iterable
from typing import Any

import yaml
from flask import Blueprint
from flask import current_app as app
from flask import jsonify, request
from flask_security import auth_required, roles_accepted

from opentakserver.sdk.manifest import (
    ManifestValidationError,
    OTSPluginError,
    PluginManifest,
)
from opentakserver.sdk.mount_registry import mount_registry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Blueprint + error handler
# ---------------------------------------------------------------------------


plugins_v2_blueprint = Blueprint('plugins_v2_api_blueprint', __name__)


# Errors raised inside plugins_v2 endpoints.
class PluginNotFoundError(OTSPluginError):
    """The given slug was not recognised as installed (vanilla or v2)."""

    def __init__(self, slug: str) -> None:
        super().__init__(
            code='plugin.not_found',
            message=f'plugin {slug!r} is not installed',
        )


class PluginInstallError(OTSPluginError):
    """Pip install / uninstall failed (non-zero return code or timeout)."""


class PluginInputError(OTSPluginError):
    """The user-supplied input (e.g. ``source``) failed validation."""

    def __init__(self, message: str) -> None:
        super().__init__(code='plugin.input_invalid', message=message)


class PluginMarketplaceError(OTSPluginError):
    """Could not fetch the marketplace JSON. Returns an empty list to UI."""


@plugins_v2_blueprint.errorhandler(OTSPluginError)
def _handle_plugin_error(exc: OTSPluginError):
    """Single translation point: ``OTSPluginError`` → JSON 500.

    Sub-error subclasses can short-circuit by setting ``http_status`` on
    the instance (e.g. 400/403/404). Default is 500.
    """

    status = getattr(exc, 'http_status', None)
    if status is None:
        if isinstance(exc, PluginInputError):
            status = 400
        elif isinstance(exc, PluginNotFoundError):
            status = 404
        elif isinstance(exc, ManifestValidationError):
            status = 400
        else:
            status = 500
    payload = {
        'success': False,
        'error': exc.code,
        'detail': str(exc),
    }
    if exc.details is not None:
        payload['details'] = exc.details
    logger.warning(
        'plugins_v2: %s -> HTTP %d',
        exc,
        status,
        extra={'error_code': exc.code},
    )
    return jsonify(payload), status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Pip source spec — covers pip names, versioned specs (==, >=), wheel URLs,
# git+https URLs, file paths. Matches "alphanumerics + . _ - + / @ : = space".
_SOURCE_RE = re.compile(r'^[a-zA-Z0-9._\-+/@:= ]+$')

_VANILLA_ENTRY_GROUP = 'opentakserver.plugin'

# Pip subprocess timeout in seconds.
_PIP_TIMEOUT = 300

# Marketplace cache.
_MARKETPLACE_CACHE_TTL_SECONDS = 300
_marketplace_cache: dict[str, Any] = {'fetched_at': 0.0, 'data': None}


def _validate_source_spec(source: str) -> str:
    """Reject anything outside our pip-spec character allowlist."""

    if not isinstance(source, str) or not source.strip():
        raise PluginInputError('"source" is required and must be a non-empty string')
    cleaned = source.strip()
    if len(cleaned) > 512:
        raise PluginInputError('"source" exceeds 512-char limit')
    if not _SOURCE_RE.match(cleaned):
        raise PluginInputError(
            'invalid "source" spec; allowed chars: a-z A-Z 0-9 . _ - + / @ : = space'
        )
    return cleaned


def _validate_slug(slug: str) -> str:
    """Slugs match the manifest regex; reject anything else early."""

    if not isinstance(slug, str) or not re.match(r'^[a-z][a-z0-9._\-]*$', slug):
        raise PluginInputError(f'invalid plugin slug: {slug!r}')
    return slug


def _v2_manager() -> Any | None:
    """Return the optional :class:`PluginManagerV2` instance, or ``None``.

    Phase A.4 wires the loader onto ``app.extensions['plugin_manager_v2']``.
    Until that ships, this returns ``None`` and the v2 plugin list / install
    re-discovery are simply skipped.
    """

    try:
        ext = getattr(app, 'extensions', {})
        return ext.get('plugin_manager_v2')
    except RuntimeError:
        return None


def _v2_manifests() -> dict[str, PluginManifest]:
    """Return a ``{slug: manifest}`` dict from the v2 manager (or empty).

    The loader's ``manifests()`` returns a ``list[PluginManifest]`` sorted
    by slug; we re-key it here so callers can do O(1) slug lookups.
    """

    mgr = _v2_manager()
    if mgr is None:
        return {}
    try:
        manifests = mgr.manifests()
    except OTSPluginError:
        raise
    except Exception as exc:  # noqa: BLE001 — surface as plugin error
        logger.error('v2 manager manifests() failed: %s', exc)
        logger.debug(traceback.format_exc())
        return {}
    if isinstance(manifests, dict):
        return dict(manifests)
    if isinstance(manifests, list):
        return {m.slug: m for m in manifests}
    return {}


def _vanilla_distributions() -> list[imd.Distribution]:
    """Distributions that own a vanilla ``opentakserver.plugin`` entry-point."""

    out: list[imd.Distribution] = []
    seen: set[str] = set()
    for ep in imd.entry_points(group=_VANILLA_ENTRY_GROUP):
        dist = ep.dist
        if dist is None:
            continue
        name = dist.metadata.get('Name') or dist.name
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(dist)
    return out


def _disabled_set() -> set[str]:
    """Return the persisted ``OTS_PLUGIN_DISABLED`` set."""

    raw = app.config.get('OTS_PLUGIN_DISABLED') or []
    if isinstance(raw, str):
        raw = [raw]
    return {str(item).lower() for item in raw}


def _persist_disabled_set(disabled: Iterable[str]) -> None:
    """Persist the disabled set to ``config.yml`` and update the live config.

    The on-disk file is treated as authoritative for restarts. If the file
    cannot be written we still update the live config so the change applies
    until restart, and log loud.
    """

    sorted_disabled = sorted({str(s).lower() for s in disabled})
    app.config['OTS_PLUGIN_DISABLED'] = sorted_disabled

    data_folder = app.config.get('OTS_DATA_FOLDER')
    if not data_folder:
        logger.warning(
            'OTS_DATA_FOLDER missing — disabled set updated in memory only'
        )
        return
    config_path = os.path.join(data_folder, 'config.yml')
    try:
        existing: dict[str, Any] = {}
        if os.path.exists(config_path):
            with open(config_path, 'r') as fh:
                existing = yaml.safe_load(fh) or {}
        existing['OTS_PLUGIN_DISABLED'] = sorted_disabled
        with open(config_path, 'w') as fh:
            yaml.safe_dump(existing, fh, sort_keys=True)
    except OSError as exc:
        logger.error('failed to persist OTS_PLUGIN_DISABLED to %s: %s', config_path, exc)


def _plugin_log_buffer(slug: str, n: int) -> list[str]:
    """Return the last ``n`` log lines buffered for ``slug``.

    The v2 manager is the source of truth when it exposes a ``log_lines``
    method (per-plugin ring buffer wired into the ``extra={'plugin':
    slug}`` log filter). When the manager is missing or doesn't yet
    implement the buffer, we return an empty list rather than 500 — the
    UI degrades gracefully ("no logs available").
    """

    mgr = _v2_manager()
    if mgr is None or not hasattr(mgr, 'log_lines'):
        return []
    try:
        return list(mgr.log_lines(slug, lines=n))
    except OTSPluginError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error('v2 manager log_lines(%s) failed: %s', slug, exc)
        return []


def _v2_plugin_summary(slug: str, manifest: PluginManifest) -> dict[str, Any]:
    """Shape one v2 entry for ``GET /installed``."""

    mounts = mount_registry.by_plugin(slug)
    kinds = sorted({m.get('kind') for m in mounts if m.get('kind')})
    return {
        'slug': slug,
        'name': manifest.name,
        'version': manifest.version,
        'version_latest': None,
        'description': manifest.description,
        'icon': manifest.icon,
        'docs_url': manifest.docs_url,
        'sdk': 'v2',
        'mounts': kinds,
        'scopes': {
            'read': list(manifest.permissions.read),
            'write': list(manifest.permissions.write),
            'mesh': bool(manifest.permissions.mesh),
            'mission': manifest.permissions.mission,
            'admin_routes': bool(manifest.permissions.admin_routes),
        },
        'enabled': slug.lower() not in _disabled_set(),
    }


def _vanilla_plugin_summary(dist: imd.Distribution) -> dict[str, Any]:
    """Shape one vanilla SDK v1 entry for ``GET /installed``."""

    md = dist.metadata
    name = md.get('Name') or dist.name or 'unknown'
    slug = name.lower()
    return {
        'slug': slug,
        'name': name,
        'version': md.get('Version') or 'unknown',
        'version_latest': None,
        'description': md.get('Summary') or '',
        'icon': None,
        'docs_url': md.get('Home-page') or md.get('Project-URL') or None,
        'sdk': 'v1',
        'mounts': [],
        'scopes': {
            'read': [],
            'write': [],
            'mesh': False,
            'mission': 'none',
            'admin_routes': False,
        },
        'enabled': slug not in _disabled_set(),
    }


# ---------------------------------------------------------------------------
# GET endpoints
# ---------------------------------------------------------------------------


@plugins_v2_blueprint.route('/api/plugins/v2/installed', methods=['GET'])
@auth_required()
def list_installed():
    """Merged list of vanilla + v2 plugins. See module docstring for shape."""

    v2_manifests = _v2_manifests()
    v2_slugs = {slug.lower() for slug in v2_manifests}

    plugins: list[dict[str, Any]] = []
    for slug, manifest in v2_manifests.items():
        plugins.append(_v2_plugin_summary(slug, manifest))

    # Vanilla — exclude any pip dist that's already represented as a v2 plugin
    # (a plugin that ships both pyproject + plugin.toml is "v2 wins").
    for dist in _vanilla_distributions():
        name = (dist.metadata.get('Name') or dist.name or '').lower()
        if name in v2_slugs:
            continue
        plugins.append(_vanilla_plugin_summary(dist))

    plugins.sort(key=lambda p: p['slug'])
    return jsonify({'plugins': plugins})


@plugins_v2_blueprint.route('/api/plugins/v2/mounts', methods=['GET'])
@auth_required()
def list_mounts():
    """Flat array of every registered mount across all v2 plugins."""

    return jsonify({'mounts': mount_registry.all()})


@plugins_v2_blueprint.route(
    '/api/plugins/v2/<slug>/manifest', methods=['GET'], strict_slashes=False
)
@auth_required()
def get_manifest(slug: str):
    """Return the raw v2 manifest as JSON, or 404 for vanilla / missing."""

    slug = _validate_slug(slug)
    manifests = _v2_manifests()
    manifest = manifests.get(slug)
    if manifest is None:
        raise PluginNotFoundError(slug)
    return jsonify(manifest.model_dump(mode='json'))


@plugins_v2_blueprint.route(
    '/api/plugins/v2/<slug>/log', methods=['GET'], strict_slashes=False
)
@auth_required()
def get_plugin_log(slug: str):
    """Return up to ``lines`` recent log lines for ``slug``."""

    slug = _validate_slug(slug)
    raw = request.args.get('lines', default='200', type=str)
    try:
        n = int(raw)
    except (TypeError, ValueError):
        raise PluginInputError(f'invalid "lines" value: {raw!r}')
    if n <= 0 or n > 5000:
        raise PluginInputError('"lines" must be between 1 and 5000')
    return jsonify({'lines': _plugin_log_buffer(slug, n)})


# ---------------------------------------------------------------------------
# Mutating endpoints — admin role required
# ---------------------------------------------------------------------------


def _set_plugin_enabled(slug: str, *, enabled: bool) -> dict[str, Any]:
    """Shared body for enable/disable. Returns the JSON response payload."""

    slug = _validate_slug(slug).lower()
    v2_known = slug in {s.lower() for s in _v2_manifests()}
    vanilla_known = any(
        (d.metadata.get('Name') or d.name or '').lower() == slug
        for d in _vanilla_distributions()
    )
    if not (v2_known or vanilla_known):
        raise PluginNotFoundError(slug)

    disabled = _disabled_set()
    if enabled:
        disabled.discard(slug)
    else:
        disabled.add(slug)
    _persist_disabled_set(disabled)

    # Best-effort vanilla compatibility — if the legacy plugin manager has
    # the slug, flip its DB-backed flag too. Failures don't bubble.
    legacy = getattr(app, 'plugin_manager', None)
    if legacy is not None and vanilla_known:
        try:
            if enabled and hasattr(legacy, 'enable_plugin'):
                legacy.enable_plugin(slug)
            elif not enabled and hasattr(legacy, 'disable_plugin'):
                legacy.disable_plugin(slug)
        except Exception as exc:  # noqa: BLE001
            logger.warning('legacy plugin manager %s(%s) failed: %s',
                           'enable' if enabled else 'disable', slug, exc)

    return {'success': True, 'enabled': enabled, 'slug': slug}


@plugins_v2_blueprint.route(
    '/api/plugins/v2/<slug>/enable', methods=['POST'], strict_slashes=False
)
@auth_required()
@roles_accepted('administrator')
def enable_plugin(slug: str):
    """Drop ``slug`` from the disabled set and persist."""

    return jsonify(_set_plugin_enabled(slug, enabled=True))


@plugins_v2_blueprint.route(
    '/api/plugins/v2/<slug>/disable', methods=['POST'], strict_slashes=False
)
@auth_required()
@roles_accepted('administrator')
def disable_plugin(slug: str):
    """Add ``slug`` to the disabled set and persist."""

    return jsonify(_set_plugin_enabled(slug, enabled=False))


def _run_pip(args: list[str]) -> tuple[int, str, str]:
    """Run ``[pip, ...args]`` with shell=False, capture, 300s timeout."""

    pip_bin = '/app/venv/bin/pip'
    cmd = [pip_bin, *args]
    logger.info('plugins_v2: running %s', cmd)
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_PIP_TIMEOUT,
            check=False,
        )
    except FileNotFoundError as exc:
        raise PluginInstallError(
            code='plugin.pip.missing',
            message=f'pip binary not found at {pip_bin}: {exc}',
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise PluginInstallError(
            code='plugin.pip.timeout',
            message=f'pip command timed out after {_PIP_TIMEOUT}s',
        ) from exc
    return proc.returncode, proc.stdout or '', proc.stderr or ''


@plugins_v2_blueprint.route('/api/plugins/v2/install', methods=['POST'])
@auth_required()
@roles_accepted('administrator')
def install_plugin():
    """Pip-install a plugin from a name / wheel URL / git+https spec."""

    body = request.get_json(silent=True) or {}
    source = body.get('source', '')
    cleaned = _validate_source_spec(source)

    # ``--no-input`` ensures pip never blocks on a TTY prompt. Split on
    # whitespace so callers can pass a single space-separated spec OR
    # multiple chunks the regex still allows.
    spec_parts = [chunk for chunk in cleaned.split(' ') if chunk]
    rc, out, err = _run_pip(['install', '--no-input', *spec_parts])

    success = rc == 0
    if success:
        # Re-discover so the new plugin appears immediately.
        mgr = _v2_manager()
        if mgr is not None:
            try:
                mgr.discover()
            except OTSPluginError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error('v2 manager discover() after install failed: %s', exc)
    else:
        logger.warning('pip install %r exited %d', cleaned, rc)

    return (
        jsonify(
            {
                'success': success,
                'returncode': rc,
                'stdout': out,
                'stderr': err,
                'source': cleaned,
            }
        ),
        200 if success else 500,
    )


@plugins_v2_blueprint.route(
    '/api/plugins/v2/<slug>/uninstall', methods=['POST'], strict_slashes=False
)
@auth_required()
@roles_accepted('administrator')
def uninstall_plugin(slug: str):
    """Pip-uninstall ``slug`` and unregister its mounts."""

    slug = _validate_slug(slug).lower()
    rc, out, err = _run_pip(['uninstall', '-y', slug])
    success = rc == 0
    if success:
        try:
            mount_registry.unregister(slug)
        except OTSPluginError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning('mount_registry.unregister(%s) failed: %s', slug, exc)
        mgr = _v2_manager()
        if mgr is not None and hasattr(mgr, 'forget'):
            try:
                mgr.forget(slug)
            except Exception as exc:  # noqa: BLE001
                logger.warning('v2 manager forget(%s) failed: %s', slug, exc)
    else:
        logger.warning('pip uninstall %r exited %d', slug, rc)

    return (
        jsonify(
            {
                'success': success,
                'returncode': rc,
                'stdout': out,
                'stderr': err,
                'slug': slug,
            }
        ),
        200 if success else 500,
    )


# ---------------------------------------------------------------------------
# Marketplace
# ---------------------------------------------------------------------------


@plugins_v2_blueprint.route('/api/plugins/v2/marketplace', methods=['GET'])
@auth_required()
def get_marketplace():
    """Return the marketplace JSON. 5-min in-memory cache, fallback ``[]``."""

    now = time.monotonic()
    cached_at = _marketplace_cache.get('fetched_at') or 0.0
    if (
        _marketplace_cache.get('data') is not None
        and now - cached_at < _MARKETPLACE_CACHE_TTL_SECONDS
    ):
        return jsonify(_marketplace_cache['data'])

    url = app.config.get('OTS_PLUGIN_MARKETPLACE_URL') or (
        'https://raw.githubusercontent.com/alstergee/OpenTAKServer/'
        'alstergee-fixes/marketplace.json'
    )
    payload: dict[str, Any] | list[Any] = {'plugins': []}
    try:
        # Lazy import — keeps the blueprint cheap to load.
        import urllib.request

        req = urllib.request.Request(
            url, headers={'User-Agent': 'opentakserver-plugins-v2'}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 — admin only
            import json as _json

            payload = _json.loads(resp.read().decode('utf-8'))
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            'marketplace fetch failed for %s: %s — returning empty list', url, exc
        )
        payload = {'plugins': []}

    _marketplace_cache['fetched_at'] = now
    _marketplace_cache['data'] = payload
    return jsonify(payload)


__all__ = [
    'plugins_v2_blueprint',
    'PluginInputError',
    'PluginInstallError',
    'PluginMarketplaceError',
    'PluginNotFoundError',
]
