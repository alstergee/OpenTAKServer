"""``OTS.config`` — per-plugin scoped key/value configuration.

Each v2 plugin gets a single YAML file at::

    /app/ots/plugins/<slug>/config.yml

The file is a flat mapping of string keys to JSON-compatible values. Reads
re-parse the file on every call so a plugin always sees the latest
on-disk state (including external edits made by an admin via the
``/api/plugins/v2/<slug>/config`` endpoint, Phase A.5).

Writes go through :func:`opentakserver.sdk.modules._paths.atomic_write` so
a crash mid-write can never leave a partially-truncated config file.

Permissions
-----------

``OTS.config`` does **not** require an explicit permission scope — every
plugin needs somewhere to keep its own settings. It does require an
active plugin context (``current_plugin()`` must return a manifest);
otherwise we can't tell whose config to read or write. Calling these
helpers from core OTS code raises
:class:`~opentakserver.sdk.manifest.OTSPluginError` with
``code='config.no_plugin_context'``.
"""

from __future__ import annotations

import logging
from typing import Any

import yaml

from opentakserver.sdk.modules._paths import atomic_write, plugin_dir

logger = logging.getLogger(__name__)


_NO_CTX = 'config.no_plugin_context'
_CONFIG_FILENAME = 'config.yml'


def _config_path() -> 'tuple[str, "Path"]':  # noqa: F821 - forward ref for clarity
    """Return ``(slug, path)`` for the active plugin's ``config.yml``."""

    pdir = plugin_dir(error_code=_NO_CTX)
    return pdir.name, pdir / _CONFIG_FILENAME


def _read_all() -> dict[str, Any]:
    """Read and parse the active plugin's config file. Empty if missing."""

    _, path = _config_path()
    if not path.exists():
        return {}
    try:
        raw = path.read_text(encoding='utf-8')
    except OSError as exc:
        logger.warning(
            'failed to read plugin config at %s: %s', path, exc,
            extra={'plugin': path.parent.name},
        )
        return {}
    if not raw.strip():
        return {}
    parsed = yaml.safe_load(raw)
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        # Defensive: refuse to feed callers a list/scalar from a manually
        # corrupted file. We could raise, but returning empty + logging is
        # friendlier and lets ``set()`` overwrite the bad file.
        logger.warning(
            'plugin config at %s is not a mapping (got %s); treating as empty',
            path, type(parsed).__name__,
            extra={'plugin': path.parent.name},
        )
        return {}
    return parsed


def _write_all(data: dict[str, Any]) -> None:
    """Persist ``data`` to the active plugin's config file."""

    slug, path = _config_path()
    payload = yaml.safe_dump(data, sort_keys=True, default_flow_style=False)
    atomic_write(path, payload.encode('utf-8'))
    logger.info('wrote plugin config (%d keys)', len(data), extra={'plugin': slug})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get(key: str, default: Any = None) -> Any:
    """Read a per-plugin config value.

    Returns ``default`` when the key is absent. Raises
    :class:`OTSPluginError` (``code='config.no_plugin_context'``) when no
    plugin context is active.
    """

    return _read_all().get(key, default)


def set(key: str, value: Any) -> None:  # noqa: A001 — matches design doc
    """Write a per-plugin config value (atomic disk write).

    Persists the entire config dict to ``config.yml`` after updating the
    in-memory snapshot. ``value`` must be YAML-serialisable
    (``yaml.safe_dump``-compatible).
    """

    data = _read_all()
    data[key] = value
    _write_all(data)


def delete(key: str) -> bool:
    """Remove ``key``. Returns ``True`` if a key was removed, else ``False``."""

    data = _read_all()
    if key not in data:
        return False
    del data[key]
    _write_all(data)
    return True


def all() -> dict[str, Any]:  # noqa: A001 — matches design doc
    """Return a snapshot copy of the entire per-plugin config dict."""

    return dict(_read_all())


def clear() -> None:
    """Wipe every key for the current plugin (useful on uninstall)."""

    _write_all({})


__all__ = ['get', 'set', 'delete', 'all', 'clear']
