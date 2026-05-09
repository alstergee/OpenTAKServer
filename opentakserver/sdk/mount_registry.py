"""Mount registry — process-singleton store of plugin mount declarations.

Every v2 plugin's ``[[plugin.mount]]`` array gets registered here after the
manifest loads. The Plugins UI fetches the merged set via
``GET /api/plugins/v2/mounts`` and renders Tabs / SubTabs / MapOverlays /
DashboardWidgets / etc. into the SPA at runtime.

Thread-safety: registration may happen from any thread (the apscheduler
worker, an AMQP consumer callback, or the request thread that imports a
plugin), so all mutating ops sit behind ``threading.Lock``.

Public surface:
* :class:`MountRegistry` — the registry class.
* :data:`mount_registry` — module-level singleton instance. Use this
  everywhere the loader / API blueprint needs to read or write.
* :func:`serialize_for_ui` — pure helper that converts a list of
  :class:`MountSpec` objects to plain JSON-ready dicts, with the owning
  plugin's slug + version stamped onto each record.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from .manifest import MountSpec, OTSPluginError

logger = logging.getLogger(__name__)


def serialize_for_ui(
    specs: list[MountSpec],
    plugin_slug: str,
    plugin_version: str,
) -> list[dict[str, Any]]:
    """Convert a list of MountSpec objects into JSON-ready dicts.

    Each output dict carries every field of the underlying mount variant
    (including the discriminator ``kind``) plus two stamped fields:

    * ``_plugin``: the owning plugin's slug.
    * ``_version``: the owning plugin's manifest version string.

    The leading underscore signals "registry metadata, not a manifest field"
    so the UI can distinguish these from native MountSpec fields.

    Args:
        specs: Mount declarations from a single plugin's manifest.
        plugin_slug: The plugin's slug (matches the pip dist name).
        plugin_version: The plugin's manifest version (e.g. ``'1.1'``).

    Returns:
        Plain dicts ready for ``json.dumps`` / ``jsonify``.
    """
    serialised: list[dict[str, Any]] = []
    blueprint_prefix = f'/api/plugins/{plugin_slug}'
    for spec in specs:
        # ``model_dump`` includes the discriminator and all variant fields.
        record = spec.model_dump(mode='json')
        # D-8: auto-prefix relative endpoints with the plugin's blueprint
        # url_prefix. Spec authors write ``endpoint = "/files"``; the UI
        # calls ``axios.get(mount.endpoint)`` raw, so without this the
        # request hits ``https://host/files`` (404) instead of the actual
        # ``/api/plugins/<slug>/files``. Skip absolute URLs (http*) and
        # already-prefixed values so re-serialise stays idempotent.
        endpoint = record.get('endpoint')
        if (
            isinstance(endpoint, str)
            and endpoint.startswith('/')
            and not endpoint.startswith('/api/plugins/')
        ):
            record['endpoint'] = f'{blueprint_prefix}{endpoint}'
        record['_plugin'] = plugin_slug
        record['_version'] = plugin_version
        serialised.append(record)
    return serialised


class MountRegistry:
    """Process-singleton mount-point registry.

    Plugins register their mounts via :meth:`register` after their manifest
    parses. The Plugins UI reads the merged set via :meth:`all`, optionally
    filtered by :meth:`by_kind` or :meth:`by_plugin`.

    Internal storage:
        * ``_mounts``: ``{slug: list[MountSpec]}`` — raw specs by plugin.
        * ``_versions``: ``{slug: str}`` — manifest version per plugin so
          serialisation can stamp records with both slug and version.
        * ``_lock``: ``threading.Lock`` guarding both dicts on all writes
          and reads (reads are short, contention is negligible).
    """

    def __init__(self) -> None:
        self._mounts: dict[str, list[MountSpec]] = {}
        self._versions: dict[str, str] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------

    def register(
        self,
        plugin_slug: str,
        mounts: list[MountSpec],
        plugin_version: str,
    ) -> None:
        """Register (or replace) a plugin's mount declarations.

        Re-registering the same ``plugin_slug`` REPLACES the previous entry
        — appending duplicates would surface twice in the UI on hot-reload.

        Args:
            plugin_slug: The plugin's slug. Matches the pip dist name.
            mounts: One or more MountSpec instances from the manifest.
            plugin_version: The manifest's ``version`` field; stamped onto
                each serialised record so the UI can show "v1.1 (1.2 latest)".

        Raises:
            OTSPluginError: ``code='mount.empty'`` if ``mounts`` is empty.
                A v2 plugin that declares zero mounts has nothing to wire
                into the UI and should not be calling this method at all.
        """
        if not mounts:
            raise OTSPluginError(
                code='mount.empty',
                message=(
                    f"Plugin '{plugin_slug}' attempted to register with an "
                    f'empty mounts list. Declare at least one [[plugin.mount]] '
                    f'or skip mount registration entirely.'
                ),
            )

        with self._lock:
            existed = plugin_slug in self._mounts
            self._mounts[plugin_slug] = list(mounts)
            self._versions[plugin_slug] = plugin_version

        action = 'replaced' if existed else 'registered'
        logger.info(
            '%s %d mount(s) for plugin',
            action,
            len(mounts),
            extra={'plugin': plugin_slug},
        )

    def unregister(self, plugin_slug: str) -> None:
        """Remove a plugin's mount declarations.

        Used during plugin disable / uninstall / hot-reload. Unregistering
        a slug that was never registered is a no-op (warn-logged so the
        operator can spot stale calls without the loader exploding).

        Args:
            plugin_slug: The plugin slug to drop.
        """
        with self._lock:
            existed = self._mounts.pop(plugin_slug, None) is not None
            self._versions.pop(plugin_slug, None)

        if not existed:
            logger.warning(
                'unregister called for unknown plugin slug',
                extra={'plugin': plugin_slug},
            )
            return

        logger.info('unregistered all mounts for plugin', extra={'plugin': plugin_slug})

    # ------------------------------------------------------------------
    # Readers
    # ------------------------------------------------------------------

    def all(self) -> list[dict[str, Any]]:
        """Return every registered mount as a flat list of JSON dicts.

        Records are ordered by plugin slug for deterministic output (helps
        the UI render in a stable order and makes diffing easy in tests).
        Within a plugin, mount order matches manifest order.
        """
        with self._lock:
            slugs = sorted(self._mounts.keys())
            return [
                record
                for slug in slugs
                for record in serialize_for_ui(
                    self._mounts[slug], slug, self._versions[slug]
                )
            ]

    def by_kind(self, kind: str) -> list[dict[str, Any]]:
        """Return every registered mount whose ``kind`` matches.

        Args:
            kind: One of the discriminator values declared in
                :class:`MountSpec` (e.g. ``'tab'``, ``'map_overlay'``).
        """
        return [record for record in self.all() if record.get('kind') == kind]

    def by_plugin(self, plugin_slug: str) -> list[dict[str, Any]]:
        """Return every registered mount owned by a single plugin.

        Args:
            plugin_slug: The plugin slug to filter on.
        """
        with self._lock:
            specs = self._mounts.get(plugin_slug)
            version = self._versions.get(plugin_slug)
        if not specs or version is None:
            return []
        return serialize_for_ui(specs, plugin_slug, version)


# Module-level singleton. Import this everywhere — do NOT instantiate
# additional MountRegistry objects in production code.
mount_registry = MountRegistry()
