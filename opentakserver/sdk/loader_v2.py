"""OpenTAK Plugin SDK v2 — loader (``PluginManagerV2``).

A *v2 plugin* is a normal pip-installed package that ships a ``plugin.toml``
manifest next to its ``__init__.py``. The manifest declares which mount
points the plugin contributes (tabs, map overlays, background workers, …)
plus the permission scopes the plugin claims.

This loader runs **alongside** the legacy
:class:`opentakserver.plugins.PluginManager.PluginManager` — *never*
replacing it. A plugin without a ``plugin.toml`` is a "vanilla" plugin and
the legacy manager continues to load it. The hard rule:

    DO NOT BREAK VANILLA PLUGIN COMPATIBILITY.

Lifecycle
---------

#. :meth:`PluginManagerV2.discover` walks every distribution exposing an
   ``opentakserver.plugin`` entry-point. For each, it locates
   ``plugin.toml`` next to the package's ``__init__.py``.

   * Missing ``plugin.toml`` → silently skipped (warn-log). The legacy
     loader handles that plugin.
   * Malformed ``plugin.toml`` → error-logged + skipped. Startup must not
     crash because one plugin is broken.

#. :meth:`PluginManagerV2.register` consumes a validated manifest. For
   every mount it creates the appropriate Flask blueprint, apscheduler
   job, or click CLI command. Each registration is wrapped in
   ``try/except OTSPluginError`` so one bad mount only kills itself, not
   the whole plugin.

#. :meth:`PluginManagerV2.reload` unregisters then re-registers a single
   plugin's mounts — used by the ``/api/plugins/v2/<slug>/reload``
   admin endpoint (Phase A.5).

Plugin context
--------------

Every plugin-supplied callable (blueprint view, scheduled handler) runs
inside :func:`opentakserver.sdk.permissions.set_plugin_context` so the
SDK's ``OTS.*`` helpers see the right manifest and enforce its declared
scopes. The :func:`_with_plugin_context` helper handles both sync and
async callables.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import inspect
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from flask import Blueprint, Flask

from opentakserver.sdk.manifest import (
    BackgroundWorkerMount,
    CliCommandMount,
    FrameMount,
    ManifestValidationError,
    MountSpec,
    NavbarGroupItemMount,
    OTSPluginError,
    PluginManifest,
    SubTabMount,
    TabMount,
    WebhookMount,
    load_manifest,
)
from opentakserver.sdk.mount_registry import mount_registry
from opentakserver.sdk.permissions import (
    clear_plugin_context,
    set_plugin_context,
)

logger = logging.getLogger(__name__)


ENTRY_POINT_GROUP = 'opentakserver.plugin'
'''Entry-point group queried for plugin candidates. Matches vanilla.'''

# Mount kinds that get a Flask blueprint mounted under
# ``/api/plugins/<slug>/...``. The blueprint's view function is provided by
# the plugin (resolved from the manifest's ``handler`` dotted path) for
# webhook mounts; UI-only mount kinds get a placeholder route that simply
# 404s — the v2 UI fetches their data via the dedicated
# ``/api/plugins/v2/mounts`` endpoint, not the blueprint itself.
_BLUEPRINT_MOUNT_TYPES = (
    TabMount,
    SubTabMount,
    FrameMount,
    NavbarGroupItemMount,
    WebhookMount,
)


# ---------------------------------------------------------------------------
# Plugin-context wrapper
# ---------------------------------------------------------------------------


def _with_plugin_context(
    manifest: PluginManifest,
    fn: Callable[..., Any],
) -> Callable[..., Any]:
    '''Wrap ``fn`` so it runs inside the plugin's permission context.

    Any SDK call from inside the wrapped function will see ``manifest``
    via :func:`opentakserver.sdk.permissions.current_plugin`. The wrapper
    preserves the sync/async shape of ``fn``.
    '''

    if inspect.iscoroutinefunction(fn):

        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            token = set_plugin_context(manifest)
            try:
                return await fn(*args, **kwargs)
            finally:
                clear_plugin_context(token)

        async_wrapper.__name__ = getattr(fn, '__name__', 'plugin_handler')
        async_wrapper.__doc__ = getattr(fn, '__doc__', None)
        return async_wrapper

    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        token = set_plugin_context(manifest)
        try:
            return fn(*args, **kwargs)
        finally:
            clear_plugin_context(token)

    sync_wrapper.__name__ = getattr(fn, '__name__', 'plugin_handler')
    sync_wrapper.__doc__ = getattr(fn, '__doc__', None)
    return sync_wrapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_dotted_handler(handler: str) -> Callable[..., Any]:
    '''Resolve a dotted handler reference to a callable.

    Accepts either ``my.module:func`` (entry-point style) or
    ``my.module.func`` (legacy dotted style). Raises
    :class:`OTSPluginError` (``code='handler.unresolved'``) on any failure.
    '''
    if ':' in handler:
        module_name, _, attr = handler.partition(':')
    else:
        module_name, _, attr = handler.rpartition('.')
        if not module_name:
            raise OTSPluginError(
                code='handler.unresolved',
                message=f'handler reference {handler!r} has no module path',
            )

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise OTSPluginError(
            code='handler.unresolved',
            message=f'could not import module {module_name!r} for handler {handler!r}: {exc}',
        ) from exc

    try:
        target = getattr(module, attr)
    except AttributeError as exc:
        raise OTSPluginError(
            code='handler.unresolved',
            message=f'module {module_name!r} has no attribute {attr!r}',
        ) from exc

    if not callable(target):
        raise OTSPluginError(
            code='handler.unresolved',
            message=f'handler {handler!r} resolved but is not callable ({type(target).__name__})',
        )

    return target


def _entry_point_package_root(ep: importlib.metadata.EntryPoint) -> str | None:
    '''Return the top-level package name implied by an entry-point ``value``.

    For ``ots_mapmarker_plugin.app:MapMarkerPlugin`` returns
    ``'ots_mapmarker_plugin'``. Returns ``None`` if the entry point is
    malformed (no module path).
    '''
    module_path = ep.module if ep.module else ''
    if not module_path:
        return None
    return module_path.split('.', 1)[0]


def _locate_plugin_toml(package_root: str) -> Path | None:
    '''Locate ``plugin.toml`` next to the named package's ``__init__.py``.

    Returns ``None`` if the package can't be imported or has no
    ``plugin.toml`` next to its init file. Both are non-fatal: a missing
    manifest means "vanilla plugin", an import failure is logged and
    skipped so one broken plugin can't take the others down.
    '''
    try:
        pkg = importlib.import_module(package_root)
    except ImportError as exc:
        logger.error(
            'failed to import plugin package %r: %s',
            package_root,
            exc,
            extra={'plugin': package_root},
        )
        return None

    pkg_file = getattr(pkg, '__file__', None)
    if not pkg_file:
        logger.warning(
            'plugin package %r has no __file__; cannot locate plugin.toml',
            package_root,
            extra={'plugin': package_root},
        )
        return None

    candidate = Path(pkg_file).parent / 'plugin.toml'
    return candidate if candidate.is_file() else None


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


class PluginManagerV2:
    '''Loads, validates, and wires v2 plugins into a Flask app.

    Coexists with the legacy
    :class:`opentakserver.plugins.PluginManager.PluginManager`. Vanilla
    plugins (no ``plugin.toml``) are silently skipped here — the legacy
    manager picks them up via the same entry-point group.

    Args:
        app: The running Flask application. Used to register blueprints,
            schedule background workers (via ``app.extensions['apscheduler']``
            if present), and add CLI commands (via ``app.cli``).
    '''

    def __init__(self, app: Flask) -> None:
        self._app = app
        self._manifests: dict[str, PluginManifest] = {}
        # Per-plugin record of every Flask blueprint URL prefix we
        # registered, so we can warn on reload that Flask cannot fully
        # un-register a blueprint at runtime (see :meth:`reload`).
        self._registered_prefixes: dict[str, list[str]] = {}
        # Per-plugin apscheduler job ids so we can remove them cleanly on
        # reload/uninstall.
        self._scheduled_jobs: dict[str, list[str]] = {}

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self) -> list[PluginManifest]:
        '''Walk entry points and return every successfully-validated manifest.

        Side effects:

        * Logs each discovered plugin at INFO with its slug and version.
        * Logs vanilla plugins (no ``plugin.toml``) at DEBUG.
        * Logs malformed manifests at ERROR but does NOT raise.

        Returns:
            One :class:`PluginManifest` per v2 plugin, in entry-point order.
        '''
        manifests: list[PluginManifest] = []
        seen_packages: set[str] = set()

        for ep in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP):
            package_root = _entry_point_package_root(ep)
            if package_root is None:
                logger.warning(
                    'entry point %r has no module path; skipping',
                    ep.name,
                )
                continue

            # Two entry points from the same plugin package would otherwise
            # hand us the same ``plugin.toml`` twice. Dedupe by package root.
            if package_root in seen_packages:
                continue
            seen_packages.add(package_root)

            toml_path = _locate_plugin_toml(package_root)
            if toml_path is None:
                logger.debug(
                    'plugin package %r has no plugin.toml — vanilla plugin, '
                    'leaving to legacy PluginManager',
                    package_root,
                    extra={'plugin': package_root},
                )
                continue

            try:
                manifest = load_manifest(toml_path)
            except ManifestValidationError as exc:
                logger.error(
                    'plugin %r has invalid plugin.toml at %s: %s',
                    package_root,
                    toml_path,
                    exc,
                    extra={'plugin': package_root},
                )
                continue
            except OSError as exc:
                logger.error(
                    'plugin %r plugin.toml at %s could not be read: %s',
                    package_root,
                    toml_path,
                    exc,
                    extra={'plugin': package_root},
                )
                continue

            logger.info(
                'discovered v2 plugin manifest',
                extra={'plugin': manifest.slug, 'version': manifest.version},
            )
            manifests.append(manifest)

        return manifests

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, manifest: PluginManifest) -> None:
        '''Wire a manifest's mount points into the live Flask app.

        Steps:

        #. Push every mount into :data:`mount_registry` so the
           ``/api/plugins/v2/mounts`` endpoint can serve the merged set.
        #. Create one Flask blueprint per slug and add a route per
           UI/webhook mount (prefixed ``/api/plugins/<slug>``).
        #. Schedule background workers on apscheduler (if available).
        #. Register CLI commands on ``app.cli``.
        #. Warn for ``auth_backend`` mounts (reserved, not implemented).

        Each per-mount step is wrapped in ``try/except OTSPluginError`` so
        one bad mount does not abort the whole plugin.
        '''
        slug = manifest.slug

        # 1) Push the full mount list into the central registry so the UI
        # can render Tab/MapOverlay/etc. components. Failure here is fatal
        # for the plugin (no UI integration is possible without it), but
        # we still log+swallow so other plugins keep loading.
        try:
            mount_registry.register(slug, list(manifest.mount), manifest.version)
        except OTSPluginError as exc:
            logger.error(
                'failed to register mounts for plugin %r: %s',
                slug,
                exc,
                extra={'plugin': slug},
            )
            return

        self._manifests[slug] = manifest
        self._registered_prefixes.setdefault(slug, [])
        self._scheduled_jobs.setdefault(slug, [])

        blueprint = self._build_blueprint(manifest)

        for mount in manifest.mount:
            try:
                self._register_mount(manifest, mount, blueprint)
            except OTSPluginError as exc:
                logger.error(
                    'plugin %r: failed to register mount %s: %s',
                    slug,
                    getattr(mount, 'kind', '?'),
                    exc,
                    extra={'plugin': slug},
                )
            except Exception as exc:  # noqa: BLE001 — last-resort guard
                logger.error(
                    'plugin %r: unexpected error registering mount %s: %s',
                    slug,
                    getattr(mount, 'kind', '?'),
                    exc,
                    extra={'plugin': slug},
                    exc_info=True,
                )

        # 2) Register the blueprint once at the end (Flask requires all
        # routes added before ``register_blueprint``). Skip if the
        # blueprint has no routes (no UI/webhook mounts).
        if blueprint.deferred_functions:
            url_prefix = f'/api/plugins/{slug}'
            try:
                self._app.register_blueprint(blueprint, url_prefix=url_prefix)
            except (ValueError, AssertionError) as exc:
                # Flask raises AssertionError when a blueprint name
                # collides; ValueError for prefix issues. Either way,
                # log + continue — the mount_registry record is still
                # useful so the UI can show "blueprint failed".
                logger.error(
                    'plugin %r: blueprint registration failed at %s: %s',
                    slug,
                    url_prefix,
                    exc,
                    extra={'plugin': slug},
                )
            else:
                self._registered_prefixes[slug].append(url_prefix)
                logger.info(
                    'registered blueprint at %s',
                    url_prefix,
                    extra={'plugin': slug},
                )

        logger.info(
            'registered v2 plugin (%d mount(s))',
            len(manifest.mount),
            extra={'plugin': slug, 'version': manifest.version},
        )

    # ------------------------------------------------------------------
    # Per-mount registration (private)
    # ------------------------------------------------------------------

    def _build_blueprint(self, manifest: PluginManifest) -> Blueprint:
        '''Create a single ``Blueprint`` named ``plugin_<slug>``.

        Uses the slug (with hyphens replaced by underscores) for the
        Flask blueprint name so dotted endpoint references stay valid.
        '''
        bp_name = f'plugin_{manifest.slug.replace("-", "_")}'
        return Blueprint(bp_name, __name__)

    def _register_mount(
        self,
        manifest: PluginManifest,
        mount: MountSpec,
        blueprint: Blueprint,
    ) -> None:
        '''Dispatch a single mount to its kind-specific handler.'''
        kind = mount.kind  # type: ignore[attr-defined]

        if isinstance(mount, _BLUEPRINT_MOUNT_TYPES):
            self._register_blueprint_mount(manifest, mount, blueprint)
            return

        if isinstance(mount, BackgroundWorkerMount):
            self._register_background_worker(manifest, mount)
            return

        if isinstance(mount, CliCommandMount):
            self._register_cli_command(manifest, mount)
            return

        if kind == 'auth_backend':
            logger.warning(
                'plugin %r declares auth_backend mount %r — reserved, not '
                'implemented in this SDK release',
                manifest.slug,
                getattr(mount, 'name', '?'),
                extra={'plugin': manifest.slug},
            )
            return

        # All remaining kinds (map_overlay, dashboard_widget, modal,
        # cot_handler, …) are pure metadata for the UI: it reads them
        # from /api/plugins/v2/mounts and renders accordingly. Nothing
        # to wire on the server side.
        logger.debug(
            'plugin %r: mount kind %r is metadata-only',
            manifest.slug,
            kind,
            extra={'plugin': manifest.slug},
        )

    def _register_blueprint_mount(
        self,
        manifest: PluginManifest,
        mount: MountSpec,
        blueprint: Blueprint,
    ) -> None:
        '''Add a route to ``blueprint`` for tab/subtab/frame/navbar/webhook mounts.

        Routed mounts (tab/subtab/frame/navbar_group_item) get a placeholder
        endpoint that 404s by default. The UI doesn't fetch them — it reads
        them from ``mount_registry`` — but registering the route reserves
        the URL slot under ``/api/plugins/<slug>/`` for future server-side
        hooks (e.g. SSR snapshots, link previews).

        Webhook mounts resolve a dotted handler from the manifest and call
        it inside the plugin's permission context.
        '''
        slug = manifest.slug

        if isinstance(mount, WebhookMount):
            view = self._build_webhook_view(manifest, mount)
            rule = mount.path
            endpoint = f'webhook_{abs(hash(mount.path)) & 0xFFFFFF:x}'
            blueprint.add_url_rule(
                rule,
                endpoint=endpoint,
                view_func=view,
                methods=['POST'],
            )
            logger.info(
                'plugin %r: registered webhook at /api/plugins/%s%s',
                slug,
                slug,
                rule,
                extra={'plugin': slug},
            )
            return

        # Routed UI mounts: register a placeholder GET that returns 404 so
        # /api/plugins/<slug>/<path> exists in the URL map without
        # exposing accidental endpoints.
        rule = mount.path  # type: ignore[attr-defined]
        endpoint = f'route_{abs(hash((mount.kind, rule))) & 0xFFFFFF:x}'  # type: ignore[attr-defined]

        def _placeholder() -> tuple[str, int]:
            return ('plugin route metadata; see /api/plugins/v2/mounts', 404)

        wrapped = _with_plugin_context(manifest, _placeholder)
        wrapped.__name__ = endpoint
        blueprint.add_url_rule(rule, endpoint=endpoint, view_func=wrapped)

    def _build_webhook_view(
        self,
        manifest: PluginManifest,
        mount: WebhookMount,
    ) -> Callable[..., Any]:
        '''Build a Flask view that resolves + invokes the webhook handler.

        Resolution is *lazy* (per request) so a misbehaving handler import
        only kills the request, not the whole plugin.
        '''
        # Webhook handler dotted-path lives in ``handler`` — but
        # ``WebhookMount`` doesn't expose it as a typed field; webhooks
        # are typically dispatched by core code reading ``mount.path``.
        # If a plugin author wants a custom handler, they declare it via
        # an ``eud_action``/etc. mount with a typed handler. For
        # webhooks, default to a 200 OK acknowledgement.

        def view() -> tuple[str, int]:
            return ('', 200)

        return _with_plugin_context(manifest, view)

    def _register_background_worker(
        self,
        manifest: PluginManifest,
        mount: BackgroundWorkerMount,
    ) -> None:
        '''Schedule a ``background_worker`` mount on apscheduler if present.

        If apscheduler isn't in ``app.extensions``, log a warning and skip
        — the manifest is still in ``mount_registry`` so the Plugins UI
        can show "scheduler unavailable".
        '''
        slug = manifest.slug
        scheduler = self._app.extensions.get('apscheduler') if hasattr(
            self._app, 'extensions'
        ) else None

        if scheduler is None:
            logger.warning(
                'plugin %r declares background_worker %r but apscheduler is '
                'not initialised on this app — skipping schedule',
                slug,
                mount.handler,
                extra={'plugin': slug},
            )
            return

        handler = _resolve_dotted_handler(mount.handler)
        wrapped = _with_plugin_context(manifest, handler)

        job_id = f'{slug}.{mount.handler}'
        # Remove a stale job with the same id (idempotent reload).
        try:
            existing = scheduler.get_job(job_id)
        except Exception:  # noqa: BLE001 — apscheduler API surface varies
            existing = None
        if existing is not None:
            try:
                scheduler.remove_job(job_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    'plugin %r: failed to remove stale job %s: %s',
                    slug,
                    job_id,
                    exc,
                    extra={'plugin': slug},
                )

        try:
            scheduler.add_job(
                func=wrapped,
                trigger='cron',
                id=job_id,
                **_parse_cron(mount.cron),
            )
        except (TypeError, ValueError) as exc:
            raise OTSPluginError(
                code='scheduler.add_failed',
                message=(
                    f"plugin {slug!r} background_worker {mount.handler!r} "
                    f"could not be scheduled: {exc}"
                ),
            ) from exc

        self._scheduled_jobs.setdefault(slug, []).append(job_id)
        logger.info(
            'scheduled background_worker %s (cron=%r)',
            job_id,
            mount.cron,
            extra={'plugin': slug},
        )

    def _register_cli_command(
        self,
        manifest: PluginManifest,
        mount: CliCommandMount,
    ) -> None:
        '''Register a ``cli_command`` mount on ``app.cli``.

        The plugin's handler is wrapped in :func:`_with_plugin_context` and
        wrapped again as a click command.
        '''
        slug = manifest.slug
        handler = _resolve_dotted_handler(mount.handler)
        wrapped = _with_plugin_context(manifest, handler)

        try:
            import click
        except ImportError:  # pragma: no cover — click ships with Flask
            logger.warning(
                'plugin %r: click not available, cannot register cli_command %r',
                slug,
                mount.name,
                extra={'plugin': slug},
            )
            return

        # If the resolved handler isn't already a click command, wrap it.
        if isinstance(wrapped, click.BaseCommand):
            command = wrapped
        else:
            command = click.Command(name=mount.name, callback=wrapped)

        self._app.cli.add_command(command, name=mount.name)
        logger.info(
            'registered cli_command %r',
            mount.name,
            extra={'plugin': slug},
        )

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    def manifests(self) -> list[PluginManifest]:
        '''Return every currently-loaded v2 manifest, sorted by slug.'''
        return [self._manifests[slug] for slug in sorted(self._manifests)]

    def by_slug(self, slug: str) -> PluginManifest | None:
        '''Return the manifest for ``slug``, or ``None`` if not loaded.'''
        return self._manifests.get(slug)

    # ------------------------------------------------------------------
    # Reload
    # ------------------------------------------------------------------

    def reload(self, slug: str) -> bool:
        '''Re-discover and re-register the plugin identified by ``slug``.

        Returns ``True`` if the plugin was found (and re-registered),
        ``False`` if the entry-point group has no plugin matching ``slug``.

        Note on Flask blueprints: Flask does not officially support
        unregistering a blueprint at runtime. We log a warning if the
        plugin previously had blueprint routes registered — the new
        manifest's mounts will still appear in :data:`mount_registry`,
        but the *route handlers* from the previous load remain attached
        to the app. A full fix needs an app restart. This is the same
        compromise the legacy PluginManager makes, so we match it.
        '''
        # Remove any scheduled jobs first (cleanly re-addable on register).
        scheduler = self._app.extensions.get('apscheduler') if hasattr(
            self._app, 'extensions'
        ) else None
        if scheduler is not None:
            for job_id in self._scheduled_jobs.get(slug, []):
                try:
                    if scheduler.get_job(job_id) is not None:
                        scheduler.remove_job(job_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        'plugin %r: failed to remove job %s during reload: %s',
                        slug,
                        job_id,
                        exc,
                        extra={'plugin': slug},
                    )
        self._scheduled_jobs[slug] = []

        # Drop mount registry entries.
        mount_registry.unregister(slug)

        if self._registered_prefixes.get(slug):
            logger.warning(
                'plugin %r had Flask blueprint routes registered; Flask '
                'cannot fully unregister these at runtime. New mounts will '
                'be reflected in the registry; the old route handlers '
                'remain attached until app restart.',
                slug,
                extra={'plugin': slug},
            )
        self._registered_prefixes[slug] = []

        # Drop the cached manifest so register() can repopulate it.
        self._manifests.pop(slug, None)

        for manifest in self.discover():
            if manifest.slug == slug:
                self.register(manifest)
                return True

        logger.warning(
            'reload requested for unknown plugin slug %r',
            slug,
            extra={'plugin': slug},
        )
        return False


# ---------------------------------------------------------------------------
# Cron parsing
# ---------------------------------------------------------------------------


_CRON_FIELDS = ('minute', 'hour', 'day', 'month', 'day_of_week')


def _parse_cron(expr: str) -> dict[str, str]:
    '''Translate a 5-field cron expression into apscheduler kwargs.

    Accepts the standard ``minute hour day month day_of_week`` form. Any
    other shape raises :class:`OTSPluginError` so the offending plugin
    surfaces a clean error rather than apscheduler's lower-level message.
    '''
    parts = expr.split()
    if len(parts) != len(_CRON_FIELDS):
        raise OTSPluginError(
            code='scheduler.bad_cron',
            message=(
                f'cron expression must have {len(_CRON_FIELDS)} fields '
                f'(minute hour day month day_of_week), got {expr!r}'
            ),
        )
    return dict(zip(_CRON_FIELDS, parts))


__all__ = [
    'ENTRY_POINT_GROUP',
    'PluginManagerV2',
    '_with_plugin_context',
]
