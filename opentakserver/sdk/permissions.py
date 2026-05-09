"""Permission enforcement decorators for the OpenTAK v2 plugin SDK.

A v2 plugin declares its scopes in ``[plugin.permissions]``. The SDK
helpers (``OTS.cot.send``, ``OTS.eud.online``, …) wrap their callable
bodies with the decorators in this module so the call fails closed if
the active plugin manifest does not declare the required scope.

The active manifest is held in a :class:`contextvars.ContextVar` so that
nested calls inside the same request thread, and async tasks spawned
from a plugin entry point, see the same plugin context. The loader
(Phase A.4) is responsible for pushing/popping the context — this
module just exposes the helpers.

When :func:`current_plugin` returns ``None`` (i.e. the helper was called
from core OTS code, not from inside a plugin), every decorator
short-circuits and allows the call through. Plugin-only failures must
not lock core code out of its own helpers.

Example:

    >>> from opentakserver.sdk.permissions import requires_read
    >>> @requires_read('eud')
    ... def list_euds() -> list[str]:
    ...     '''List connected EUDs (requires ``read = ["eud"]``).'''
    ...     return []
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypeVar

from opentakserver.sdk.manifest import OTSPluginError, PluginManifest

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])

_current_plugin_manifest: contextvars.ContextVar[PluginManifest | None] = (
    contextvars.ContextVar('_current_plugin_manifest', default=None)
)


class PermissionDeniedError(OTSPluginError):
    """Raised when a plugin calls an SDK helper outside its declared scopes.

    The ``code`` field follows ``permission.<kind>.<scope>`` so the loader
    and dashboard can match without parsing the message. ``kind`` is one
    of ``read``, ``write``, ``mesh``, ``mission``.
    """


def set_plugin_context(manifest: PluginManifest) -> contextvars.Token:
    """Push ``manifest`` onto the plugin-context stack.

    Returns the :class:`contextvars.Token` that must be passed to
    :func:`clear_plugin_context` when the plugin call boundary ends. The
    loader pairs the two around every plugin invocation.
    """

    return _current_plugin_manifest.set(manifest)


def clear_plugin_context(token: contextvars.Token) -> None:
    """Pop the plugin context using the token returned by :func:`set_plugin_context`."""

    _current_plugin_manifest.reset(token)


def current_plugin() -> PluginManifest | None:
    """Return the manifest of the plugin in the current context, or ``None``.

    Never raises. ``None`` means we are running outside a plugin call
    boundary (e.g. core OTS code, a unit test, or a request handler that
    has not entered a plugin yet).
    """

    try:
        return _current_plugin_manifest.get()
    except LookupError:
        # Defensive: ContextVar with a default should never raise here,
        # but if a future refactor drops the default we still want to be
        # safe outside any context.
        return None


def _missing(scopes: tuple[str, ...], declared: list[str]) -> str | None:
    """Return the first scope in ``scopes`` not present in ``declared``."""

    declared_set = set(declared)
    for scope in scopes:
        if scope not in declared_set:
            return scope
    return None


def _wrap(
    func: Callable[..., Any],
    check: Callable[[PluginManifest], None],
) -> Callable[..., Any]:
    """Bind ``check`` in front of ``func``; preserve sync/async shape."""

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            manifest = current_plugin()
            if manifest is not None:
                check(manifest)
            return await func(*args, **kwargs)

        return async_wrapper

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        manifest = current_plugin()
        if manifest is not None:
            check(manifest)
        return func(*args, **kwargs)

    return sync_wrapper


def requires_read(*scopes: str) -> Callable[[F], F]:
    """Gate a callable behind ``permissions.read`` covering all ``scopes``.

    Raises :class:`PermissionDeniedError` with code
    ``permission.read.<missing_scope>`` if the active plugin's manifest
    omits any of the requested scopes. Allows the call through when no
    plugin context is active.
    """

    if not scopes:
        raise ValueError('requires_read needs at least one scope')

    def check(manifest: PluginManifest) -> None:
        missing = _missing(scopes, manifest.permissions.read)
        if missing is not None:
            raise PermissionDeniedError(
                code=f'permission.read.{missing}',
                message=(
                    f'Plugin {manifest.slug!r} called a helper requiring '
                    f'read scope {missing!r}, but its manifest declares '
                    f'read = {manifest.permissions.read!r}.'
                ),
            )

    def decorator(func: F) -> F:
        return _wrap(func, check)  # type: ignore[return-value]

    return decorator


def requires_write(*scopes: str) -> Callable[[F], F]:
    """Gate a callable behind ``permissions.write`` covering all ``scopes``.

    Raises :class:`PermissionDeniedError` with code
    ``permission.write.<missing_scope>`` on first missing scope.
    """

    if not scopes:
        raise ValueError('requires_write needs at least one scope')

    def check(manifest: PluginManifest) -> None:
        missing = _missing(scopes, manifest.permissions.write)
        if missing is not None:
            raise PermissionDeniedError(
                code=f'permission.write.{missing}',
                message=(
                    f'Plugin {manifest.slug!r} called a helper requiring '
                    f'write scope {missing!r}, but its manifest declares '
                    f'write = {manifest.permissions.write!r}.'
                ),
            )

    def decorator(func: F) -> F:
        return _wrap(func, check)  # type: ignore[return-value]

    return decorator


def requires_mesh() -> Callable[[F], F]:
    """Gate a callable behind ``permissions.mesh = true``.

    Raises :class:`PermissionDeniedError` with code ``permission.mesh``
    when the plugin has not opted into Meshtastic publish/subscribe.
    """

    def check(manifest: PluginManifest) -> None:
        if not manifest.permissions.mesh:
            raise PermissionDeniedError(
                code='permission.mesh',
                message=(
                    f'Plugin {manifest.slug!r} called a Meshtastic helper '
                    f'but did not declare mesh = true.'
                ),
            )

    def decorator(func: F) -> F:
        return _wrap(func, check)  # type: ignore[return-value]

    return decorator


def requires_mission(level: Literal['read', 'write']) -> Callable[[F], F]:
    """Gate a callable behind ``permissions.mission`` at ``level`` or higher.

    ``level = "read"`` accepts ``mission in {"read", "write"}``.
    ``level = "write"`` accepts only ``mission == "write"``.

    Raises :class:`PermissionDeniedError` with code
    ``permission.mission.<level>`` on insufficient grant.
    """

    if level not in ('read', 'write'):
        raise ValueError(f'requires_mission level must be read|write, got {level!r}')

    def check(manifest: PluginManifest) -> None:
        granted = manifest.permissions.mission
        ok = (level == 'read' and granted in ('read', 'write')) or (
            level == 'write' and granted == 'write'
        )
        if not ok:
            raise PermissionDeniedError(
                code=f'permission.mission.{level}',
                message=(
                    f'Plugin {manifest.slug!r} called a mission helper '
                    f'requiring mission = {level!r} but declared '
                    f'mission = {granted!r}.'
                ),
            )

    def decorator(func: F) -> F:
        return _wrap(func, check)  # type: ignore[return-value]

    return decorator


__all__ = [
    'PermissionDeniedError',
    '_current_plugin_manifest',
    'clear_plugin_context',
    'current_plugin',
    'requires_mesh',
    'requires_mission',
    'requires_read',
    'requires_write',
    'set_plugin_context',
]


# Silence "imported but unused" for the ``Awaitable`` import — kept for
# documentation/typing clarity in IDEs that resolve coroutine returns.
_: type = Awaitable
