"""``OTS.storage`` — per-plugin scoped filesystem.

Every v2 plugin gets a private directory at::

    /app/ots/plugins/<slug>/storage/

This module exposes a small filesystem API rooted there. All paths
supplied by callers are forced to remain inside the plugin's storage
directory (see :func:`_paths.safe_join`).

Permissions
-----------

* Writers (``write``, ``delete``) require ``write = ["storage"]`` in the
  plugin manifest.
* Readers (``read``, ``read_text``, ``exists``, ``list``, ``path``)
  require ``read = ["storage"]``.

The decorators short-circuit when no plugin context is active (so core
OTS code calling into the helpers from a maintenance task is not
blocked); plugin-context calls without the right scope raise
:class:`~opentakserver.sdk.permissions.PermissionDeniedError`.

Path safety
-----------

Every ``filename`` argument flows through
:func:`opentakserver.sdk.modules._paths.safe_join` which rejects:

* absolute paths
* path components containing ``..``
* anything that resolves outside the plugin's scoped directory

Violations raise :class:`OTSPluginError` with
``code='storage.path_traversal'``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from opentakserver.sdk.modules._paths import (
    atomic_write,
    safe_join,
    storage_dir,
)
from opentakserver.sdk.permissions import requires_read, requires_write

logger = logging.getLogger(__name__)


_NO_CTX = 'storage.no_plugin_context'


def _resolved(filename: str) -> Path:
    """Return the absolute path for ``filename`` under the plugin's storage dir.

    Wraps :func:`storage_dir` + :func:`safe_join`. Raises ``OTSPluginError``
    on path-traversal or missing plugin context.
    """

    base = storage_dir(error_code=_NO_CTX)
    return safe_join(base, filename)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


@requires_write('storage')
def write(filename: str, content: bytes | str) -> Path:
    """Write ``content`` to the plugin's scoped storage dir, atomically.

    Strings are encoded as UTF-8. Parent directories are created as needed.
    Returns the absolute :class:`Path` of the written file.
    """

    target = _resolved(filename)
    if isinstance(content, str):
        payload = content.encode('utf-8')
    elif isinstance(content, (bytes, bytearray, memoryview)):
        payload = bytes(content)
    else:
        from opentakserver.sdk.manifest import OTSPluginError

        raise OTSPluginError(
            code='storage.bad_content',
            message=(
                f'OTS.storage.write expects bytes or str, got '
                f'{type(content).__name__}'
            ),
        )
    atomic_write(target, payload)
    logger.info(
        'wrote plugin storage file (%d bytes) -> %s',
        len(payload), target,
        extra={'plugin': target.parent.parent.name},
    )
    return target


@requires_write('storage')
def delete(filename: str) -> bool:
    """Delete ``filename``. Returns ``True`` if removed, ``False`` if absent."""

    target = _resolved(filename)
    if not target.exists():
        return False
    if target.is_dir():
        from opentakserver.sdk.manifest import OTSPluginError

        raise OTSPluginError(
            code='storage.is_directory',
            message=(
                f'OTS.storage.delete refuses to delete directory {filename!r}; '
                f'this API is for files only.'
            ),
        )
    target.unlink()
    logger.info(
        'deleted plugin storage file: %s', target,
        extra={'plugin': target.parent.parent.name},
    )
    return True


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


@requires_read('storage')
def read(filename: str) -> bytes:
    """Read ``filename`` and return its bytes. Raises :class:`FileNotFoundError`."""

    return _resolved(filename).read_bytes()


@requires_read('storage')
def read_text(filename: str, encoding: str = 'utf-8') -> str:
    """Read ``filename`` as text. Same semantics as :func:`read` plus decode."""

    return _resolved(filename).read_text(encoding=encoding)


@requires_read('storage')
def exists(filename: str) -> bool:
    """Return whether ``filename`` exists in the plugin's storage dir."""

    return _resolved(filename).exists()


@requires_read('storage')
def list(subdir: str = '') -> list[str]:  # noqa: A001 — matches design doc
    """Return relative paths of every file under ``subdir`` (recursive).

    Paths are returned as POSIX strings (``a/b/c.json``) to keep the API
    cross-platform and to match the URL-style paths plugins typically
    write through this module. Directories themselves are not returned.
    """

    base = storage_dir(error_code=_NO_CTX)
    root = base if subdir == '' else safe_join(base, subdir)
    if not root.exists():
        return []
    if root.is_file():
        return [root.relative_to(base).as_posix()]

    out: list[str] = []
    for entry in root.rglob('*'):
        if entry.is_file():
            out.append(entry.relative_to(base).as_posix())
    out.sort()
    return out


@requires_read('storage')
def path(filename: str) -> Path:
    """Return the absolute :class:`Path` for ``filename`` without I/O.

    Useful when handing a path to a third-party library that only accepts
    a filesystem path. Path-safety checks still run.
    """

    return _resolved(filename)


__all__ = [
    'delete',
    'exists',
    'list',
    'path',
    'read',
    'read_text',
    'write',
]
