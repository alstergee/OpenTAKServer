"""Internal helpers shared by ``OTS.config`` and ``OTS.storage``.

Both modules persist data into a per-plugin scoped directory rooted at
``PLUGINS_ROOT``. ``PLUGINS_ROOT`` defaults to ``/app/ots/plugins`` (the
production layout — see *D-1* in the architecture map). Tests should
monkeypatch this module-level attribute via :func:`set_plugins_root` or by
direct ``monkeypatch.setattr`` so each test gets an isolated ``tmp_path``
sandbox.

Path safety
-----------

:func:`safe_join` is the single entry point used by both modules to turn a
caller-supplied ``filename`` into a real :class:`~pathlib.Path` underneath
``base``. It rejects:

* absolute paths (``/etc/passwd``)
* path components containing ``..`` (``../../etc/passwd``)
* any final resolved path that escapes ``base``

Violations raise :class:`~opentakserver.sdk.manifest.OTSPluginError` with
``code='storage.path_traversal'``. The same code is used for both ``config``
and ``storage`` callers because the underlying defect is identical — a
plugin trying to escape its scoped directory.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Final

from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.permissions import current_plugin

#: Root directory holding every plugin's per-plugin scoped data. Tests
#: monkeypatch this to redirect both ``config`` and ``storage`` writes into a
#: ``tmp_path`` sandbox.
PLUGINS_ROOT: Path = Path('/app/ots/plugins')


def set_plugins_root(path: str | Path) -> None:
    """Override the on-disk root for plugin-scoped data (test hook)."""

    global PLUGINS_ROOT
    PLUGINS_ROOT = Path(path)


def _require_plugin_slug(error_code: str) -> str:
    """Return the active plugin's slug or raise ``OTSPluginError``.

    ``error_code`` is the SDK error code to surface — callers pass a
    namespaced code (e.g. ``'config.no_plugin_context'``) so the dashboard
    can distinguish config errors from storage errors.
    """

    manifest = current_plugin()
    if manifest is None:
        raise OTSPluginError(
            code=error_code,
            message=(
                'OTS.config / OTS.storage requires a plugin context. '
                'Call this helper from a v2 plugin entry point, not from '
                'core OTS code.'
            ),
        )
    return manifest.slug


def plugin_dir(*, error_code: str) -> Path:
    """Return ``PLUGINS_ROOT/<slug>`` for the active plugin, creating it."""

    slug = _require_plugin_slug(error_code)
    p = PLUGINS_ROOT / slug
    p.mkdir(parents=True, exist_ok=True)
    return p


def storage_dir(*, error_code: str) -> Path:
    """Return ``PLUGINS_ROOT/<slug>/storage`` for the active plugin, creating it."""

    p = plugin_dir(error_code=error_code) / 'storage'
    p.mkdir(parents=True, exist_ok=True)
    return p


_PATH_TRAVERSAL_CODE: Final[str] = 'storage.path_traversal'


def safe_join(base: Path, filename: str) -> Path:
    """Resolve ``filename`` underneath ``base`` or raise on escape.

    The check rejects absolute paths and any ``..`` component up front
    (cheap textual check) and then re-validates after :meth:`Path.resolve`
    using :meth:`PurePath.is_relative_to` so symlink-swap or unicode
    normalisation tricks can't smuggle a path out of ``base``.
    """

    if not isinstance(filename, str) or filename == '':
        raise OTSPluginError(
            code=_PATH_TRAVERSAL_CODE,
            message=f'filename must be a non-empty string, got {filename!r}',
        )

    # Reject anything that smells like an absolute path or a parent traversal
    # before we touch the filesystem.
    posix = PurePosixPath(filename)
    if posix.is_absolute() or os.path.isabs(filename):
        raise OTSPluginError(
            code=_PATH_TRAVERSAL_CODE,
            message=f'filename must be relative, got absolute: {filename!r}',
        )
    if any(part == '..' for part in posix.parts):
        raise OTSPluginError(
            code=_PATH_TRAVERSAL_CODE,
            message=f'filename must not contain ".." components: {filename!r}',
        )

    base_resolved = base.resolve()
    candidate = (base / filename).resolve()
    if not candidate.is_relative_to(base_resolved):
        raise OTSPluginError(
            code=_PATH_TRAVERSAL_CODE,
            message=(
                f'filename {filename!r} resolves outside the plugin scope '
                f'({candidate} not under {base_resolved})'
            ),
        )
    return candidate


def atomic_write(target: Path, content: bytes) -> None:
    """Write ``content`` to ``target`` atomically.

    Strategy: write to a sibling tempfile in the same directory (so the
    rename is on the same filesystem), ``fsync``, then ``os.replace`` over
    the destination. Parent directories are created on demand. Existing
    file mode is preserved when present; otherwise we leave umask defaults.
    """

    target.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        prefix=f'.{target.name}.',
        suffix='.tmp',
        dir=str(target.parent),
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, 'wb') as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        # Preserve mode of an existing target if present.
        if target.exists():
            try:
                tmp.chmod(target.stat().st_mode & 0o777)
            except OSError:
                # Mode preservation is best-effort — don't fail the write.
                pass
        os.replace(tmp, target)
    except BaseException:
        # Clean up the tempfile on any failure path so we don't leave
        # ``.config.yml.<rand>.tmp`` litter behind.
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


__all__ = [
    'PLUGINS_ROOT',
    'atomic_write',
    'plugin_dir',
    'safe_join',
    'set_plugins_root',
    'storage_dir',
]
