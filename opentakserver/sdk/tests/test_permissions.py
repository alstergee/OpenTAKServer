"""Tests for :mod:`opentakserver.sdk.permissions`.

Run inside the OTS container:
    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_permissions.py
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator

import pytest

from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    current_plugin,
    requires_mesh,
    requires_mission,
    requires_read,
    requires_write,
    set_plugin_context,
)


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    """Push ``manifest`` for the duration of the ``with`` block."""

    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


def _manifest(
    *,
    read: list[str] | None = None,
    write: list[str] | None = None,
    mesh: bool = False,
    mission: str = 'none',
) -> PluginManifest:
    # A.1's full schema requires api_version + name + version + author + license
    # + description + at least one mount. The mount body is scaffolding — these
    # tests only exercise the permission decorators, not the manifest schema.
    return PluginManifest(
        api_version=2,
        name='Test',
        slug='test-plugin',
        version='0.0.0',
        author='test',
        license='MIT',
        description='test fixture',
        permissions=PluginPermissions(
            read=read or [],
            write=write or [],
            mesh=mesh,
            mission=mission,  # type: ignore[arg-type]
        ),
        mount=[{'kind': 'tab', 'label': 'Test', 'path': '/test'}],
    )


# ---------------------------------------------------------------------------
# requires_read
# ---------------------------------------------------------------------------


def test_requires_read_raises_when_scope_missing() -> None:
    @requires_read('eud')
    def list_euds() -> str:
        return 'ok'

    with plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            list_euds()

    assert exc_info.value.code == 'permission.read.eud'
    assert isinstance(exc_info.value, OTSPluginError)


def test_requires_read_passes_when_scope_declared() -> None:
    @requires_read('eud')
    def list_euds() -> str:
        return 'ok'

    with plugin_context(_manifest(read=['eud'])):
        assert list_euds() == 'ok'


def test_requires_read_passes_when_no_plugin_context() -> None:
    """Core OTS code (no plugin context) should not be blocked."""

    @requires_read('eud')
    def list_euds() -> str:
        return 'ok'

    assert current_plugin() is None
    assert list_euds() == 'ok'


def test_requires_read_multi_scope_raises_on_first_missing() -> None:
    @requires_read('eud', 'geochat')
    def fetch() -> str:
        return 'ok'

    with plugin_context(_manifest(read=['eud'])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            fetch()

    assert exc_info.value.code == 'permission.read.geochat'


def test_requires_read_with_no_scopes_is_a_programming_error() -> None:
    with pytest.raises(ValueError):
        requires_read()


# ---------------------------------------------------------------------------
# requires_write
# ---------------------------------------------------------------------------


def test_requires_write_raises_with_correct_code() -> None:
    @requires_write('kml')
    def write_kml() -> str:
        return 'wrote'

    with plugin_context(_manifest(write=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            write_kml()

    assert exc_info.value.code == 'permission.write.kml'


def test_requires_write_passes_when_declared() -> None:
    @requires_write('kml')
    def write_kml() -> str:
        return 'wrote'

    with plugin_context(_manifest(write=['kml'])):
        assert write_kml() == 'wrote'


# ---------------------------------------------------------------------------
# requires_mesh
# ---------------------------------------------------------------------------


def test_requires_mesh_raises_when_false() -> None:
    @requires_mesh()
    def publish() -> str:
        return 'sent'

    with plugin_context(_manifest(mesh=False)):
        with pytest.raises(PermissionDeniedError) as exc_info:
            publish()

    assert exc_info.value.code == 'permission.mesh'


def test_requires_mesh_passes_when_true() -> None:
    @requires_mesh()
    def publish() -> str:
        return 'sent'

    with plugin_context(_manifest(mesh=True)):
        assert publish() == 'sent'


# ---------------------------------------------------------------------------
# requires_mission
# ---------------------------------------------------------------------------


def test_requires_mission_write_raises_when_only_read() -> None:
    @requires_mission('write')
    def add_content() -> str:
        return 'added'

    with plugin_context(_manifest(mission='read')):
        with pytest.raises(PermissionDeniedError) as exc_info:
            add_content()

    assert exc_info.value.code == 'permission.mission.write'


def test_requires_mission_write_passes_when_write_granted() -> None:
    @requires_mission('write')
    def add_content() -> str:
        return 'added'

    with plugin_context(_manifest(mission='write')):
        assert add_content() == 'added'


def test_requires_mission_read_passes_when_read_or_write() -> None:
    @requires_mission('read')
    def list_missions() -> str:
        return 'listed'

    with plugin_context(_manifest(mission='read')):
        assert list_missions() == 'listed'

    with plugin_context(_manifest(mission='write')):
        assert list_missions() == 'listed'


def test_requires_mission_read_raises_when_none() -> None:
    @requires_mission('read')
    def list_missions() -> str:
        return 'listed'

    with plugin_context(_manifest(mission='none')):
        with pytest.raises(PermissionDeniedError) as exc_info:
            list_missions()

    assert exc_info.value.code == 'permission.mission.read'


def test_requires_mission_invalid_level() -> None:
    with pytest.raises(ValueError):
        requires_mission('admin')  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Async support
# ---------------------------------------------------------------------------


def test_requires_read_works_on_async_function() -> None:
    @requires_read('eud')
    async def list_euds_async() -> str:
        return 'ok'

    # Pass case
    async def run_pass() -> str:
        with plugin_context(_manifest(read=['eud'])):
            return await list_euds_async()

    assert asyncio.run(run_pass()) == 'ok'

    # Fail case
    async def run_fail() -> str:
        with plugin_context(_manifest(read=[])):
            return await list_euds_async()

    with pytest.raises(PermissionDeniedError) as exc_info:
        asyncio.run(run_fail())
    assert exc_info.value.code == 'permission.read.eud'


def test_async_function_preserves_name_and_docstring() -> None:
    @requires_read('eud')
    async def list_euds_async() -> str:
        '''Async docstring stays put.'''
        return 'ok'

    assert list_euds_async.__name__ == 'list_euds_async'
    assert list_euds_async.__doc__ == 'Async docstring stays put.'


def test_sync_function_preserves_name_and_docstring() -> None:
    @requires_write('kml')
    def write_kml() -> str:
        '''Sync docstring stays put.'''
        return 'ok'

    assert write_kml.__name__ == 'write_kml'
    assert write_kml.__doc__ == 'Sync docstring stays put.'


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------


def test_current_plugin_returns_none_outside_context() -> None:
    assert current_plugin() is None


def test_current_plugin_returns_manifest_inside_context() -> None:
    m = _manifest(read=['eud'])
    with plugin_context(m):
        assert current_plugin() is m
    assert current_plugin() is None


def test_set_and_clear_plugin_context_token_roundtrip() -> None:
    m = _manifest()
    token = set_plugin_context(m)
    try:
        assert current_plugin() is m
    finally:
        clear_plugin_context(token)
    assert current_plugin() is None
