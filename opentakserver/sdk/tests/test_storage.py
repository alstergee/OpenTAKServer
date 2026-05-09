"""Tests for :mod:`opentakserver.sdk.modules.storage`.

Run inside the OTS container::

    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_storage.py
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path

import pytest

from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import _paths, storage
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _manifest(
    *,
    slug: str = 'test-plugin',
    read: list[str] | None = None,
    write: list[str] | None = None,
) -> PluginManifest:
    return PluginManifest(
        api_version=2,
        name='Test',
        slug=slug,
        version='0.0.0',
        author='test',
        license='MIT',
        description='test fixture',
        permissions=PluginPermissions(
            read=read if read is not None else ['storage'],
            write=write if write is not None else ['storage'],
        ),
        mount=[{'kind': 'tab', 'label': 'T', 'path': '/t'}],
    )


@contextlib.contextmanager
def _plugin_context(manifest: PluginManifest) -> Iterator[None]:
    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


@pytest.fixture
def isolated_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect ``PLUGINS_ROOT`` into a tmp dir for the test."""
    monkeypatch.setattr(_paths, 'PLUGINS_ROOT', tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# Plugin-context required
# ---------------------------------------------------------------------------


def test_write_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        storage.write('a.txt', 'hi')
    assert exc_info.value.code == 'storage.no_plugin_context'


def test_read_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        storage.read('a.txt')
    assert exc_info.value.code == 'storage.no_plugin_context'


# ---------------------------------------------------------------------------
# Roundtrip
# ---------------------------------------------------------------------------


def test_write_then_read_bytes_roundtrip(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        target = storage.write('a.bin', b'\x00\x01\x02')
        assert target.is_absolute()
        assert storage.read('a.bin') == b'\x00\x01\x02'


def test_write_then_read_text_roundtrip(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        storage.write('greeting.txt', 'hello, world')
        assert storage.read_text('greeting.txt') == 'hello, world'


def test_write_creates_subdirectories(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        storage.write('nested/deep/file.json', '{}')
        assert storage.exists('nested/deep/file.json') is True


def test_exists_returns_false_for_missing(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        assert storage.exists('not-there.txt') is False


def test_read_missing_raises_filenotfound(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        with pytest.raises(FileNotFoundError):
            storage.read('not-there.txt')


def test_delete_returns_true_when_removed(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        storage.write('byebye.txt', 'data')
        assert storage.delete('byebye.txt') is True
        assert storage.exists('byebye.txt') is False


def test_delete_returns_false_when_absent(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        assert storage.delete('not-there.txt') is False


def test_path_returns_absolute_without_io(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        p = storage.path('config.json')
        assert p.is_absolute()
        # path() must NOT create the file.
        assert not p.exists()
        # ...nor write parents (storage_dir creation is OK; actual file no).
        assert p.parent.exists()


def test_path_handed_to_third_party_lib(isolated_root: Path) -> None:
    """Demonstrates the documented use-case: pass to a non-SDK API."""
    with _plugin_context(_manifest()):
        storage.write('blob.bin', b'data')
        p = storage.path('blob.bin')
        # Pretend a 3rd-party lib calls path.read_bytes() directly.
        assert p.read_bytes() == b'data'


# ---------------------------------------------------------------------------
# list()
# ---------------------------------------------------------------------------


def test_list_returns_recursive_relative_posix_paths(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        storage.write('a.txt', 'A')
        storage.write('sub/b.txt', 'B')
        storage.write('sub/deeper/c.txt', 'C')

        listing = storage.list()
        assert listing == ['a.txt', 'sub/b.txt', 'sub/deeper/c.txt']


def test_list_filters_to_subdir(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        storage.write('a.txt', 'A')
        storage.write('sub/b.txt', 'B')
        storage.write('sub/deeper/c.txt', 'C')

        sub_listing = storage.list('sub')
        assert sub_listing == ['sub/b.txt', 'sub/deeper/c.txt']


def test_list_empty_when_subdir_missing(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        assert storage.list('does-not-exist') == []


# ---------------------------------------------------------------------------
# Path traversal rejection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'bad',
    [
        '../escape.txt',
        '../../etc/passwd',
        'subdir/../../escape.txt',
        '/etc/passwd',
        '/tmp/file.txt',
        '',
    ],
)
def test_write_rejects_traversal(isolated_root: Path, bad: str) -> None:
    with _plugin_context(_manifest()):
        with pytest.raises(OTSPluginError) as exc_info:
            storage.write(bad, 'pwn')
        assert exc_info.value.code == 'storage.path_traversal'


@pytest.mark.parametrize(
    'bad',
    ['../etc/passwd', '/etc/passwd', 'a/../../escape'],
)
def test_read_rejects_traversal(isolated_root: Path, bad: str) -> None:
    with _plugin_context(_manifest()):
        with pytest.raises(OTSPluginError) as exc_info:
            storage.read(bad)
        assert exc_info.value.code == 'storage.path_traversal'


def test_path_rejects_traversal(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        with pytest.raises(OTSPluginError) as exc_info:
            storage.path('../escape')
        assert exc_info.value.code == 'storage.path_traversal'


# ---------------------------------------------------------------------------
# Permission gating
# ---------------------------------------------------------------------------


def test_write_requires_write_storage_scope(isolated_root: Path) -> None:
    with _plugin_context(_manifest(write=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            storage.write('a.txt', 'x')
        assert exc_info.value.code == 'permission.write.storage'


def test_delete_requires_write_storage_scope(isolated_root: Path) -> None:
    with _plugin_context(_manifest(write=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            storage.delete('a.txt')
        assert exc_info.value.code == 'permission.write.storage'


def test_read_requires_read_storage_scope(isolated_root: Path) -> None:
    with _plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            storage.read('a.txt')
        assert exc_info.value.code == 'permission.read.storage'


def test_exists_requires_read_storage_scope(isolated_root: Path) -> None:
    with _plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            storage.exists('a.txt')
        assert exc_info.value.code == 'permission.read.storage'


def test_list_requires_read_storage_scope(isolated_root: Path) -> None:
    with _plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            storage.list()
        assert exc_info.value.code == 'permission.read.storage'


def test_path_requires_read_storage_scope(isolated_root: Path) -> None:
    with _plugin_context(_manifest(read=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            storage.path('a.txt')
        assert exc_info.value.code == 'permission.read.storage'


# ---------------------------------------------------------------------------
# Per-plugin isolation
# ---------------------------------------------------------------------------


def test_each_plugin_gets_its_own_dir(isolated_root: Path) -> None:
    with _plugin_context(_manifest(slug='alpha')):
        storage.write('shared-name.txt', 'alpha-data')
    with _plugin_context(_manifest(slug='beta')):
        storage.write('shared-name.txt', 'beta-data')

    assert (isolated_root / 'alpha' / 'storage' / 'shared-name.txt').exists()
    assert (isolated_root / 'beta' / 'storage' / 'shared-name.txt').exists()

    with _plugin_context(_manifest(slug='alpha')):
        assert storage.read_text('shared-name.txt') == 'alpha-data'
    with _plugin_context(_manifest(slug='beta')):
        assert storage.read_text('shared-name.txt') == 'beta-data'


# ---------------------------------------------------------------------------
# Misc edge cases
# ---------------------------------------------------------------------------


def test_write_rejects_non_bytes_or_str(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        with pytest.raises(OTSPluginError) as exc_info:
            storage.write('a.txt', 123)  # type: ignore[arg-type]
        assert exc_info.value.code == 'storage.bad_content'


def test_delete_directory_refused(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        storage.write('subdir/a.txt', 'x')
        with pytest.raises(OTSPluginError) as exc_info:
            storage.delete('subdir')
        assert exc_info.value.code == 'storage.is_directory'
