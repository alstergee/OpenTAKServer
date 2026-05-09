"""Tests for :mod:`opentakserver.sdk.modules.config`.

Run inside the OTS container::

    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_config.py
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
from opentakserver.sdk.modules import _paths, config
from opentakserver.sdk.permissions import (
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _manifest(slug: str = 'test-plugin') -> PluginManifest:
    """Minimum manifest for tests — A.1's full schema requires all of this."""
    return PluginManifest(
        api_version=2,
        name='Test',
        slug=slug,
        version='0.0.0',
        author='test',
        license='MIT',
        description='test fixture',
        permissions=PluginPermissions(),  # config doesn't gate on scopes
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
# Plugin context required
# ---------------------------------------------------------------------------


def test_get_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        config.get('foo')
    assert exc_info.value.code == 'config.no_plugin_context'


def test_set_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        config.set('foo', 'bar')
    assert exc_info.value.code == 'config.no_plugin_context'


def test_delete_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        config.delete('foo')
    assert exc_info.value.code == 'config.no_plugin_context'


def test_all_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        config.all()
    assert exc_info.value.code == 'config.no_plugin_context'


def test_clear_without_plugin_context_raises(isolated_root: Path) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        config.clear()
    assert exc_info.value.code == 'config.no_plugin_context'


# ---------------------------------------------------------------------------
# Roundtrip
# ---------------------------------------------------------------------------


def test_get_returns_default_when_missing(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        assert config.get('absent') is None
        assert config.get('absent', default='fallback') == 'fallback'


def test_set_then_get_roundtrip(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        config.set('greeting', 'hello')
        assert config.get('greeting') == 'hello'


def test_set_persists_complex_values(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        config.set('nested', {'a': [1, 2, 3], 'b': True})
        assert config.get('nested') == {'a': [1, 2, 3], 'b': True}


def test_delete_returns_true_when_removed(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        config.set('k', 'v')
        assert config.delete('k') is True
        assert config.get('k') is None


def test_delete_returns_false_when_absent(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        assert config.delete('never-set') is False


def test_all_returns_snapshot_copy(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        config.set('a', 1)
        config.set('b', 2)
        snap = config.all()
        assert snap == {'a': 1, 'b': 2}
        # Mutating the snapshot must not affect the persisted state.
        snap['a'] = 999
        assert config.get('a') == 1


def test_clear_wipes_everything(isolated_root: Path) -> None:
    with _plugin_context(_manifest()):
        config.set('a', 1)
        config.set('b', 2)
        config.clear()
        assert config.all() == {}


# ---------------------------------------------------------------------------
# Disk persistence — re-reads on every get()
# ---------------------------------------------------------------------------


def test_get_re_reads_disk(isolated_root: Path) -> None:
    """A separate process / external editor changing config.yml is picked up."""
    with _plugin_context(_manifest()):
        config.set('k', 'first')
        assert config.get('k') == 'first'

        # Simulate an external write (e.g. /api/plugins/v2/<slug>/config PATCH)
        config_file = isolated_root / 'test-plugin' / 'config.yml'
        config_file.write_text('k: second\n', encoding='utf-8')

        assert config.get('k') == 'second'


def test_writes_to_correct_per_plugin_path(isolated_root: Path) -> None:
    with _plugin_context(_manifest(slug='alpha')):
        config.set('only-alpha', True)
    with _plugin_context(_manifest(slug='beta')):
        config.set('only-beta', True)

    assert (isolated_root / 'alpha' / 'config.yml').exists()
    assert (isolated_root / 'beta' / 'config.yml').exists()

    # Each plugin sees only its own keys.
    with _plugin_context(_manifest(slug='alpha')):
        assert config.get('only-alpha') is True
        assert config.get('only-beta') is None


def test_atomic_write_leaves_no_temp_files(isolated_root: Path) -> None:
    """After a successful write the only file in the dir is config.yml."""
    with _plugin_context(_manifest()):
        config.set('k', 'v')

    plugin_dir = isolated_root / 'test-plugin'
    files = sorted(p.name for p in plugin_dir.iterdir())
    assert files == ['config.yml']


def test_corrupt_file_treated_as_empty(isolated_root: Path) -> None:
    """A corrupted YAML scalar is logged + recovered via the next set()."""
    plugin_dir = isolated_root / 'test-plugin'
    plugin_dir.mkdir(parents=True)
    (plugin_dir / 'config.yml').write_text('"just a string"', encoding='utf-8')

    with _plugin_context(_manifest()):
        # Bad file → treated as empty.
        assert config.all() == {}
        # set() recovers by overwriting.
        config.set('k', 'v')
        assert config.get('k') == 'v'
