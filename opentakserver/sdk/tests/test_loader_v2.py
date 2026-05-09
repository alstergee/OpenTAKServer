"""Tests for :mod:`opentakserver.sdk.loader_v2`.

Run inside the OTS container:
    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_loader_v2.py
"""

from __future__ import annotations

import importlib.metadata
import logging
import sys
import textwrap
import types
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from flask import Flask

from opentakserver.sdk.loader_v2 import (
    ENTRY_POINT_GROUP,
    PluginManagerV2,
    _with_plugin_context,
)
from opentakserver.sdk.manifest import PluginManifest
from opentakserver.sdk.mount_registry import mount_registry
from opentakserver.sdk.permissions import current_plugin


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


VALID_TOML = textwrap.dedent(
    """
    [plugin]
    api_version = 2
    name = "Fake Plugin"
    slug = "fake-plugin"
    version = "0.1.0"
    author = "Tester"
    license = "MIT"
    description = "Fixture for loader_v2 tests"

    [plugin.permissions]
    read = ["eud"]
    write = []

    [[plugin.mount]]
    kind = "tab"
    label = "Fake Tab"
    path = "/plugin/fake"
    icon = "tabler:icons:flag"
    roles = ["administrator"]

    [[plugin.mount]]
    kind = "map_overlay"
    label = "Fake Overlay"
    endpoint = "/overlay.json"
    """
).strip()


MALFORMED_TOML = textwrap.dedent(
    """
    [plugin]
    api_version = 2
    name = "Broken"
    slug = "BAD SLUG WITH SPACES"
    version = "x"
    """
).strip()


def _write_fake_package(
    tmp_path: Path,
    package_name: str,
    toml_body: str | None,
) -> Path:
    """Create a fake plugin package directory and (optionally) plugin.toml.

    Returns the package's ``__init__.py`` path (matches how an installed
    pip distro looks: ``site-packages/<package_name>/__init__.py``).
    """
    pkg_dir = tmp_path / package_name
    pkg_dir.mkdir()
    init_py = pkg_dir / '__init__.py'
    init_py.write_text('# fake plugin\n')
    if toml_body is not None:
        (pkg_dir / 'plugin.toml').write_text(toml_body)
    return init_py


@pytest.fixture
def fake_plugin_package(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """Set up a real importable Python package on disk + on sys.modules.

    Yields ``(package_name, init_py_path)``. Cleans up sys.modules after.
    """
    package_name = 'fake_plugin_pkg'
    init_py = _write_fake_package(tmp_path, package_name, VALID_TOML)

    # Manually register the package in sys.modules so import_module finds it
    # without poking at sys.path. This matches how a real pip-installed
    # plugin would behave (its __file__ points at site-packages).
    module = types.ModuleType(package_name)
    module.__file__ = str(init_py)
    module.__path__ = [str(init_py.parent)]
    sys.modules[package_name] = module

    try:
        yield package_name, init_py
    finally:
        sys.modules.pop(package_name, None)


@pytest.fixture
def vanilla_plugin_package(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """A plugin package WITHOUT a plugin.toml — must be skipped silently."""
    package_name = 'vanilla_plugin_pkg'
    init_py = _write_fake_package(tmp_path, package_name, toml_body=None)
    module = types.ModuleType(package_name)
    module.__file__ = str(init_py)
    module.__path__ = [str(init_py.parent)]
    sys.modules[package_name] = module
    try:
        yield package_name, init_py
    finally:
        sys.modules.pop(package_name, None)


@pytest.fixture
def malformed_plugin_package(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """A plugin package whose plugin.toml fails Pydantic validation."""
    package_name = 'malformed_plugin_pkg'
    init_py = _write_fake_package(tmp_path, package_name, MALFORMED_TOML)
    module = types.ModuleType(package_name)
    module.__file__ = str(init_py)
    module.__path__ = [str(init_py.parent)]
    sys.modules[package_name] = module
    try:
        yield package_name, init_py
    finally:
        sys.modules.pop(package_name, None)


def _fake_entry_point(name: str, package: str) -> importlib.metadata.EntryPoint:
    """Build an importlib.metadata.EntryPoint that loads ``<package>.__init__``."""
    return importlib.metadata.EntryPoint(
        name=name,
        value=f'{package}:__name__',  # ep.module = package, ep.attr = __name__
        group=ENTRY_POINT_GROUP,
    )


@pytest.fixture
def app() -> Flask:
    """A bare Flask app — no extensions, no apscheduler."""
    return Flask(__name__)


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    """Wipe any prior test's mount-registry leftovers."""
    # Snapshot + clear
    saved = dict(mount_registry._mounts)  # noqa: SLF001 — test introspection
    saved_versions = dict(mount_registry._versions)  # noqa: SLF001
    with mount_registry._lock:  # noqa: SLF001
        mount_registry._mounts.clear()  # noqa: SLF001
        mount_registry._versions.clear()  # noqa: SLF001
    try:
        yield
    finally:
        with mount_registry._lock:  # noqa: SLF001
            mount_registry._mounts.clear()  # noqa: SLF001
            mount_registry._versions.clear()  # noqa: SLF001
            mount_registry._mounts.update(saved)  # noqa: SLF001
            mount_registry._versions.update(saved_versions)  # noqa: SLF001


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------


def test_discover_finds_v2_manifest(
    app: Flask,
    fake_plugin_package: tuple[str, Path],
) -> None:
    """``discover`` resolves entry-points to validated PluginManifest objects."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        manifests = pm.discover()

    assert len(manifests) == 1
    m = manifests[0]
    assert isinstance(m, PluginManifest)
    assert m.slug == 'fake-plugin'
    assert m.version == '0.1.0'
    assert len(m.mount) == 2


def test_discover_skips_vanilla_plugin_silently(
    app: Flask,
    vanilla_plugin_package: tuple[str, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Plugin without plugin.toml is silently skipped (vanilla compat)."""
    package_name, _ = vanilla_plugin_package
    ep = _fake_entry_point('vanilla_plugin', package_name)

    pm = PluginManagerV2(app)
    with caplog.at_level(logging.DEBUG, logger='opentakserver.sdk.loader_v2'):
        with mock.patch(
            'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
            return_value=[ep],
        ):
            manifests = pm.discover()

    assert manifests == []
    # Must NOT log at WARNING/ERROR — vanilla is the legacy loader's job.
    severe = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert severe == [], f'unexpected severe log lines: {[r.message for r in severe]}'


def test_discover_logs_and_skips_malformed_manifest(
    app: Flask,
    malformed_plugin_package: tuple[str, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Plugin with broken plugin.toml is error-logged but does NOT crash discover."""
    package_name, _ = malformed_plugin_package
    ep = _fake_entry_point('broken_plugin', package_name)

    pm = PluginManagerV2(app)
    with caplog.at_level(logging.ERROR, logger='opentakserver.sdk.loader_v2'):
        with mock.patch(
            'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
            return_value=[ep],
        ):
            # Must not raise.
            manifests = pm.discover()

    assert manifests == []
    assert any(
        'invalid plugin.toml' in r.message for r in caplog.records
    ), f'expected error log; got {[r.message for r in caplog.records]}'


def test_discover_with_no_entry_points_returns_empty(app: Flask) -> None:
    """No entry points = empty list (regression guard for vanilla compat)."""
    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[],
    ):
        assert pm.discover() == []


# ---------------------------------------------------------------------------
# register()
# ---------------------------------------------------------------------------


def test_register_pushes_mounts_into_registry(
    app: Flask,
    fake_plugin_package: tuple[str, Path],
) -> None:
    """``register`` adds the plugin's mounts to the global mount_registry."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        manifest = pm.discover()[0]
    pm.register(manifest)

    by_plugin = mount_registry.by_plugin('fake-plugin')
    assert len(by_plugin) == 2
    kinds = {r['kind'] for r in by_plugin}
    assert kinds == {'tab', 'map_overlay'}
    assert pm.by_slug('fake-plugin') is manifest
    assert pm.manifests() == [manifest]


def test_register_creates_blueprint_under_api_plugins_slug(
    app: Flask,
    fake_plugin_package: tuple[str, Path],
) -> None:
    """A ``tab`` mount produces a route under ``/api/plugins/<slug>/...``."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        manifest = pm.discover()[0]
    pm.register(manifest)

    rules = [str(r) for r in app.url_map.iter_rules()]
    assert any(
        rule.startswith('/api/plugins/fake-plugin') for rule in rules
    ), f'expected blueprint mounted at /api/plugins/fake-plugin/, got: {rules}'


# ---------------------------------------------------------------------------
# reload()
# ---------------------------------------------------------------------------


def test_reload_unregisters_then_reregisters(
    app: Flask,
    fake_plugin_package: tuple[str, Path],
) -> None:
    """``reload`` keeps the mount count stable (unregister + re-register)."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        manifest = pm.discover()[0]
        pm.register(manifest)
        before = len(mount_registry.by_plugin('fake-plugin'))
        assert before == 2

        result = pm.reload('fake-plugin')

    assert result is True
    after = len(mount_registry.by_plugin('fake-plugin'))
    assert after == before == 2
    assert pm.by_slug('fake-plugin') is not None


def test_reload_unknown_slug_returns_false(
    app: Flask,
    fake_plugin_package: tuple[str, Path],
) -> None:
    """Reloading a slug that doesn't exist in the entry-point group returns False."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        # Never registered — reload should return False.
        assert pm.reload('not-a-real-plugin') is False


# ---------------------------------------------------------------------------
# Plugin-context wrapper
# ---------------------------------------------------------------------------


def test_with_plugin_context_sets_current_plugin_during_call(
    fake_plugin_package: tuple[str, Path],
    app: Flask,
) -> None:
    """The plugin context wrapper exposes ``current_plugin()`` to the wrapped fn."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        manifest = pm.discover()[0]

    seen: list[PluginManifest | None] = []

    def fn() -> str:
        seen.append(current_plugin())
        return 'ok'

    wrapped = _with_plugin_context(manifest, fn)

    # Outside the wrapper there is no plugin context.
    assert current_plugin() is None
    assert wrapped() == 'ok'
    # Inside the wrapped call, current_plugin() resolved to our manifest.
    assert seen == [manifest]
    # And the context was cleared after the call.
    assert current_plugin() is None


def test_with_plugin_context_clears_context_on_exception(
    fake_plugin_package: tuple[str, Path],
    app: Flask,
) -> None:
    """Exceptions from the wrapped function still clear the plugin context."""
    package_name, _ = fake_plugin_package
    ep = _fake_entry_point('fake_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        manifest = pm.discover()[0]

    def boom() -> None:
        raise RuntimeError('boom')

    wrapped = _with_plugin_context(manifest, boom)
    with pytest.raises(RuntimeError, match='boom'):
        wrapped()
    assert current_plugin() is None


# ---------------------------------------------------------------------------
# Vanilla compatibility regression guard
# ---------------------------------------------------------------------------


def test_vanilla_entry_point_does_not_raise_from_discover(
    app: Flask,
    vanilla_plugin_package: tuple[str, Path],
) -> None:
    """An entry-point WITHOUT plugin.toml must NOT raise from discover().

    This is the hard-rule regression guard: vanilla plugins keep loading
    via the legacy PluginManager. Loader v2 must coexist without raising.
    """
    package_name, _ = vanilla_plugin_package
    ep = _fake_entry_point('vanilla_plugin', package_name)

    pm = PluginManagerV2(app)
    with mock.patch(
        'opentakserver.sdk.loader_v2.importlib.metadata.entry_points',
        return_value=[ep],
    ):
        # The crucial assertion: this must NOT raise.
        manifests = pm.discover()

    assert manifests == []
    # And no mounts ended up in the registry from a vanilla plugin.
    assert mount_registry.by_plugin(package_name) == []
