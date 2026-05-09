"""Tests for the v2 plugin mount registry."""

from __future__ import annotations

import threading

import pytest

from opentakserver.sdk.manifest import (
    MapOverlayMount,
    OTSPluginError,
    TabMount,
)
from opentakserver.sdk.mount_registry import MountRegistry, mount_registry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry() -> MountRegistry:
    """A fresh MountRegistry per test (the module singleton is shared)."""
    return MountRegistry()


@pytest.fixture
def tab_mount() -> TabMount:
    return TabMount(
        kind='tab',
        label='Map Marker',
        icon='tabler:icons:map-pin',
        path='/plugin/mapmarker',
        roles=['administrator'],
    )


@pytest.fixture
def overlay_mount() -> MapOverlayMount:
    return MapOverlayMount(
        kind='map_overlay',
        label='Map Marker pins',
        endpoint='/overlays/mapmarker.json',
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_register_then_all_returns_records(
    registry: MountRegistry,
    tab_mount: TabMount,
    overlay_mount: MapOverlayMount,
) -> None:
    """``register`` then ``all`` round-trips with full record shape."""
    registry.register(
        'ots-mapmarker-plugin',
        [tab_mount, overlay_mount],
        plugin_version='1.1',
    )

    records = registry.all()
    assert len(records) == 2

    tab_record = next(r for r in records if r['kind'] == 'tab')
    assert tab_record['label'] == 'Map Marker'
    assert tab_record['icon'] == 'tabler:icons:map-pin'
    assert tab_record['path'] == '/plugin/mapmarker'
    assert tab_record['roles'] == ['administrator']
    assert tab_record['_plugin'] == 'ots-mapmarker-plugin'
    assert tab_record['_version'] == '1.1'

    overlay_record = next(r for r in records if r['kind'] == 'map_overlay')
    assert overlay_record['label'] == 'Map Marker pins'
    # D-8: relative endpoints are auto-prefixed with the plugin's blueprint
    # so the UI can ``axios.get(mount.endpoint)`` without manually prepending.
    assert overlay_record['endpoint'] == (
        '/api/plugins/ots-mapmarker-plugin/overlays/mapmarker.json'
    )
    assert overlay_record['_plugin'] == 'ots-mapmarker-plugin'
    assert overlay_record['_version'] == '1.1'


def test_register_twice_replaces_not_appends(
    registry: MountRegistry,
    tab_mount: TabMount,
    overlay_mount: MapOverlayMount,
) -> None:
    """Re-registering the same slug REPLACES the previous mounts."""
    registry.register('ots-mapmarker-plugin', [tab_mount, overlay_mount], plugin_version='1.0')
    assert len(registry.all()) == 2

    new_tab = TabMount(kind='tab', label='Renamed Tab', path='/plugin/mapmarker', roles=[])
    registry.register('ots-mapmarker-plugin', [new_tab], plugin_version='1.1')

    records = registry.all()
    assert len(records) == 1
    assert records[0]['label'] == 'Renamed Tab'
    assert records[0]['_version'] == '1.1'


def test_unregister_removes(
    registry: MountRegistry,
    tab_mount: TabMount,
) -> None:
    """``unregister`` drops the slug's records from ``all``."""
    registry.register('ots-mapmarker-plugin', [tab_mount], plugin_version='1.1')
    assert len(registry.all()) == 1

    registry.unregister('ots-mapmarker-plugin')
    assert registry.all() == []
    assert registry.by_plugin('ots-mapmarker-plugin') == []


def test_unregister_unknown_slug_is_noop(
    registry: MountRegistry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unregistering a slug that was never registered warns but doesn't raise."""
    import logging

    with caplog.at_level(logging.WARNING, logger='opentakserver.sdk.mount_registry'):
        registry.unregister('never-registered-plugin')

    assert any('unregister called for unknown' in r.message for r in caplog.records)
    assert registry.all() == []


def test_by_kind_filters(
    registry: MountRegistry,
    tab_mount: TabMount,
    overlay_mount: MapOverlayMount,
) -> None:
    """``by_kind`` returns only records with the matching discriminator."""
    registry.register('p1', [tab_mount, overlay_mount], plugin_version='1.0')

    tabs = registry.by_kind('tab')
    overlays = registry.by_kind('map_overlay')
    drawers = registry.by_kind('map_drawer')

    assert len(tabs) == 1
    assert tabs[0]['kind'] == 'tab'
    assert len(overlays) == 1
    assert overlays[0]['kind'] == 'map_overlay'
    assert drawers == []


def test_by_plugin_filters(
    registry: MountRegistry,
    tab_mount: TabMount,
    overlay_mount: MapOverlayMount,
) -> None:
    """``by_plugin`` returns only the records owned by the named slug."""
    registry.register('plugin-a', [tab_mount], plugin_version='1.0')
    registry.register('plugin-b', [overlay_mount], plugin_version='2.5')

    a_records = registry.by_plugin('plugin-a')
    b_records = registry.by_plugin('plugin-b')

    assert len(a_records) == 1
    assert a_records[0]['_plugin'] == 'plugin-a'
    assert a_records[0]['kind'] == 'tab'

    assert len(b_records) == 1
    assert b_records[0]['_plugin'] == 'plugin-b'
    assert b_records[0]['_version'] == '2.5'
    assert b_records[0]['kind'] == 'map_overlay'

    assert registry.by_plugin('plugin-c-nonexistent') == []


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_register_with_empty_mounts_raises(registry: MountRegistry) -> None:
    """``register`` with ``mounts=[]`` raises ``OTSPluginError(code='mount.empty')``."""
    with pytest.raises(OTSPluginError) as exc_info:
        registry.register('ots-mapmarker-plugin', [], plugin_version='1.0')

    assert exc_info.value.code == 'mount.empty'
    assert 'ots-mapmarker-plugin' in exc_info.value.message
    # Failed register must not pollute state.
    assert registry.all() == []


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_register_from_multiple_threads(registry: MountRegistry) -> None:
    """Four threads register four different slugs; the final state has all four."""
    barrier = threading.Barrier(4)
    errors: list[BaseException] = []

    def worker(slug: str) -> None:
        try:
            barrier.wait(timeout=5)
            mount = TabMount(kind='tab', label=f'tab-{slug}', path=f'/plugin/{slug}')
            registry.register(slug, [mount], plugin_version='1.0')
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            errors.append(exc)

    slugs = ['plugin-1', 'plugin-2', 'plugin-3', 'plugin-4']
    threads = [threading.Thread(target=worker, args=(s,)) for s in slugs]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert errors == []
    records = registry.all()
    assert len(records) == 4

    seen_slugs = {r['_plugin'] for r in records}
    assert seen_slugs == set(slugs)


# ---------------------------------------------------------------------------
# D-8: endpoint auto-prefixing
# ---------------------------------------------------------------------------


def test_endpoint_already_prefixed_is_idempotent(
    registry: MountRegistry,
) -> None:
    """Re-serialising must not double-prefix already-prefixed endpoints."""
    mount = MapOverlayMount(
        kind='map_overlay',
        label='Pre-prefixed',
        endpoint='/api/plugins/ots-x/overlays/data.json',
    )
    registry.register('ots-x', [mount], plugin_version='1.0')

    record = registry.by_plugin('ots-x')[0]
    assert record['endpoint'] == '/api/plugins/ots-x/overlays/data.json'

    # And after a re-register (hot-reload simulation):
    registry.register('ots-x', [mount], plugin_version='1.0')
    record = registry.by_plugin('ots-x')[0]
    assert record['endpoint'] == '/api/plugins/ots-x/overlays/data.json'


# ---------------------------------------------------------------------------
# Module-level singleton smoke test
# ---------------------------------------------------------------------------


def test_module_singleton_is_a_mount_registry() -> None:
    """The exported ``mount_registry`` is a real ``MountRegistry`` instance."""
    assert isinstance(mount_registry, MountRegistry)
