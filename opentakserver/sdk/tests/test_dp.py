"""Tests for :mod:`opentakserver.sdk.modules.dp`.

Run inside the OTS container:
    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_dp.py
"""

from __future__ import annotations

import contextlib
import io
import os
import re
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from flask import Flask

from opentakserver.sdk.manifest import (
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
)
from opentakserver.sdk.modules import dp
from opentakserver.sdk.permissions import (
    PermissionDeniedError,
    clear_plugin_context,
    set_plugin_context,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def plugin_context(manifest: PluginManifest) -> Iterator[None]:
    """Push ``manifest`` for the duration of the ``with`` block."""

    token = set_plugin_context(manifest)
    try:
        yield
    finally:
        clear_plugin_context(token)


def _manifest(*, write: list[str] | None = None) -> PluginManifest:
    """Build a minimal v2 manifest with the requested write scopes."""

    return PluginManifest(
        api_version=2,
        name='Test',
        slug='test-dp-plugin',
        version='0.0.0',
        author='test',
        license='MIT',
        description='dp test fixture',
        permissions=PluginPermissions(write=write or []),
        mount=[{'kind': 'tab', 'label': 'Test', 'path': '/test'}],
    )


@pytest.fixture
def write_dp_manifest() -> PluginManifest:
    return _manifest(write=['dp'])


@pytest.fixture
def kml_on_disk(tmp_path: Path) -> Path:
    p = tmp_path / 'crew.kml'
    p.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<kml xmlns="http://www.opengis.net/kml/2.2">'
        '<Document><name>Crew</name></Document></kml>\n'
    )
    return p


# ---------------------------------------------------------------------------
# Permission gating
# ---------------------------------------------------------------------------


def test_create_requires_write_dp_scope() -> None:
    """``dp.create`` must raise when called without ``write = ["dp"]``."""

    with plugin_context(_manifest(write=[])):
        with pytest.raises(PermissionDeniedError) as exc_info:
            dp.create('No-Scope Pkg')

    assert exc_info.value.code == 'permission.write.dp'
    assert isinstance(exc_info.value, OTSPluginError)


def test_create_allows_when_scope_declared(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        builder = dp.create('Allowed')
    assert isinstance(builder, dp.DataPackageBuilder)
    assert builder.name == 'Allowed'


def test_create_allows_when_no_plugin_context() -> None:
    """Calls from core OTS code (no manifest context) pass through."""

    builder = dp.create('Core caller')
    assert isinstance(builder, dp.DataPackageBuilder)


def test_dp_scope_is_valid_in_manifest() -> None:
    """The manifest's ``_VALID_SCOPES`` must accept ``write = ["dp"]``."""

    m = _manifest(write=['dp'])
    assert 'dp' in m.permissions.write


# ---------------------------------------------------------------------------
# Builder — basics + KML attachment
# ---------------------------------------------------------------------------


def test_build_returns_valid_zip_with_manifest_and_kml(
    write_dp_manifest: PluginManifest,
    kml_on_disk: Path,
) -> None:
    with plugin_context(write_dp_manifest):
        zip_bytes = dp.create('Briefing').add_kml('crew.kml', kml_on_disk).build()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = set(zf.namelist())
        assert 'MANIFEST/MANIFEST.xml' in names
        assert 'crew.kml' in names

        manifest_xml = zf.read('MANIFEST/MANIFEST.xml').decode('utf-8')
        kml_body = zf.read('crew.kml').decode('utf-8')

    # MANIFEST shape: ATAK-required attributes
    assert 'MissionPackageManifest version="2"' in manifest_xml
    assert '<Configuration>' in manifest_xml
    assert '<Contents>' in manifest_xml
    assert 'name="onReceiveImport" value="true"' in manifest_xml
    assert 'name="uid"' in manifest_xml
    assert 'value="Briefing"' in manifest_xml
    assert 'zipEntry="crew.kml"' in manifest_xml
    assert '<Document><name>Crew</name>' in kml_body


def test_build_raises_when_empty(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        builder = dp.create('Empty')
        with pytest.raises(dp.DataPackageError) as exc_info:
            builder.build()
    assert exc_info.value.code == 'dp.empty'


def test_add_kml_missing_source_raises(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        with pytest.raises(dp.DataPackageError) as exc_info:
            dp.create('X').add_kml('missing.kml', '/nonexistent/path/foo.kml')
    assert exc_info.value.code == 'dp.source_missing'


def test_uid_is_unique_per_package(write_dp_manifest: PluginManifest, kml_on_disk: Path) -> None:
    with plugin_context(write_dp_manifest):
        a = dp.create('A').add_kml('a.kml', kml_on_disk)
        b = dp.create('B').add_kml('b.kml', kml_on_disk)
    assert a.uid != b.uid
    assert re.fullmatch(r'[0-9a-f-]{36}', a.uid)


# ---------------------------------------------------------------------------
# Builder — network link
# ---------------------------------------------------------------------------


def test_add_network_link_emits_wrapper_kml(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        zip_bytes = (
            dp.create('Live')
            .add_network_link('Live KML',
                              url='https://server/api/foo/live.kml',
                              refresh_seconds=300)
            .build()
        )

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        wrappers = [n for n in names if n.startswith('netlink_') and n.endswith('.kml')]
        assert wrappers, f'expected a netlink_*.kml in {names!r}'
        wrapper_body = zf.read(wrappers[0]).decode('utf-8')
        manifest_xml = zf.read('MANIFEST/MANIFEST.xml').decode('utf-8')

    # Wrapper shape
    assert '<NetworkLink>' in wrapper_body
    assert '<Link>' in wrapper_body
    assert '<href>https://server/api/foo/live.kml</href>' in wrapper_body
    assert '<refreshMode>onInterval</refreshMode>' in wrapper_body
    assert '<refreshInterval>300</refreshInterval>' in wrapper_body
    assert '<name>Live KML</name>' in wrapper_body
    # Manifest references the wrapper
    assert f'zipEntry="{wrappers[0]}"' in manifest_xml


def test_add_network_link_validates_args(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        builder = dp.create('Live')
        with pytest.raises(dp.DataPackageError):
            builder.add_network_link('', url='https://x/y.kml')
        with pytest.raises(dp.DataPackageError):
            builder.add_network_link('name', url='')
        with pytest.raises(dp.DataPackageError):
            builder.add_network_link('name', url='https://x/y.kml', refresh_seconds=-1)


# ---------------------------------------------------------------------------
# Builder — markers
# ---------------------------------------------------------------------------


def test_add_marker_produces_cot_xml(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        zip_bytes = (
            dp.create('Crew')
            .add_marker(uid='cot-uid-1', lat=37.5, lon=-115.25,
                        cot_type='a-f-G-U-C', callsign='Alpha-1',
                        stale_minutes=60)
            .build()
        )

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        marker_entries = [n for n in zf.namelist() if n.startswith('marker_')]
        assert marker_entries, f'expected a marker_*.cot.xml in {zf.namelist()!r}'
        body = zf.read(marker_entries[0]).decode('utf-8')

    # Canonical CoT shape
    assert body.startswith('<?xml')
    assert '<event' in body
    assert 'uid="cot-uid-1"' in body
    assert 'type="a-f-G-U-C"' in body
    assert '<point' in body
    assert 'lat="37.500000"' in body
    assert 'lon="-115.250000"' in body
    assert '<contact callsign="Alpha-1"/>' in body


def test_add_marker_validates_lat_lon(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        builder = dp.create('Bad coords')
        with pytest.raises(dp.DataPackageError):
            builder.add_marker(uid='x', lat=999.0, lon=0.0)
        with pytest.raises(dp.DataPackageError):
            builder.add_marker(uid='x', lat=0.0, lon=999.0)
        with pytest.raises(dp.DataPackageError):
            builder.add_marker(uid='', lat=0.0, lon=0.0)


# ---------------------------------------------------------------------------
# Builder — generic add_file + duplicate detection
# ---------------------------------------------------------------------------


def test_add_file_attach_false_omits_from_manifest(
    write_dp_manifest: PluginManifest,
    kml_on_disk: Path,
) -> None:
    with plugin_context(write_dp_manifest):
        zip_bytes = (
            dp.create('Mixed')
            .add_kml('main.kml', kml_on_disk)
            .add_file('icons/blip.png', b'\x89PNG\r\n\x1a\n', attach_to_manifest=False)
            .build()
        )

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = set(zf.namelist())
        assert 'icons/blip.png' in names
        manifest_xml = zf.read('MANIFEST/MANIFEST.xml').decode('utf-8')
    assert 'zipEntry="main.kml"' in manifest_xml
    assert 'icons/blip.png' not in manifest_xml


def test_duplicate_entry_names_raise(
    write_dp_manifest: PluginManifest,
    kml_on_disk: Path,
) -> None:
    with plugin_context(write_dp_manifest):
        builder = dp.create('Dup')
        builder.add_kml('same.kml', kml_on_disk)
        builder.add_kml('same.kml', kml_on_disk)
        with pytest.raises(dp.DataPackageError) as exc_info:
            builder.build()
    assert exc_info.value.code == 'dp.invalid_argument'


def test_invalid_entry_names_rejected(write_dp_manifest: PluginManifest) -> None:
    with plugin_context(write_dp_manifest):
        builder = dp.create('Bad')
        with pytest.raises(dp.DataPackageError):
            builder.add_file('/abs/path.kml', b'<kml/>')
        with pytest.raises(dp.DataPackageError):
            builder.add_file('../escape.kml', b'<kml/>')
        with pytest.raises(dp.DataPackageError):
            builder.add_file('   ', b'<kml/>')


# ---------------------------------------------------------------------------
# share_url — writes file + builds URL
# ---------------------------------------------------------------------------


def test_share_url_writes_zip_and_returns_url(
    write_dp_manifest: PluginManifest,
    kml_on_disk: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out_dir = tmp_path / 'datapackages'
    monkeypatch.setenv('OTS_DATAPACKAGE_DIR', str(out_dir))
    monkeypatch.setenv('OTS_FQDN', 'tak.example.test')

    with plugin_context(write_dp_manifest):
        url = dp.create('Festival Briefing').add_kml('crew.kml', kml_on_disk).share_url()

    files = list(out_dir.iterdir())
    assert len(files) == 1, files
    written = files[0]
    assert written.suffix == '.zip'
    assert written.name.startswith('Festival_Briefing_')

    # URL is the Marti sync-content shape ATAK clients understand.
    assert url.startswith('https://tak.example.test:8180/Marti/sync/content?hash=')
    assert url.endswith(written.name)

    # Sanity: the zip on disk is the same DP we'd get from build().
    with zipfile.ZipFile(written) as zf:
        assert 'MANIFEST/MANIFEST.xml' in zf.namelist()
        assert 'crew.kml' in zf.namelist()


def test_share_url_uses_flask_request_host_when_available(
    write_dp_manifest: PluginManifest,
    kml_on_disk: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out_dir = tmp_path / 'dp2'
    monkeypatch.setenv('OTS_DATAPACKAGE_DIR', str(out_dir))

    app = Flask(__name__)
    app.config['OTS_FQDN'] = 'fallback.example.test'
    app.config['OTS_HTTPS_PORT'] = 8180

    with app.test_request_context('/api/plugins/x/data_package',
                                  base_url='https://my-host.example.test'):
        with plugin_context(write_dp_manifest):
            url = dp.create('Live').add_kml('a.kml', kml_on_disk).share_url()

    assert url.startswith('https://my-host.example.test:8180/Marti/sync/content?hash=')


# ---------------------------------------------------------------------------
# Order preserved + content count
# ---------------------------------------------------------------------------


def test_manifest_lists_entries_in_add_order(
    write_dp_manifest: PluginManifest,
    kml_on_disk: Path,
) -> None:
    with plugin_context(write_dp_manifest):
        zip_bytes = (
            dp.create('Order')
            .add_kml('first.kml', kml_on_disk)
            .add_kml('second.kml', kml_on_disk)
            .add_kml('third.kml', kml_on_disk)
            .build()
        )

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        manifest_xml = zf.read('MANIFEST/MANIFEST.xml').decode('utf-8')

    idx_first = manifest_xml.index('first.kml')
    idx_second = manifest_xml.index('second.kml')
    idx_third = manifest_xml.index('third.kml')
    assert idx_first < idx_second < idx_third
