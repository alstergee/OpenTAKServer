"""Tests for :mod:`opentakserver.sdk.manifest`.

Covers:
* happy-path manifest parses end-to-end via ``load_manifest``
* missing required field raises ``code='manifest.invalid'``
* bad slug regex raises
* unknown mount kind raises with the kind name in the message
* unknown permission scope raises (``PluginPermissions(read=['bogus'])``)
* discriminated union dispatch:
    - ``tab`` without ``path`` raises
    - ``webhook`` without ``path`` raises
    - ``tab`` with extra ``endpoint`` is silently ignored
      (mount models opt in to ``extra='ignore'`` for forward-compat)
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from pydantic import ValidationError

from opentakserver.sdk.manifest import (
    ManifestValidationError,
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
    TabMount,
    WebhookMount,
    load_manifest,
)


HAPPY_TOML = textwrap.dedent(
    '''
    [plugin]
    api_version = 2
    name = "MapMarker"
    slug = "ots-mapmarker-plugin"
    version = "1.1"
    author = "Alstergee"
    license = "MIT"
    description = "Google Drive KML sync for OpenTAKServer"
    docs_url = "https://example.com/readme"
    icon = "ui/icon.svg"

    [plugin.permissions]
    read = ["eud", "geochat", "kml"]
    write = ["kml"]
    mesh = false
    mission = "read"
    admin_routes = false

    [[plugin.mount]]
    kind = "tab"
    label = "Map Marker"
    icon = "tabler:icons:map-pin"
    path = "/plugin/mapmarker"
    roles = ["administrator"]

    [[plugin.mount]]
    kind = "map_overlay"
    label = "Map Marker pins"
    endpoint = "/overlay.kml"

    [[plugin.mount]]
    kind = "data_package_generator"
    label = "Generate ATAK Data Package"
    endpoint = "/data_package"

    [[plugin.mount]]
    kind = "background_worker"
    label = "Sync KMLs"
    cron = "*/5 * * * *"
    handler = "ots_mapmarker_plugin.workers:sync"

    [[plugin.mount]]
    kind = "webhook"
    label = "Drive push"
    path = "/drive/push"
    hmac_secret_env = "MAPMARKER_DRIVE_HMAC"

    [plugin.menu]
    eud_action_label = "Send to ATAK"
    eud_action_endpoint = "/eud_action"
    '''
)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_manifest_parses(tmp_path: Path) -> None:
    p = tmp_path / 'plugin.toml'
    p.write_text(HAPPY_TOML)

    manifest = load_manifest(p)

    assert isinstance(manifest, PluginManifest)
    assert manifest.api_version == 2
    assert manifest.slug == 'ots-mapmarker-plugin'
    assert manifest.version == '1.1'
    assert manifest.permissions.mission == 'read'
    assert manifest.permissions.read == ['eud', 'geochat', 'kml']
    assert manifest.permissions.write == ['kml']
    assert len(manifest.mount) == 5

    kinds = [m.kind for m in manifest.mount]
    assert kinds == [
        'tab',
        'map_overlay',
        'data_package_generator',
        'background_worker',
        'webhook',
    ]
    assert manifest.menu is not None
    assert manifest.menu.eud_action_endpoint == '/eud_action'


# ---------------------------------------------------------------------------
# Error paths — top-level manifest
# ---------------------------------------------------------------------------


def test_missing_required_field_raises_manifest_invalid(tmp_path: Path) -> None:
    # `version` removed
    bad = HAPPY_TOML.replace('version = "1.1"\n', '')
    p = tmp_path / 'plugin.toml'
    p.write_text(bad)

    with pytest.raises(ManifestValidationError) as excinfo:
        load_manifest(p)

    assert excinfo.value.code == 'manifest.invalid'
    # OTSPluginError parent contract:
    assert isinstance(excinfo.value, OTSPluginError)
    assert str(excinfo.value).startswith('[manifest.invalid]')
    # Pydantic detail preserved for UI rendering:
    assert excinfo.value.details is not None
    paths = [tuple(err['loc']) for err in excinfo.value.details]
    assert ('version',) in paths


def test_bad_slug_regex_raises(tmp_path: Path) -> None:
    bad = HAPPY_TOML.replace(
        'slug = "ots-mapmarker-plugin"',
        'slug = "OTS_MapMarker"',  # uppercase + underscore — both forbidden
    )
    p = tmp_path / 'plugin.toml'
    p.write_text(bad)

    with pytest.raises(ManifestValidationError) as excinfo:
        load_manifest(p)

    assert excinfo.value.code == 'manifest.invalid'
    paths = [tuple(err['loc']) for err in excinfo.value.details]
    assert ('slug',) in paths


def test_unknown_mount_kind_raises_with_kind_in_message(tmp_path: Path) -> None:
    bad = HAPPY_TOML.replace('kind = "tab"', 'kind = "totally_made_up_kind"')
    p = tmp_path / 'plugin.toml'
    p.write_text(bad)

    with pytest.raises(ManifestValidationError) as excinfo:
        load_manifest(p)

    assert excinfo.value.code == 'manifest.invalid'
    assert 'totally_made_up_kind' in str(excinfo.value)


# ---------------------------------------------------------------------------
# Error paths — direct model construction
# ---------------------------------------------------------------------------


def test_plugin_permissions_rejects_unknown_scope() -> None:
    with pytest.raises(ValidationError) as excinfo:
        PluginPermissions(read=['bogus'])

    assert 'bogus' in str(excinfo.value)


def test_tab_mount_requires_path() -> None:
    with pytest.raises(ValidationError) as excinfo:
        TabMount(kind='tab', label='X')  # type: ignore[call-arg]

    paths = [tuple(err['loc']) for err in excinfo.value.errors()]
    assert ('path',) in paths


def test_webhook_mount_requires_path() -> None:
    with pytest.raises(ValidationError) as excinfo:
        WebhookMount(kind='webhook', label='X')  # type: ignore[call-arg]

    paths = [tuple(err['loc']) for err in excinfo.value.errors()]
    assert ('path',) in paths


def test_tab_mount_silently_drops_extra_endpoint_field() -> None:
    """Mount models use ``extra='ignore'`` (forward-compat — see manifest.py).

    A future v2.1 manifest could add ``endpoint`` to ``tab``; today's server
    must accept it (and ignore it) so plugin authors don't have to publish
    server-version-pinned manifests.
    """

    tab = TabMount(
        kind='tab',
        label='X',
        path='/x',
        endpoint='/should-be-dropped',  # type: ignore[call-arg]
    )
    # `endpoint` was silently dropped by Pydantic — not stored on the model.
    assert not hasattr(tab, 'endpoint')
    assert tab.path == '/x'


def test_relative_path_rejected_on_tab() -> None:
    with pytest.raises(ValidationError):
        TabMount(kind='tab', label='X', path='no-leading-slash')


def test_relative_endpoint_rejected_on_webhook() -> None:
    with pytest.raises(ValidationError):
        WebhookMount(kind='webhook', label='X', path='no-leading-slash')
