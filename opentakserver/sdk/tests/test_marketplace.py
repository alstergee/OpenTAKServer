"""Tests for :mod:`opentakserver.sdk.marketplace`.

Covers:
* a valid ``marketplace.json``-shaped payload round-trips through
  :class:`Marketplace`
* a bad slug (regex mismatch) raises ``MarketplaceValidationError``
* missing required fields (e.g. ``install_source``) raise
* ``schema_json()`` returns a JSON-Schema-shaped dict
* ``sha256`` shape is enforced
* unknown top-level keys are rejected (``extra='forbid'``)
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from opentakserver.sdk.marketplace import (
    Marketplace,
    MarketplaceEntry,
    MarketplaceValidationError,
    load_marketplace,
    schema_json,
)


HAPPY_PAYLOAD: dict = {
    'schema_version': 1,
    'last_updated': '2026-05-09T00:00:00Z',
    'plugins': [
        {
            'slug': 'ots-mapmarker-plugin',
            'name': 'MapMarker',
            'version': '1.2',
            'description': 'Google Drive KML sync.',
            'author': 'Alstergee',
            'license': 'MIT',
            'homepage': 'https://github.com/alstergee/OpenTAKServer',
            'docs_url': 'https://example.test/docs',
            'icon': 'map-pin',
            'install_source': 'git+https://example.test/foo.git',
            'sdk_version': 2,
            'mounts_summary': ['tab', 'map_overlay'],
            'permissions_summary': {
                'read': ['kml'],
                'write': ['kml'],
                'mesh': False,
                'mission': 'none',
                'admin_routes': True,
            },
            'published_at': '2026-05-09T00:00:00Z',
        }
    ],
}


# ---------------------------------------------------------------------------
# Happy-path round-trip
# ---------------------------------------------------------------------------


def test_round_trip_valid_marketplace() -> None:
    """A valid payload validates and re-dumps to an equivalent JSON dict."""

    market = load_marketplace(HAPPY_PAYLOAD)
    assert isinstance(market, Marketplace)
    assert market.schema_version == 1
    assert len(market.plugins) == 1

    entry = market.plugins[0]
    assert isinstance(entry, MarketplaceEntry)
    assert entry.slug == 'ots-mapmarker-plugin'
    assert entry.sdk_version == 2
    assert entry.mounts_summary == ['tab', 'map_overlay']
    assert entry.permissions_summary == {
        'read': ['kml'],
        'write': ['kml'],
        'mesh': False,
        'mission': 'none',
        'admin_routes': True,
    }

    # Round-trip via JSON: model -> json -> dict -> model -> identical
    dumped = market.model_dump_json()
    restored = load_marketplace(json.loads(dumped))
    assert restored.model_dump() == market.model_dump()


def test_minimal_entry_with_optional_fields_omitted() -> None:
    """All ``Optional`` fields can be omitted."""

    entry = MarketplaceEntry(
        slug='ots-min',
        name='Min',
        version='0.1',
        description='Minimal plugin.',
        author='Test',
        license='MIT',
        install_source='ots-min',
        published_at='2026-05-09T00:00:00Z',
    )
    assert entry.homepage is None
    assert entry.docs_url is None
    assert entry.icon is None
    assert entry.mounts_summary == []
    assert entry.permissions_summary is None
    assert entry.sha256 is None
    assert entry.signature is None
    assert entry.minimum_ots_version is None
    assert entry.sdk_version == 2  # default


# ---------------------------------------------------------------------------
# Bad-slug error path
# ---------------------------------------------------------------------------


def test_bad_slug_uppercase_raises() -> None:
    """Uppercase slugs must be rejected by the regex."""

    with pytest.raises(ValidationError) as excinfo:
        MarketplaceEntry(
            slug='OTS-Bad',
            name='Bad',
            version='1.0',
            description='Bad slug.',
            author='Test',
            license='MIT',
            install_source='ots-bad',
            published_at='2026-05-09T00:00:00Z',
        )
    errors = excinfo.value.errors()
    assert any(err['loc'] == ('slug',) for err in errors)


def test_bad_slug_starts_with_digit_raises() -> None:
    """Slugs must start with a lowercase letter."""

    with pytest.raises(ValidationError):
        MarketplaceEntry(
            slug='1bad',
            name='Bad',
            version='1.0',
            description='Bad slug.',
            author='Test',
            license='MIT',
            install_source='1bad',
            published_at='2026-05-09T00:00:00Z',
        )


def test_bad_slug_in_marketplace_raises_marketplace_error() -> None:
    """Bad slug inside a full Marketplace document raises a tagged error."""

    bad = json.loads(json.dumps(HAPPY_PAYLOAD))
    bad['plugins'][0]['slug'] = 'BAD_SLUG'
    with pytest.raises(MarketplaceValidationError) as excinfo:
        load_marketplace(bad)
    assert excinfo.value.code == 'marketplace.invalid'
    # details preserves the pydantic error list for UI consumption
    assert isinstance(excinfo.value.details, list)
    assert any(err['loc'][-1] == 'slug' for err in excinfo.value.details)


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------


def test_missing_required_install_source_raises() -> None:
    """Required ``install_source`` cannot be omitted."""

    with pytest.raises(ValidationError) as excinfo:
        MarketplaceEntry(
            slug='ots-x',
            name='X',
            version='1.0',
            description='X plugin.',
            author='Test',
            license='MIT',
            published_at='2026-05-09T00:00:00Z',
        )
    errors = excinfo.value.errors()
    assert any(err['loc'] == ('install_source',) for err in errors)


def test_missing_required_marketplace_last_updated_raises() -> None:
    """Top-level ``last_updated`` is required."""

    with pytest.raises(ValidationError):
        Marketplace(plugins=[])


def test_missing_required_published_at_raises() -> None:
    """``published_at`` is required."""

    with pytest.raises(ValidationError) as excinfo:
        MarketplaceEntry(
            slug='ots-x',
            name='X',
            version='1.0',
            description='X plugin.',
            author='Test',
            license='MIT',
            install_source='ots-x',
        )
    errors = excinfo.value.errors()
    assert any(err['loc'] == ('published_at',) for err in errors)


# ---------------------------------------------------------------------------
# Strictness
# ---------------------------------------------------------------------------


def test_unknown_top_level_field_raises() -> None:
    """``Marketplace`` rejects unknown top-level keys (extra='forbid')."""

    bad = dict(HAPPY_PAYLOAD)
    bad['rogue_field'] = 'oops'
    with pytest.raises(MarketplaceValidationError):
        load_marketplace(bad)


def test_unknown_entry_field_raises() -> None:
    """``MarketplaceEntry`` rejects unknown keys (extra='forbid')."""

    bad = json.loads(json.dumps(HAPPY_PAYLOAD))
    bad['plugins'][0]['rogue'] = 'oops'
    with pytest.raises(MarketplaceValidationError):
        load_marketplace(bad)


def test_invalid_sdk_version_raises() -> None:
    """``sdk_version`` is constrained to ``1`` or ``2``."""

    with pytest.raises(ValidationError):
        MarketplaceEntry(
            slug='ots-x',
            name='X',
            version='1.0',
            description='X plugin.',
            author='Test',
            license='MIT',
            install_source='ots-x',
            sdk_version=3,  # type: ignore[arg-type]
            published_at='2026-05-09T00:00:00Z',
        )


def test_bad_sha256_shape_raises() -> None:
    """``sha256`` must be exactly 64 lowercase hex chars when supplied."""

    with pytest.raises(ValidationError):
        MarketplaceEntry(
            slug='ots-x',
            name='X',
            version='1.0',
            description='X plugin.',
            author='Test',
            license='MIT',
            install_source='ots-x',
            sha256='not-a-real-hash',
            published_at='2026-05-09T00:00:00Z',
        )


def test_valid_sha256_accepted() -> None:
    """A correctly-shaped sha256 passes."""

    entry = MarketplaceEntry(
        slug='ots-x',
        name='X',
        version='1.0',
        description='X plugin.',
        author='Test',
        license='MIT',
        install_source='ots-x',
        sha256='a' * 64,
        published_at='2026-05-09T00:00:00Z',
    )
    assert entry.sha256 == 'a' * 64


# ---------------------------------------------------------------------------
# Schema dump
# ---------------------------------------------------------------------------


def test_schema_json_returns_json_schema_dict() -> None:
    """``schema_json()`` returns a JSON-Schema-shaped dict."""

    schema = schema_json()
    assert isinstance(schema, dict)
    # JSON Schema documents always carry a `properties` mapping at the root.
    assert 'properties' in schema
    assert 'plugins' in schema['properties']
    # Discovered nested model should appear in `$defs` / `definitions`.
    nested = schema.get('$defs') or schema.get('definitions') or {}
    assert 'MarketplaceEntry' in nested


def test_schema_json_serialises_to_json() -> None:
    """The schema dict is serialisable as a JSON string (no ``set``s etc.)."""

    schema = schema_json()
    blob = json.dumps(schema, sort_keys=True)
    assert blob.startswith('{')
