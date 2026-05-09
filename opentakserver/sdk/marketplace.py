"""OpenTAK Plugin SDK v2 — marketplace JSON contract.

Defines the wire format for ``marketplace.json`` — the public catalogue file
hosted at a fixed URL (``OTS_PLUGIN_MARKETPLACE_URL``) and consumed by
:func:`opentakserver.blueprints.ots_api.plugins_v2_api.get_marketplace`.

The schema is intentionally narrower than :class:`PluginManifest`: the
marketplace describes plugins for *discovery* (browse, install), while
``plugin.toml`` describes them for *runtime registration* (mounts,
permissions). Marketplace entries carry a ``mounts_summary`` and
``permissions_summary`` that the dashboard renders during install for
informed-consent — the canonical mount + permission set is loaded later from
the installed plugin's own ``plugin.toml``.

Design notes
------------
* Errors raised here are :class:`MarketplaceValidationError` (an
  :class:`OTSPluginError` subclass with ``code='marketplace.invalid'``) so the
  v2 API blueprint can surface them via the existing JSON envelope handler.
* :func:`schema_json` returns a JSON Schema dump of :class:`Marketplace` for
  use by external contributors (``jsonschema -i my_entry.json
  marketplace.schema.json``).
* ``install_source`` is a free-form pip-installable spec — a package name,
  wheel URL, or ``git+https://...`` URL. We do not validate beyond
  non-empty/string-shape because the v2 API's install endpoint already
  re-validates with a strict allowlist regex before invoking pip.
* ``sha256`` and ``signature`` are optional and reserved for future integrity
  / signing work. Validation only checks shape (64 hex chars).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .manifest import OTSPluginError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class MarketplaceValidationError(OTSPluginError):
    """Raised when ``marketplace.json`` (or a single entry) fails validation.

    ``code`` is always ``'marketplace.invalid'``. ``details`` carries the
    pydantic ``ValidationError.errors()`` output so the UI can surface
    field-level errors to the user.
    """

    def __init__(self, message: str, *, details: Any | None = None) -> None:
        super().__init__('marketplace.invalid', message, details=details)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')


class MarketplaceEntry(BaseModel):
    """A single plugin advertised in the marketplace.

    Mirrors the dashboard's "Available" plugin row in B.4: name, version,
    description, mount + scope summaries, and an ``install_source`` the v2
    API can hand to pip.
    """

    model_config = ConfigDict(extra='forbid')

    slug: str = Field(pattern=r'^[a-z][a-z0-9-]*$')
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    description: str = Field(min_length=1)
    author: str = Field(min_length=1)
    license: str = Field(min_length=1)
    homepage: str | None = None
    docs_url: str | None = None
    icon: str | None = None
    install_source: str = Field(min_length=1)
    sdk_version: Literal[1, 2] = 2
    mounts_summary: list[str] = Field(default_factory=list)
    permissions_summary: dict[str, Any] | None = None
    sha256: str | None = None
    signature: str | None = None
    published_at: datetime
    minimum_ots_version: str | None = None

    @field_validator('sha256')
    @classmethod
    def _validate_sha256(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not _SHA256_RE.match(value):
            raise ValueError(
                'sha256 must be 64 lowercase hex characters'
            )
        return value


class Marketplace(BaseModel):
    """Top-level marketplace catalogue document."""

    model_config = ConfigDict(extra='forbid')

    schema_version: int = 1
    last_updated: datetime
    plugins: list[MarketplaceEntry] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def schema_json() -> dict[str, Any]:
    """Return the JSON Schema for the :class:`Marketplace` document.

    Equivalent to ``Marketplace.model_json_schema()`` but exposed as a
    versionable helper so callers (the v2 API, the docs route, the
    ``marketplace.schema.json`` file at the repo root) all reference the same
    entry point.
    """

    return Marketplace.model_json_schema()


def load_marketplace(data: dict[str, Any]) -> Marketplace:
    """Validate a parsed JSON dict against :class:`Marketplace`.

    Raises :class:`MarketplaceValidationError` on schema failure with the
    pydantic error list preserved on ``.details`` for UI consumption.
    """

    try:
        return Marketplace.model_validate(data)
    except ValidationError as exc:
        logger.warning('marketplace.json validation failed: %s', exc.errors())
        raise MarketplaceValidationError(
            'marketplace.json is invalid', details=exc.errors()
        ) from exc


__all__ = [
    'Marketplace',
    'MarketplaceEntry',
    'MarketplaceValidationError',
    'load_marketplace',
    'schema_json',
]
