"""OpenTAK Plugin SDK v2 — server-side package.

This package provides the v2 plugin manifest schema, permission decorators,
mount registry, and the ``OTS`` namespace surface that v2 plugins import.

Vanilla SDK v1 plugins under :mod:`opentakserver.plugins` are unaffected.
"""

from __future__ import annotations

from opentakserver.sdk.manifest import (
    ManifestValidationError,
    MountSpec,
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
    load_manifest,
)

# Populated by :mod:`opentakserver.sdk.ots_namespace` at import time. Until
# Phase A.4 wires the loader, importing ``OTS`` resolves to ``None`` and
# helpers raise :class:`OTSPluginError` at call time.
OTS = None

__all__ = [
    'OTS',
    'OTSPluginError',
    'ManifestValidationError',
    'MountSpec',
    'PluginManifest',
    'PluginPermissions',
    'load_manifest',
]
