"""OpenTAK Plugin SDK v2 — Data Package builder (``OTS.dp``).

An ATAK *Data Package* (DP) is a regular zip file with one mandatory entry —
``MANIFEST/MANIFEST.xml`` — and any number of content files (KMLs, KMZs,
images, CoT XML, etc.). When ATAK / iTAK imports the zip, it reads the
manifest's ``Configuration`` block for behavior flags and registers each
``<Content>`` entry as an overlay / mission item.

This module provides a tiny, scope-gated builder for those zips so plugins
don't have to roll their own XML each time. The API is fluent::

    from opentakserver.sdk.modules import dp

    pkg = dp.create('Festival Briefing')
    pkg.add_kml('crew.kml', '/app/ots/kml/crew.kml')
    pkg.add_marker(uid='cot-1', lat=37.0, lon=-115.0,
                   cot_type='a-f-G-U-C', callsign='Crew')
    pkg.add_network_link('Live KML',
                         url='https://server/api/.../live.kml',
                         refresh_seconds=300)
    zip_bytes = pkg.build()
    # or, write to disk + return a download URL the user can hand crew:
    download_url = pkg.share_url()

The reference implementation modeled on the live one is the MapMarker
plugin's ``data_package()`` view in
``plugins/ots-mapmarker-plugin/ots_mapmarker_plugin/app.py``.

Permission contract
-------------------
``dp.create`` is decorated with :func:`requires_write('dp')`. A plugin that
calls ``dp.create`` outside its declared ``write = ["dp"]`` scope gets a
:class:`~opentakserver.sdk.permissions.PermissionDeniedError` (code
``permission.write.dp``). Builder methods don't re-check — they're already
inside the request that opened the builder.
"""

from __future__ import annotations

import io
import logging
import os
import re
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from xml.sax.saxutils import escape as xml_escape

from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.permissions import requires_write

if TYPE_CHECKING:
    from typing_extensions import Self
else:
    Self = 'DataPackageBuilder'

logger = logging.getLogger(__name__)

# Where ``share_url`` writes the zip when called outside a Flask request
# context. Matches the bind-mount the OTS container uses
# (``/docker/opentak/ots → /app/ots``). Override in tests with the
# ``OTS_DATAPACKAGE_DIR`` env var or by setting Flask
# ``app.config['OTS_DATAPACKAGE_DIR']`` before the call.
_DEFAULT_DP_DIR = '/app/ots/datapackages'

_SLUG_RE = re.compile(r'[^A-Za-z0-9._-]+')


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class DataPackageError(OTSPluginError):
    """Raised when a Data Package can't be built or written.

    ``code`` is one of:
      * ``dp.source_missing``       — source file path doesn't exist
      * ``dp.empty``                — build() called with zero content entries
      * ``dp.write_failed``         — share_url() couldn't write the zip
      * ``dp.invalid_argument``     — bad ``name_in_zip`` / lat / lon / etc.
    """


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


class DataPackageBuilder:
    """Fluent builder for an ATAK Mission Package zip.

    Construct via :func:`create` so the ``write='dp'`` scope check fires.
    Direct instantiation bypasses scope enforcement and is reserved for
    internal callers (and tests). Builder methods mutate ``self`` and
    return ``self`` so calls can be chained.

    The MANIFEST.xml shape produced by :meth:`build` is::

        <MissionPackageManifest version="2">
          <Configuration>
            <Parameter name="uid" value="<uuid4>"/>
            <Parameter name="name" value="<package name>"/>
            <Parameter name="onReceiveImport" value="true"/>
            <Parameter name="onReceiveDelete" value="false"/>
          </Configuration>
          <Contents>
            <Content ignore="false" zipEntry="<entry name>"/>
            ...
          </Contents>
        </MissionPackageManifest>
    """

    def __init__(self, name: str) -> None:
        if not name or not name.strip():
            raise DataPackageError(
                code='dp.invalid_argument',
                message='Data Package name must be a non-empty string',
            )
        self.name: str = name.strip()
        self.uid: str = str(uuid.uuid4())
        # Ordered list of (entry_name, bytes, attach_to_manifest) tuples. We
        # keep the user's add-order so the resulting MANIFEST <Contents>
        # block lists entries in a predictable order — useful for tests and
        # for the ATAK preview UI.
        self._entries: list[tuple[str, bytes, bool]] = []

    # -- low-level ----------------------------------------------------------

    def add_file(
        self,
        name_in_zip: str,
        content: bytes,
        attach_to_manifest: bool = True,
    ) -> Self:
        """Add an arbitrary file. Set ``attach_to_manifest=False`` to ship a
        helper resource (e.g. an icon referenced from a KML) without making
        ATAK try to import it as an overlay."""

        entry = self._validate_entry_name(name_in_zip)
        if not isinstance(content, (bytes, bytearray)):
            raise DataPackageError(
                code='dp.invalid_argument',
                message=f'add_file content must be bytes, got {type(content).__name__}',
            )
        self._entries.append((entry, bytes(content), bool(attach_to_manifest)))
        return self

    # -- KML / KMZ ----------------------------------------------------------

    def add_kml(self, name_in_zip: str, source_path: str | Path) -> Self:
        """Read a KML from disk and add it as a manifest content entry."""

        return self._add_from_path(name_in_zip, source_path)

    def add_kmz(self, name_in_zip: str, source_path: str | Path) -> Self:
        """Read a KMZ from disk and add it as a manifest content entry."""

        return self._add_from_path(name_in_zip, source_path)

    def _add_from_path(self, name_in_zip: str, source_path: str | Path) -> Self:
        path = Path(source_path)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise DataPackageError(
                code='dp.source_missing',
                message=f'source file not found: {path}',
            ) from exc
        except OSError as exc:
            raise DataPackageError(
                code='dp.source_missing',
                message=f'could not read source file {path}: {exc}',
            ) from exc
        return self.add_file(name_in_zip, data, attach_to_manifest=True)

    # -- Network Link wrapper ----------------------------------------------

    def add_network_link(
        self,
        name: str,
        url: str,
        refresh_seconds: int = 300,
    ) -> Self:
        """Add a wrapper KML that points ATAK at a remote KML feed.

        The wrapper is a tiny KML containing one ``<NetworkLink>`` whose
        ``<Link><href>`` is ``url`` and which auto-refreshes every
        ``refresh_seconds`` seconds. ATAK treats the wrapper as a single
        overlay and pulls the actual content from the remote URL.
        """

        if not name or not name.strip():
            raise DataPackageError(
                code='dp.invalid_argument',
                message='network_link name must be non-empty',
            )
        if not url or not url.strip():
            raise DataPackageError(
                code='dp.invalid_argument',
                message='network_link url must be non-empty',
            )
        if refresh_seconds < 0:
            raise DataPackageError(
                code='dp.invalid_argument',
                message='refresh_seconds must be >= 0',
            )

        clean_name = name.strip()
        wrapper = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<kml xmlns="http://www.opengis.net/kml/2.2">\n'
            '  <Folder>\n'
            f'    <name>{xml_escape(clean_name)}</name>\n'
            '    <NetworkLink>\n'
            f'      <name>{xml_escape(clean_name)}</name>\n'
            '      <Link>\n'
            f'        <href>{xml_escape(url.strip())}</href>\n'
            '        <refreshMode>onInterval</refreshMode>\n'
            f'        <refreshInterval>{int(refresh_seconds)}</refreshInterval>\n'
            '      </Link>\n'
            '    </NetworkLink>\n'
            '  </Folder>\n'
            '</kml>\n'
        )
        entry_name = f'netlink_{_slugify(clean_name)}.kml'
        return self.add_file(entry_name, wrapper.encode('utf-8'), attach_to_manifest=True)

    # -- Marker (CoT-as-KML) ------------------------------------------------

    def add_marker(
        self,
        *,
        uid: str,
        lat: float,
        lon: float,
        cot_type: str = 'a-f-G-U-C',
        callsign: str = '',
        stale_minutes: int = 60,
    ) -> Self:
        """Add a single-marker CoT XML to the package.

        ATAK accepts CoT XML directly inside a Mission Package — it parses
        the ``<event>`` and renders the marker as if it had arrived over
        the network. We ship one ``<event>`` per ``add_marker`` call so
        UIDs round-trip cleanly. ``cot_type`` defaults to a friendly
        ground unit (``a-f-G-U-C``); pass any 2525C / MIL-STD code.
        """

        if not uid or not uid.strip():
            raise DataPackageError(
                code='dp.invalid_argument',
                message='marker uid must be non-empty',
            )
        if not -90.0 <= float(lat) <= 90.0:
            raise DataPackageError(
                code='dp.invalid_argument',
                message=f'marker lat out of range [-90, 90]: {lat!r}',
            )
        if not -180.0 <= float(lon) <= 180.0:
            raise DataPackageError(
                code='dp.invalid_argument',
                message=f'marker lon out of range [-180, 180]: {lon!r}',
            )
        if stale_minutes <= 0:
            raise DataPackageError(
                code='dp.invalid_argument',
                message='stale_minutes must be > 0',
            )

        now = datetime.now(timezone.utc)
        stale = now + timedelta(minutes=int(stale_minutes))
        # ATAK accepts ISO8601 with 'Z' suffix; isoformat() already emits
        # +00:00, so swap to 'Z' for the canonical CoT shape.
        time_iso = now.isoformat().replace('+00:00', 'Z')
        stale_iso = stale.isoformat().replace('+00:00', 'Z')
        callsign_clean = (callsign or '').strip()

        xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            f'<event version="2.0" uid="{xml_escape(uid.strip())}" '
            f'type="{xml_escape(cot_type)}" '
            f'time="{time_iso}" start="{time_iso}" stale="{stale_iso}" '
            'how="m-g">\n'
            f'  <point lat="{float(lat):.6f}" lon="{float(lon):.6f}" '
            'hae="0" ce="9999999.0" le="9999999.0"/>\n'
            '  <detail>\n'
            f'    <contact callsign="{xml_escape(callsign_clean)}"/>\n'
            f'    <__group name="Cyan" role="Team Member"/>\n'
            '  </detail>\n'
            '</event>\n'
        )
        entry_name = f'marker_{_slugify(uid)}.cot.xml'
        return self.add_file(entry_name, xml.encode('utf-8'), attach_to_manifest=True)

    # -- finalization -------------------------------------------------------

    def build(self) -> bytes:
        """Render the data package zip and return it as bytes.

        Raises :class:`DataPackageError` (``code='dp.empty'``) if no
        manifest-attached content has been added.
        """

        if not any(attach for _name, _data, attach in self._entries):
            raise DataPackageError(
                code='dp.empty',
                message='build() called with zero manifest-attached content entries',
            )

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            seen: set[str] = set()
            for entry_name, data, _attach in self._entries:
                if entry_name in seen:
                    raise DataPackageError(
                        code='dp.invalid_argument',
                        message=f'duplicate zip entry name: {entry_name!r}',
                    )
                seen.add(entry_name)
                zf.writestr(entry_name, data)
            zf.writestr('MANIFEST/MANIFEST.xml', self._render_manifest_xml())

        logger.info(
            'built data package',
            extra={'plugin': self.name, 'entries': len(self._entries)},
        )
        return buf.getvalue()

    def share_url(self) -> str:
        """Write the package to ``OTS_DATAPACKAGE_DIR`` and return a
        crew-shareable download URL.

        The URL points at the Marti sync-content endpoint that already
        exists in OTS (``/Marti/sync/content?hash=<filename>``). This is
        the same shape ATAK expects for any other server-hosted DP, so
        ``Send to ATAK`` style flows just work.
        """

        zip_bytes = self.build()
        out_dir = self._resolve_dp_dir()
        try:
            os.makedirs(out_dir, exist_ok=True)
        except OSError as exc:
            raise DataPackageError(
                code='dp.write_failed',
                message=f'could not create datapackage dir {out_dir}: {exc}',
            ) from exc

        slug = _slugify(self.name) or 'package'
        filename = f'{slug}_{int(time.time())}.zip'
        out_path = os.path.join(out_dir, filename)
        try:
            with open(out_path, 'wb') as fh:
                fh.write(zip_bytes)
        except OSError as exc:
            raise DataPackageError(
                code='dp.write_failed',
                message=f'could not write datapackage to {out_path}: {exc}',
            ) from exc

        return self._build_download_url(filename)

    # -- internals ----------------------------------------------------------

    def _render_manifest_xml(self) -> str:
        """Emit the MANIFEST/MANIFEST.xml body. ATAK requires
        ``MissionPackageManifest version="2"`` and the four standard
        Configuration parameters."""

        lines: list[str] = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<MissionPackageManifest version="2">',
            '  <Configuration>',
            f'    <Parameter name="uid" value="{xml_escape(self.uid)}"/>',
            f'    <Parameter name="name" value="{xml_escape(self.name)}"/>',
            '    <Parameter name="onReceiveImport" value="true"/>',
            '    <Parameter name="onReceiveDelete" value="false"/>',
            '  </Configuration>',
            '  <Contents>',
        ]
        for entry_name, _data, attach in self._entries:
            if not attach:
                continue
            lines.append(f'    <Content ignore="false" zipEntry="{xml_escape(entry_name)}"/>')
        lines.append('  </Contents>')
        lines.append('</MissionPackageManifest>')
        lines.append('')
        return '\n'.join(lines)

    @staticmethod
    def _validate_entry_name(name: str) -> str:
        if not name or not name.strip():
            raise DataPackageError(
                code='dp.invalid_argument',
                message='zip entry name must be non-empty',
            )
        clean = name.strip()
        if clean.startswith('/') or '..' in clean.split('/'):
            raise DataPackageError(
                code='dp.invalid_argument',
                message=f'zip entry name {clean!r} must be relative and not contain ".."',
            )
        return clean

    @staticmethod
    def _resolve_dp_dir() -> str:
        """Pick an output dir for ``share_url``, in priority order:

        1. ``OTS_DATAPACKAGE_DIR`` env var (tests / dev override).
        2. Flask ``app.config['OTS_DATAPACKAGE_DIR']`` if a Flask app
           context is active.
        3. The default ``/app/ots/datapackages`` (matches the OTS bind
           mount).
        """

        env_dir = os.environ.get('OTS_DATAPACKAGE_DIR')
        if env_dir:
            return env_dir
        try:
            from flask import current_app

            return current_app.config.get('OTS_DATAPACKAGE_DIR') or _DEFAULT_DP_DIR
        except RuntimeError:
            # No Flask app context. Fall through to the default.
            return _DEFAULT_DP_DIR
        except ImportError:
            return _DEFAULT_DP_DIR

    def _build_download_url(self, filename: str) -> str:
        """Return a URL the user can hand crew. Prefers Flask request
        host so the URL works on whatever interface dialed in; falls
        back to the configured ``OTS_FQDN`` and the dashboard's public
        port (8180)."""

        try:
            from flask import current_app, request
            from urllib.parse import urlparse

            host = (urlparse(request.url_root).hostname or '').split(',')[0].strip()
            if not host:
                host = current_app.config.get('OTS_FQDN', '') or 'localhost'
            port = current_app.config.get('OTS_HTTPS_PORT', 8180)
            scheme = 'https' if port in (443, 8180, 8443) else 'http'
            return f'{scheme}://{host}:{port}/Marti/sync/content?hash={filename}'
        except RuntimeError:
            # No request context — best-effort URL anchored at the
            # configured FQDN. Plugins running inside a request will
            # always hit the branch above.
            host = os.environ.get('OTS_FQDN', 'localhost')
            return f'https://{host}:8180/Marti/sync/content?hash={filename}'


def _slugify(value: str) -> str:
    """Best-effort filename-safe slug. Empty input → ``''`` (callers fall back)."""

    cleaned = _SLUG_RE.sub('_', value or '').strip('_')
    return cleaned or ''


# ---------------------------------------------------------------------------
# Module-level factory (the public entry point)
# ---------------------------------------------------------------------------


@requires_write('dp')
def create(name: str) -> DataPackageBuilder:
    """Open a fresh :class:`DataPackageBuilder`.

    Requires ``write = ["dp"]`` in the calling plugin's manifest.
    """

    return DataPackageBuilder(name)


__all__ = [
    'DataPackageBuilder',
    'DataPackageError',
    'create',
]
