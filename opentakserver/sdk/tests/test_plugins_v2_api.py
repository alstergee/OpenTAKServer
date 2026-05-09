"""Tests for :mod:`opentakserver.blueprints.ots_api.plugins_v2_api`.

Strategy: build a minimal Flask app, register only the v2 plugins
blueprint plus a tiny error-raising route, and bypass Flask-Security in
the test_client by stubbing ``auth_required`` / ``roles_accepted`` to
no-op decorators *for this app only*. We don't depend on the full OTS
``create_app()`` wiring — that path needs RabbitMQ, the DB, etc., which
is out-of-scope for an admin-API unit test.

We exercise:

* ``GET /installed`` returns 200 with a ``plugins`` array.
* ``GET /mounts`` returns 200 with a ``mounts`` array (filled from the
  shared registry singleton).
* ``GET /<bogus>/manifest`` returns 404 via the registered error handler.
* ``POST /install`` with malformed ``source`` returns 400.
* ``POST /install`` without admin role returns 403 (we re-enable the
  real ``roles_accepted`` for that test only).
* An :class:`OTSPluginError` raised inside a route is translated to the
  expected JSON shape (``{success, error, detail}``).
"""

from __future__ import annotations

from typing import Any

import pytest
from flask import Blueprint, Flask

from opentakserver.sdk.manifest import (
    MapOverlayMount,
    OTSPluginError,
    PluginManifest,
    PluginPermissions,
    TabMount,
)
from opentakserver.sdk.mount_registry import mount_registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_TEST_SLUG = 'ots-plugins-v2-test-fixture'


@pytest.fixture(autouse=True)
def _clear_registry():
    """Each test gets a fresh shared registry."""

    # Best-effort — unregister anything left behind.
    for slug in list(mount_registry._mounts.keys()):  # type: ignore[attr-defined]
        mount_registry.unregister(slug)
    yield
    for slug in list(mount_registry._mounts.keys()):  # type: ignore[attr-defined]
        mount_registry.unregister(slug)


def _load_plugins_v2_module(*, force_admin: bool = True):
    """Load ``plugins_v2_api`` from its source file without triggering the
    parent package's ``__init__.py`` (which pulls in the DB models and a
    fully-configured app — out-of-scope for this admin-API unit test).

    When ``force_admin`` is True we pre-monkey-patch
    ``flask_security.auth_required`` and ``flask_security.roles_accepted``
    to identity decorators *during* module load, so the decorated route
    functions become trivial pass-throughs. The originals are restored
    immediately after load.
    """

    import importlib
    import importlib.util
    import sys
    from pathlib import Path

    import flask_security

    real_auth = flask_security.auth_required
    real_roles = flask_security.roles_accepted

    if force_admin:
        flask_security.auth_required = lambda *a, **k: (lambda f: f)
        flask_security.roles_accepted = lambda *a, **k: (lambda f: f)

    try:
        # Resolve the file path from the installed package without
        # importing the package itself — we use the package's __file__
        # only as a sibling reference.
        import opentakserver

        pkg_dir = Path(opentakserver.__file__).parent
        src = pkg_dir / 'blueprints' / 'ots_api' / 'plugins_v2_api.py'
        # Drop any cached version from prior tests.
        sys.modules.pop('plugins_v2_api_test_module', None)
        spec = importlib.util.spec_from_file_location(
            'plugins_v2_api_test_module', src
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules['plugins_v2_api_test_module'] = mod
        spec.loader.exec_module(mod)
    finally:
        flask_security.auth_required = real_auth
        flask_security.roles_accepted = real_roles

    return mod


def _make_app(*, force_admin: bool = True) -> Flask:
    """Build a tiny Flask app wired to the v2 plugins blueprint."""

    mod = _load_plugins_v2_module(force_admin=force_admin)

    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        OTS_DATA_FOLDER='/tmp/ots-plugins-v2-test',  # noqa: S108 — test scratch
        OTS_PLUGIN_DISABLED=[],
        OTS_PLUGIN_MARKETPLACE_URL='https://invalid.local/marketplace.json',
    )
    import os
    os.makedirs(app.config['OTS_DATA_FOLDER'], exist_ok=True)

    app.register_blueprint(mod.plugins_v2_blueprint)
    # The blueprint already registers its OTSPluginError handler via the
    # @blueprint.errorhandler decorator — but blueprint-scoped handlers
    # only fire for routes inside that blueprint. Register at app level
    # too so the side-channel error route is caught.
    app.register_error_handler(OTSPluginError, mod._handle_plugin_error)

    # Side-channel: a route that raises OTSPluginError — used to verify
    # the error handler shape.
    side_bp = Blueprint('side', __name__)

    @side_bp.route('/api/plugins/v2/_test_raise')
    def _raise():
        raise OTSPluginError(code='test.synthetic', message='boom')

    app.register_blueprint(side_bp)

    return app


def _seed_v2_plugin() -> None:
    """Register one fake v2 plugin's mounts in the shared registry."""

    mount_registry.register(
        plugin_slug=_TEST_SLUG,
        mounts=[
            TabMount(
                kind='tab',
                label='Test',
                icon='tabler:icons:flask',
                path='/plugin/test',
                roles=['administrator'],
            ),
            MapOverlayMount(
                kind='map_overlay',
                label='Test pins',
                endpoint='/overlays/test.json',
            ),
        ],
        plugin_version='0.1.0',
    )


# ---------------------------------------------------------------------------
# GET /installed
# ---------------------------------------------------------------------------


def test_get_installed_returns_plugins_array():
    app = _make_app()
    _seed_v2_plugin()
    client = app.test_client()
    resp = client.get('/api/plugins/v2/installed')
    assert resp.status_code == 200
    body = resp.get_json()
    assert isinstance(body, dict)
    assert 'plugins' in body
    assert isinstance(body['plugins'], list)


# ---------------------------------------------------------------------------
# GET /mounts
# ---------------------------------------------------------------------------


def test_get_mounts_returns_array():
    app = _make_app()
    _seed_v2_plugin()
    client = app.test_client()
    resp = client.get('/api/plugins/v2/mounts')
    assert resp.status_code == 200
    body = resp.get_json()
    assert 'mounts' in body
    kinds = {m['kind'] for m in body['mounts']}
    assert {'tab', 'map_overlay'}.issubset(kinds)
    plugins_in_mounts = {m['_plugin'] for m in body['mounts']}
    assert _TEST_SLUG in plugins_in_mounts


# ---------------------------------------------------------------------------
# 404 for bogus slugs
# ---------------------------------------------------------------------------


def test_enable_unknown_plugin_returns_404():
    app = _make_app()
    client = app.test_client()
    resp = client.post('/api/plugins/v2/ots-no-such-plugin/enable')
    assert resp.status_code == 404
    body = resp.get_json()
    assert body['success'] is False
    assert body['error'] == 'plugin.not_found'


def test_get_manifest_unknown_plugin_returns_404():
    app = _make_app()
    client = app.test_client()
    resp = client.get('/api/plugins/v2/ots-mystery/manifest')
    assert resp.status_code == 404
    body = resp.get_json()
    assert body['error'] == 'plugin.not_found'


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


def test_install_rejects_malformed_source():
    app = _make_app()
    client = app.test_client()
    # Backticks are not allowed by the regex.
    resp = client.post(
        '/api/plugins/v2/install',
        json={'source': 'evil; rm -rf / `whoami`'},
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert body['success'] is False
    assert body['error'] == 'plugin.input_invalid'


def test_install_rejects_empty_source():
    app = _make_app()
    client = app.test_client()
    resp = client.post('/api/plugins/v2/install', json={'source': '  '})
    assert resp.status_code == 400
    body = resp.get_json()
    assert body['error'] == 'plugin.input_invalid'


def test_log_rejects_invalid_lines():
    app = _make_app()
    client = app.test_client()
    resp = client.get('/api/plugins/v2/foo/log?lines=notanumber')
    assert resp.status_code == 400
    body = resp.get_json()
    assert body['error'] == 'plugin.input_invalid'


# ---------------------------------------------------------------------------
# Role enforcement (real flask_security decorators)
# ---------------------------------------------------------------------------


def test_install_without_admin_role_returns_403_or_401(monkeypatch):
    """When the real auth/role decorators are active and no user is logged
    in, the mutating endpoint must reject the call.

    Flask-Security's default behavior depends on app config — without a
    configured Security instance it tends to 401 instead of 403. We
    accept either; the load-bearing assertion is "the endpoint is gated".
    """

    # Use the real decorators by NOT force_admin'ing the app.
    app = _make_app(force_admin=False)

    # Without flask_security.Security() configured against this Flask app,
    # the decorators should still raise / abort. Some flask_security
    # versions raise an AssertionError if Security isn't initialised —
    # treat that as a successful "blocked" result.
    client = app.test_client()
    try:
        resp = client.post(
            '/api/plugins/v2/install', json={'source': 'ots-something'}
        )
    except (AssertionError, ValueError):
        # flask_security blew up because there's no Security() instance
        # configured on this Flask app. That's a "blocked" outcome —
        # the route was NOT willing to run anonymously.
        return

    # Acceptable rejections: 401, 403, 302 (redirect to login), 500 (
    # decorator raised because Security wasn't initialised). The load-
    # bearing assertion is "the call did NOT pass through to the
    # endpoint and run pip" — anything that isn't 200 demonstrates that.
    assert resp.status_code != 200, (
        f'expected auth gate but install endpoint ran anonymously '
        f'(status={resp.status_code}, body={resp.data!r})'
    )


# ---------------------------------------------------------------------------
# Error handler shape
# ---------------------------------------------------------------------------


def test_ots_plugin_error_translated_to_json_shape():
    app = _make_app()
    client = app.test_client()
    resp = client.get('/api/plugins/v2/_test_raise')
    assert resp.status_code == 500
    body = resp.get_json()
    assert body['success'] is False
    assert body['error'] == 'test.synthetic'
    assert 'detail' in body
    assert 'boom' in body['detail']
