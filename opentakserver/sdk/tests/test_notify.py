"""Tests for :mod:`opentakserver.sdk.modules.notify`.

Run inside the OTS container::

    docker exec opentakserver /app/venv/bin/pytest -x \\
        /app/venv/lib/python3.13/site-packages/opentakserver/sdk/tests/test_notify.py
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator
from types import SimpleNamespace
from unittest import mock

import pytest
from flask import Flask

from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.modules import notify


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app() -> Flask:
    """Bare Flask app — ``current_app`` is available inside ``with app.app_context():``."""
    app = Flask(__name__)
    app.config['OTS_ENABLE_EMAIL'] = False
    return app


@pytest.fixture
def socketio_emit() -> Iterator[mock.MagicMock]:
    """Patch the bound ``socketio`` symbol on the notify module."""
    with mock.patch.object(notify, 'socketio') as patched:
        yield patched.emit


@pytest.fixture
def cot_send() -> Iterator[mock.MagicMock]:
    """Patch ``cot.send`` so we don't hit AMQP."""
    with mock.patch.object(notify.cot, 'send') as patched:
        yield patched


@pytest.fixture
def cot_broadcast() -> Iterator[mock.MagicMock]:
    """Patch ``cot.broadcast`` so we don't hit AMQP."""
    with mock.patch.object(notify.cot, 'broadcast') as patched:
        patched.return_value = 'broadcast-uid-123'
        yield patched


@pytest.fixture
def resolve_target() -> Iterator[mock.MagicMock]:
    """Patch the EUD-resolution helper so we don't need a DB."""
    with mock.patch.object(notify, '_resolve_target_uid') as patched:
        yield patched


# ---------------------------------------------------------------------------
# notify.toast
# ---------------------------------------------------------------------------


def test_toast_emits_socketio_notification_event(
    socketio_emit: mock.MagicMock,
) -> None:
    notify.toast('Hello', 'World', level='success', timeout_ms=2500)

    socketio_emit.assert_called_once()
    call = socketio_emit.call_args
    assert call.args[0] == 'notification'
    payload = call.args[1]
    assert payload['title'] == 'Hello'
    assert payload['message'] == 'World'
    assert payload['level'] == 'success'
    assert payload['timeout_ms'] == 2500
    assert payload['to_users'] is None
    # ISO-8601 string with timezone — should round-trip.
    parsed = _dt.datetime.fromisoformat(payload['timestamp'])
    assert parsed.tzinfo is not None
    assert call.kwargs['namespace'] == '/socket.io'


def test_toast_includes_to_users_for_client_side_filtering(
    socketio_emit: mock.MagicMock,
) -> None:
    notify.toast('Hi', to_users=['alice', 'bob'])

    payload = socketio_emit.call_args.args[1]
    assert payload['to_users'] == ['alice', 'bob']


def test_toast_rejects_empty_title(socketio_emit: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.toast('')
    assert exc_info.value.code == 'notify.invalid_title'
    socketio_emit.assert_not_called()


def test_toast_rejects_unknown_level(socketio_emit: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.toast('Hi', level='emergency')  # type: ignore[arg-type]
    assert exc_info.value.code == 'notify.invalid_level'
    socketio_emit.assert_not_called()


def test_toast_rejects_negative_timeout(socketio_emit: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.toast('Hi', timeout_ms=-1)
    assert exc_info.value.code == 'notify.invalid_timeout'
    socketio_emit.assert_not_called()


def test_toast_rejects_empty_to_users_list(socketio_emit: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.toast('Hi', to_users=[])
    assert exc_info.value.code == 'notify.invalid_to_users'
    socketio_emit.assert_not_called()


def test_toast_rejects_blank_username(socketio_emit: mock.MagicMock) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.toast('Hi', to_users=['ok', ''])
    assert exc_info.value.code == 'notify.invalid_to_users'
    socketio_emit.assert_not_called()


# ---------------------------------------------------------------------------
# notify.atak
# ---------------------------------------------------------------------------


def test_atak_resolves_target_and_calls_cot_send(
    resolve_target: mock.MagicMock,
    cot_send: mock.MagicMock,
) -> None:
    resolve_target.return_value = 'ANDROID-bob-uid'

    event_uid = notify.atak('Bravo', 'Alert!', 'Drone overhead')

    resolve_target.assert_called_once_with('Bravo')
    cot_send.assert_called_once()
    kwargs = cot_send.call_args.kwargs
    assert kwargs['to_uids'] == ['ANDROID-bob-uid']
    assert kwargs['type'] == 'b-a-o-tbl'
    assert kwargs['callsign'] == 'Alert!'
    assert kwargs['remarks'] == 'Drone overhead'
    assert kwargs['stale'] == _dt.timedelta(minutes=5)
    assert isinstance(event_uid, str) and event_uid.startswith('notify-')


def test_atak_passes_custom_cot_type_and_stale(
    resolve_target: mock.MagicMock,
    cot_send: mock.MagicMock,
) -> None:
    resolve_target.return_value = 'uid-x'

    notify.atak(
        'uid-x',
        'Recall',
        cot_type='b-a-o-can',
        stale_minutes=15,
    )

    kwargs = cot_send.call_args.kwargs
    assert kwargs['type'] == 'b-a-o-can'
    assert kwargs['stale'] == _dt.timedelta(minutes=15)


def test_atak_raises_unknown_target_when_resolver_fails(
    cot_send: mock.MagicMock,
) -> None:
    """When the EUD lookup raises, atak() propagates the unknown_target code."""

    with mock.patch.object(notify, '_resolve_target_uid') as resolve:
        resolve.side_effect = OTSPluginError(
            code='notify.unknown_target',
            message='no such EUD',
        )
        with pytest.raises(OTSPluginError) as exc_info:
            notify.atak('Ghost', 'Title')

    assert exc_info.value.code == 'notify.unknown_target'
    cot_send.assert_not_called()


def test_atak_rejects_empty_title(
    resolve_target: mock.MagicMock,
    cot_send: mock.MagicMock,
) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.atak('uid-x', '')
    assert exc_info.value.code == 'notify.invalid_title'
    resolve_target.assert_not_called()
    cot_send.assert_not_called()


def test_atak_rejects_non_positive_stale(
    resolve_target: mock.MagicMock,
    cot_send: mock.MagicMock,
) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.atak('uid-x', 'Title', stale_minutes=0)
    assert exc_info.value.code == 'notify.invalid_stale'
    resolve_target.assert_not_called()
    cot_send.assert_not_called()


def test_resolve_target_rejects_empty_string() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify._resolve_target_uid('')
    assert exc_info.value.code == 'notify.invalid_target'


def test_resolve_target_returns_uid_when_eud_found(app: Flask) -> None:
    """Happy-path DB lookup mocked at the SQLAlchemy session seam.

    We pre-import the EUD/db modules so the local import inside
    ``_resolve_target_uid`` is a no-op (already in sys.modules), then
    patch the *attributes* on the live ``db`` extension object — that
    way the SQLAlchemy model classes never see a Mock as their owning
    db, and dataclass decoration succeeds.
    """

    from opentakserver import extensions as ext  # noqa: F401 — eagerly import
    from opentakserver.models.EUD import EUD  # noqa: F401

    fake_eud = SimpleNamespace(uid='ANDROID-resolved-uid')
    fake_scalar = mock.MagicMock()
    fake_scalar.scalar_one_or_none.return_value = fake_eud
    fake_session = mock.MagicMock()
    fake_session.execute.return_value = fake_scalar

    with mock.patch.object(ext.db, 'session', fake_session), mock.patch.object(
        ext.db, 'select', return_value=mock.MagicMock()
    ):
        with app.app_context():
            assert notify._resolve_target_uid('Bravo') == 'ANDROID-resolved-uid'


def test_resolve_target_raises_unknown_when_no_match(app: Flask) -> None:
    from opentakserver import extensions as ext  # noqa: F401
    from opentakserver.models.EUD import EUD  # noqa: F401

    fake_scalar = mock.MagicMock()
    fake_scalar.scalar_one_or_none.return_value = None
    fake_session = mock.MagicMock()
    fake_session.execute.return_value = fake_scalar

    with mock.patch.object(ext.db, 'session', fake_session), mock.patch.object(
        ext.db, 'select', return_value=mock.MagicMock()
    ):
        with app.app_context():
            with pytest.raises(OTSPluginError) as exc_info:
                notify._resolve_target_uid('Ghost')

    assert exc_info.value.code == 'notify.unknown_target'


# ---------------------------------------------------------------------------
# notify.broadcast
# ---------------------------------------------------------------------------


def test_broadcast_delegates_to_cot_broadcast(
    cot_broadcast: mock.MagicMock,
) -> None:
    event_uid = notify.broadcast('All Hands', 'Standby', stale_minutes=10)

    cot_broadcast.assert_called_once()
    kwargs = cot_broadcast.call_args.kwargs
    assert kwargs['type'] == 'b-a-o-tbl'
    assert kwargs['callsign'] == 'All Hands'
    assert kwargs['remarks'] == 'Standby'
    assert kwargs['stale'] == _dt.timedelta(minutes=10)
    assert event_uid == 'broadcast-uid-123'


def test_broadcast_rejects_empty_title(
    cot_broadcast: mock.MagicMock,
) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.broadcast('')
    assert exc_info.value.code == 'notify.invalid_title'
    cot_broadcast.assert_not_called()


def test_broadcast_rejects_non_positive_stale(
    cot_broadcast: mock.MagicMock,
) -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.broadcast('Title', stale_minutes=-1)
    assert exc_info.value.code == 'notify.invalid_stale'
    cot_broadcast.assert_not_called()


# ---------------------------------------------------------------------------
# notify.email
# ---------------------------------------------------------------------------


def test_email_returns_false_and_warns_when_disabled(
    app: Flask,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app.config['OTS_ENABLE_EMAIL'] = False

    with app.app_context(), caplog.at_level('WARNING', logger=notify.logger.name):
        result = notify.email('alex@example.com', 'Hi', 'Body')

    assert result is False
    assert any('OTS_ENABLE_EMAIL is disabled' in rec.message for rec in caplog.records)


def test_email_dispatches_via_flask_mailman_when_enabled(app: Flask) -> None:
    app.config['OTS_ENABLE_EMAIL'] = True

    sent_messages: list[mock.MagicMock] = []

    def make_message(*args: object, **kwargs: object) -> mock.MagicMock:
        msg = mock.MagicMock()
        msg.init_args = args
        msg.init_kwargs = kwargs
        sent_messages.append(msg)
        return msg

    with mock.patch('flask_mailman.EmailMessage', side_effect=make_message) as em:
        with app.app_context():
            ok = notify.email(['alex@example.com'], 'Subj', 'Body')

    assert ok is True
    assert em.call_count == 1
    kwargs = em.call_args.kwargs
    assert kwargs['subject'] == 'Subj'
    assert kwargs['body'] == 'Body'
    assert kwargs['to'] == ['alex@example.com']
    sent_messages[0].send.assert_called_once()


def test_email_uses_multi_alternatives_when_html_supplied(app: Flask) -> None:
    app.config['OTS_ENABLE_EMAIL'] = True

    sent: list[mock.MagicMock] = []

    def make_message(*args: object, **kwargs: object) -> mock.MagicMock:
        msg = mock.MagicMock()
        msg.init_args = args
        msg.init_kwargs = kwargs
        sent.append(msg)
        return msg

    with mock.patch(
        'flask_mailman.EmailMultiAlternatives', side_effect=make_message
    ) as ema, mock.patch('flask_mailman.EmailMessage') as em_plain:
        with app.app_context():
            ok = notify.email(
                'alex@example.com',
                'Hi',
                'plain body',
                html='<p>html body</p>',
            )

    assert ok is True
    assert ema.call_count == 1
    em_plain.assert_not_called()
    sent[0].attach_alternative.assert_called_once_with(
        '<p>html body</p>', 'text/html'
    )
    sent[0].send.assert_called_once()


def test_email_accepts_string_recipient(app: Flask) -> None:
    app.config['OTS_ENABLE_EMAIL'] = True

    with mock.patch('flask_mailman.EmailMessage') as em:
        em.return_value.send.return_value = None
        with app.app_context():
            ok = notify.email('one@example.com', 'Subj', 'Body')

    assert ok is True
    assert em.call_args.kwargs['to'] == ['one@example.com']


def test_email_rejects_empty_recipients() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.email([], 'Hi', 'Body')
    assert exc_info.value.code == 'notify.invalid_email_recipients'


def test_email_rejects_blank_recipient_in_list() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.email(['ok@example.com', ''], 'Hi', 'Body')
    assert exc_info.value.code == 'notify.invalid_email_recipients'


def test_email_rejects_empty_subject() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.email('alex@example.com', '', 'Body')
    assert exc_info.value.code == 'notify.invalid_email_subject'


def test_email_rejects_non_string_recipient_type() -> None:
    with pytest.raises(OTSPluginError) as exc_info:
        notify.email(123, 'Hi', 'Body')  # type: ignore[arg-type]
    assert exc_info.value.code == 'notify.invalid_email_recipients'
