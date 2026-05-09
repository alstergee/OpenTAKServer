"""``OTS.notify`` — user-friendly notification surface for v2 plugins.

Plugins import this module via
``from opentakserver.sdk.modules import notify`` (or, once
:mod:`opentakserver.sdk.ots_namespace` lands, via ``OTS.notify``).

Four helpers
------------

* :func:`toast` — push a Mantine notification to dashboard users via
  Socket.IO. Emits a ``'notification'`` event on the ``/socket.io``
  namespace, mirroring how the existing alerts pipeline broadcasts the
  ``'alert'`` event.
* :func:`atak` — send a CoT alert to a single EUD (by uid or callsign)
  via :func:`opentakserver.sdk.modules.cot.send`.
* :func:`broadcast` — same as :func:`atak` but to every connected EUD.
* :func:`email` — dispatch an email via Flask-Mailman, gated by the
  ``OTS_ENABLE_EMAIL`` config flag.

Permissions
-----------

``notify.*`` helpers are intentionally **not** scope-gated. They produce
user-visible side effects (a toast in someone's dashboard, an alert on
an ATAK device, an email) — there's no data exfiltration risk to
mediate, and forcing every plugin author to declare ``write = ["cot"]``
just to call ``notify.atak`` would be friction without payoff.
:func:`atak` and :func:`broadcast` *do* call :func:`cot.send`/`broadcast`
under the hood, which carry their own ``write = ["cot"]`` gate; the
helpers here run with whatever scope the calling plugin has at the
moment of invocation. Note that this means a plugin without
``write = ["cot"]`` cannot use :func:`atak` / :func:`broadcast` — by
design, they're a thin convenience layer, not a permission bypass.

Per-user targeting
------------------

Socket.IO does not currently maintain a ``username -> sid`` mapping
(:mod:`opentakserver.blueprints.ots_socketio` only logs ``current_user``
on connect — it doesn't ``join_room`` per username). Targeting a toast
at a specific user therefore has two viable shapes:

1. Server-side filtering — requires the new mapping, plus per-user
   rooms and a join hook. Out of scope for C.8.
2. Client-side filtering — the server broadcasts the ``'notification'``
   payload with a ``to_users`` field; the dashboard subscriber compares
   that field against the current user's ``username`` and drops the
   toast if it doesn't match.

We ship (2): :func:`toast` always broadcasts; ``to_users=None`` means
"everyone", and a non-empty list is included in the payload so the
client can filter. The B.4 dashboard subscriber will read this field.
This is captured in the architecture map under "Open architectural
decisions" as **D-6**.
"""

from __future__ import annotations

import datetime as _dt
import logging
from typing import Any, Literal

from flask import current_app

from opentakserver.extensions import socketio
from opentakserver.sdk.manifest import OTSPluginError
from opentakserver.sdk.modules import cot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


ToastLevel = Literal['info', 'success', 'warning', 'error']

_VALID_LEVELS: frozenset[str] = frozenset({'info', 'success', 'warning', 'error'})

_SOCKETIO_NAMESPACE = '/socket.io'
_NOTIFICATION_EVENT = 'notification'


# ---------------------------------------------------------------------------
# toast
# ---------------------------------------------------------------------------


def toast(
    title: str,
    message: str = '',
    level: ToastLevel = 'info',
    timeout_ms: int = 4000,
    to_users: list[str] | None = None,
) -> None:
    """Show a Mantine notification on the dashboard.

    The payload is broadcast on the ``'notification'`` Socket.IO event in
    the ``/socket.io`` namespace. Every connected dashboard receives it;
    when ``to_users`` is a list, the dashboard subscriber filters
    client-side by comparing the current user's username against the
    list. When ``to_users`` is ``None``, every user sees the toast.

    Parameters
    ----------
    title:
        Required, non-empty notification title. Rendered as the toast's
        bold heading.
    message:
        Optional body text. Defaults to ``''``.
    level:
        One of ``'info' | 'success' | 'warning' | 'error'``. Maps to the
        Mantine notification color/icon on the UI side.
    timeout_ms:
        Auto-dismiss timeout in milliseconds. ``0`` means "stay open
        until dismissed". Negative values are rejected.
    to_users:
        Optional list of usernames to target. ``None`` (default) means
        broadcast to everyone. Empty list ``[]`` is rejected — pass
        ``None`` for "everyone".

    Raises
    ------
    OTSPluginError
        If ``title`` is empty (``code='notify.invalid_title'``), if
        ``level`` is not a recognised value
        (``code='notify.invalid_level'``), if ``timeout_ms`` is negative
        (``code='notify.invalid_timeout'``), or if ``to_users`` is an
        empty list (``code='notify.invalid_to_users'``).
    """

    if not isinstance(title, str) or not title:
        raise OTSPluginError(
            code='notify.invalid_title',
            message='notify.toast: title must be a non-empty string',
        )
    if level not in _VALID_LEVELS:
        allowed = ', '.join(sorted(_VALID_LEVELS))
        raise OTSPluginError(
            code='notify.invalid_level',
            message=f'notify.toast: level must be one of [{allowed}], got {level!r}',
        )
    if not isinstance(timeout_ms, int) or timeout_ms < 0:
        raise OTSPluginError(
            code='notify.invalid_timeout',
            message='notify.toast: timeout_ms must be a non-negative int',
        )
    if to_users is not None:
        if not isinstance(to_users, list) or not to_users:
            raise OTSPluginError(
                code='notify.invalid_to_users',
                message=(
                    'notify.toast: to_users must be None (broadcast) or a '
                    'non-empty list of usernames'
                ),
            )
        for entry in to_users:
            if not isinstance(entry, str) or not entry:
                raise OTSPluginError(
                    code='notify.invalid_to_users',
                    message=(
                        'notify.toast: to_users entries must be non-empty strings'
                    ),
                )

    payload: dict[str, Any] = {
        'title': title,
        'message': message,
        'level': level,
        'timeout_ms': timeout_ms,
        'to_users': to_users,
        'timestamp': _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }
    socketio.emit(
        _NOTIFICATION_EVENT, payload, namespace=_SOCKETIO_NAMESPACE
    )
    logger.info(
        'notify.toast emitted',
        extra={
            'level': level,
            'to_users': to_users if to_users is not None else 'all',
            'title': title,
        },
    )


# ---------------------------------------------------------------------------
# ATAK helpers
# ---------------------------------------------------------------------------


def _resolve_target_uid(uid_or_callsign: str) -> str:
    """Resolve a uid-or-callsign string to an EUD uid.

    Returns the input unchanged if it already matches a uid. Falls back
    to looking up the callsign in the ``euds`` table.

    Raises
    ------
    OTSPluginError
        ``code='notify.unknown_target'`` if no EUD matches by uid OR
        callsign. ``code='notify.invalid_target'`` if the argument is
        empty / non-string.
    """

    if not isinstance(uid_or_callsign, str) or not uid_or_callsign:
        raise OTSPluginError(
            code='notify.invalid_target',
            message=(
                'notify.atak: uid_or_callsign must be a non-empty string'
            ),
        )

    # Local import to avoid pulling SQLAlchemy into the module's import
    # graph (keeps test fixtures cheap and avoids circular-import edge
    # cases during app boot).
    from opentakserver.extensions import db
    from opentakserver.models.EUD import EUD

    eud = db.session.execute(
        db.select(EUD).filter(
            (EUD.uid == uid_or_callsign) | (EUD.callsign == uid_or_callsign)
        )
    ).scalar_one_or_none()
    if eud is None:
        raise OTSPluginError(
            code='notify.unknown_target',
            message=(
                f'notify.atak: no EUD found with uid or callsign '
                f'{uid_or_callsign!r}'
            ),
        )
    return eud.uid


def atak(
    uid_or_callsign: str,
    title: str,
    message: str = '',
    cot_type: str = 'b-a-o-tbl',
    stale_minutes: int = 5,
) -> str:
    """Send an ATAK alert via CoT to a specific EUD (by uid or callsign).

    The CoT type defaults to ``b-a-o-tbl`` (alert / troops in contact).
    Returns the sent CoT event UID. Built on top of :func:`cot.send` so
    permissions, AMQP routing, and envelope shape match every other
    server-originated CoT.

    Parameters
    ----------
    uid_or_callsign:
        An EUD uid (e.g. ``'ANDROID-abc123'``) or a callsign
        (e.g. ``'Bravo'``). Resolved against the ``euds`` table.
    title:
        Required, non-empty alert title — rendered as the CoT
        ``callsign`` so ATAK shows it in the marker label.
    message:
        Optional alert body — rendered as the CoT ``remarks`` block.
    cot_type:
        CoT type. Defaults to ``'b-a-o-tbl'``. Pass any valid CoT type
        if the alert needs a different shape (``'b-a-o-can'``, etc.).
    stale_minutes:
        How long the CoT remains relevant. Defaults to 5 minutes.

    Raises
    ------
    OTSPluginError
        ``'notify.invalid_target'`` if ``uid_or_callsign`` is empty.
        ``'notify.unknown_target'`` if no EUD matches.
        ``'notify.invalid_title'`` if ``title`` is empty.
        ``'notify.invalid_stale'`` if ``stale_minutes`` is non-positive.
    """

    if not isinstance(title, str) or not title:
        raise OTSPluginError(
            code='notify.invalid_title',
            message='notify.atak: title must be a non-empty string',
        )
    if not isinstance(stale_minutes, int) or stale_minutes <= 0:
        raise OTSPluginError(
            code='notify.invalid_stale',
            message='notify.atak: stale_minutes must be a positive int',
        )

    target_uid = _resolve_target_uid(uid_or_callsign)
    event_uid = f'notify-{_dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S%f")}'
    cot.send(
        uid=event_uid,
        type=cot_type,
        lat=0.0,
        lon=0.0,
        callsign=title,
        remarks=message,
        to_uids=[target_uid],
        stale=_dt.timedelta(minutes=stale_minutes),
    )
    logger.info(
        'notify.atak sent',
        extra={
            'target_uid': target_uid,
            'cot_type': cot_type,
            'event_uid': event_uid,
        },
    )
    return event_uid


def broadcast(
    title: str,
    message: str = '',
    cot_type: str = 'b-a-o-tbl',
    stale_minutes: int = 5,
) -> str:
    """Send an ATAK alert via CoT to every connected EUD.

    Same shape as :func:`atak` but uses :func:`cot.broadcast` so the
    alert fans out via the broadcast exchange. Returns the broadcast
    event uid.

    Raises
    ------
    OTSPluginError
        ``'notify.invalid_title'`` / ``'notify.invalid_stale'`` on the
        same input rules as :func:`atak`.
    """

    if not isinstance(title, str) or not title:
        raise OTSPluginError(
            code='notify.invalid_title',
            message='notify.broadcast: title must be a non-empty string',
        )
    if not isinstance(stale_minutes, int) or stale_minutes <= 0:
        raise OTSPluginError(
            code='notify.invalid_stale',
            message='notify.broadcast: stale_minutes must be a positive int',
        )

    event_uid = cot.broadcast(
        type=cot_type,
        lat=0.0,
        lon=0.0,
        callsign=title,
        remarks=message,
        stale=_dt.timedelta(minutes=stale_minutes),
    )
    logger.info(
        'notify.broadcast sent',
        extra={'cot_type': cot_type, 'event_uid': event_uid},
    )
    return event_uid


# ---------------------------------------------------------------------------
# email
# ---------------------------------------------------------------------------


def email(
    to: list[str] | str,
    subject: str,
    body: str,
    html: str | None = None,
) -> bool:
    """Send an email via Flask-Mailman.

    Returns ``True`` when the message has been handed to Flask-Mailman's
    ``send()``, ``False`` if email is disabled (``OTS_ENABLE_EMAIL`` is
    falsy or unset). When disabled, logs a warning so plugin authors can
    diagnose why their email never went out.

    Parameters
    ----------
    to:
        Recipient address or list of addresses.
    subject:
        Email subject line. Must be non-empty.
    body:
        Plain-text body. Always sent; if ``html`` is also supplied, the
        message is multipart and includes both.
    html:
        Optional HTML alternative. When supplied, an
        ``EmailMultiAlternatives`` is built and the HTML is attached as
        ``text/html``.

    Raises
    ------
    OTSPluginError
        ``'notify.invalid_email_recipients'`` if ``to`` is empty / not a
        string-or-list-of-strings.
        ``'notify.invalid_email_subject'`` if ``subject`` is empty.
    """

    if isinstance(to, str):
        if not to:
            raise OTSPluginError(
                code='notify.invalid_email_recipients',
                message='notify.email: to must be a non-empty string or list',
            )
        recipients = [to]
    elif isinstance(to, list):
        if not to or not all(isinstance(x, str) and x for x in to):
            raise OTSPluginError(
                code='notify.invalid_email_recipients',
                message=(
                    'notify.email: to must be a non-empty list of address strings'
                ),
            )
        recipients = list(to)
    else:
        raise OTSPluginError(
            code='notify.invalid_email_recipients',
            message='notify.email: to must be str or list[str]',
        )

    if not isinstance(subject, str) or not subject:
        raise OTSPluginError(
            code='notify.invalid_email_subject',
            message='notify.email: subject must be a non-empty string',
        )

    if not current_app.config.get('OTS_ENABLE_EMAIL'):
        logger.warning(
            'notify.email called while OTS_ENABLE_EMAIL is disabled — dropping message',
            extra={'recipients': recipients, 'subject': subject},
        )
        return False

    # Local import — Flask-Mailman's EmailMessage uses ``current_app`` in
    # its constructor, so we want this lazy. Also keeps the import graph
    # of this module tiny for downstream consumers.
    from flask_mailman import EmailMessage, EmailMultiAlternatives

    if html is not None:
        msg = EmailMultiAlternatives(
            subject=subject, body=body, to=recipients
        )
        msg.attach_alternative(html, 'text/html')
    else:
        msg = EmailMessage(subject=subject, body=body, to=recipients)

    msg.send()
    logger.info(
        'notify.email sent',
        extra={'recipients': recipients, 'subject': subject},
    )
    return True


__all__ = [
    'ToastLevel',
    'atak',
    'broadcast',
    'email',
    'toast',
]
