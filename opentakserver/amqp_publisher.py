"""
Singleton AMQP publisher.

Replaces the 26 per-call `pika.BlockingConnection(...)` sites scattered across
the codebase. Each of those was opening a fresh TCP/TLS handshake + AMQP
handshake + channel + close per request — chat sends spent ~80ms in connection
overhead alone, and RabbitMQ saw connection churn equal to the API request
rate. Audit 2026-05-08 finding H-B4/H-B7.

The reusable approach:
  * One module-level connection per process, lazily created on first publish
  * Single channel cached on the connection
  * Reconnect on `ConnectionClosed` / `ChannelClosed` with exponential backoff
    capped at 30s
  * `socket_timeout=5` and `heartbeat=30` so a slow RabbitMQ stops blocking
    Flask threads indefinitely
  * `threading.Lock()` around the publish path — pika.BlockingConnection is
    NOT thread-safe; multi-worker Flask + apscheduler threads need
    serialisation
  * Caller passes credentials via the existing `app.config` keys so callers
    don't change shape

Use:

    from opentakserver.amqp_publisher import publish
    publish(exchange="chatrooms", routing_key=room_id, body=payload)
    publish(exchange="amq.topic", routing_key="msh.2.e.STAFF.outgoing",
            body=service_envelope.SerializeToString(),
            properties=pika.BasicProperties(expiration=ttl))
"""

import logging
import threading
import time

import pika
from flask import current_app


logger = logging.getLogger(__name__)


class _Publisher:
    def __init__(self):
        self._lock = threading.Lock()
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self._reconnect_delay = 1.0  # exponential, capped at 30s

    def _params(self):
        cfg = current_app.config
        creds = pika.PlainCredentials(
            cfg.get("OTS_RABBITMQ_USERNAME", "guest"),
            cfg.get("OTS_RABBITMQ_PASSWORD", "guest"),
        )
        return pika.ConnectionParameters(
            host=cfg.get("OTS_RABBITMQ_SERVER_ADDRESS", "rabbitmq"),
            credentials=creds,
            socket_timeout=5,
            heartbeat=30,
            blocked_connection_timeout=30,
            connection_attempts=3,
            retry_delay=1,
        )

    def _ensure_open(self):
        """Open connection + channel if not already. Caller holds the lock."""
        if self._channel is not None and self._channel.is_open:
            return
        # Fresh connection / channel
        try:
            if self._connection is None or self._connection.is_closed:
                self._connection = pika.BlockingConnection(self._params())
            self._channel = self._connection.channel()
            self._reconnect_delay = 1.0  # reset backoff on success
        except Exception as e:
            logger.warning("amqp_publisher: connect failed: %s", e)
            self._channel = None
            self._connection = None
            raise

    def publish(self, *, exchange, routing_key, body, properties=None):
        """Publish a single message. Reconnects up to 3 times on
        ConnectionClosed/ChannelClosed. Raises only after all retries fail."""
        last_err = None
        for attempt in range(3):
            with self._lock:
                try:
                    self._ensure_open()
                    self._channel.basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=body,
                        properties=properties,
                    )
                    return
                except (pika.exceptions.ConnectionClosed,
                        pika.exceptions.ChannelClosed,
                        pika.exceptions.StreamLostError,
                        pika.exceptions.AMQPConnectionError,
                        OSError) as e:
                    last_err = e
                    logger.warning(
                        "amqp_publisher: publish attempt %d/3 failed (%s); reconnecting",
                        attempt + 1, e,
                    )
                    self._channel = None
                    self._connection = None
            # exponential backoff outside the lock
            time.sleep(min(self._reconnect_delay, 30.0))
            self._reconnect_delay = min(self._reconnect_delay * 2, 30.0)
        if last_err is not None:
            raise last_err

    def close(self):
        with self._lock:
            try:
                if self._connection is not None and self._connection.is_open:
                    self._connection.close()
            except Exception:
                pass
            self._connection = None
            self._channel = None


_publisher: _Publisher | None = None


def _get():
    global _publisher
    if _publisher is None:
        _publisher = _Publisher()
    return _publisher


def publish(*, exchange, routing_key, body, properties=None):
    """Convenience wrapper — most callers want this."""
    _get().publish(exchange=exchange, routing_key=routing_key,
                   body=body, properties=properties)


def close():
    """Tear down the singleton. Call from app shutdown hooks."""
    global _publisher
    if _publisher is not None:
        _publisher.close()
        _publisher = None
