import threading
import time
from threading import Thread

import flask_sqlalchemy
import pika
from flask import Flask
from pika.channel import Channel

from opentakserver.extensions import *


class RabbitMQClient:
    def __init__(self, context: Flask):
        self.context = context
        self.logger = logger
        self.db: flask_sqlalchemy.SQLAlchemy = db
        self.socketio = socketio

        self.online_euds = {}
        self.online_callsigns = {}
        self.exchanges = []
        self._reconnect_delay = 5
        self._reconnecting = threading.Lock()

        self._connect()

    def _connect(self):
        try:
            rabbit_credentials = pika.PlainCredentials(
                self.context.app.config.get("OTS_RABBITMQ_USERNAME"),
                self.context.app.config.get("OTS_RABBITMQ_PASSWORD"),
            )
            rabbit_host = self.context.app.config.get("OTS_RABBITMQ_SERVER_ADDRESS")
            self.rabbit_connection = pika.SelectConnection(
                pika.ConnectionParameters(host=rabbit_host, credentials=rabbit_credentials),
                self.on_connection_open,
            )
            self.rabbit_channel: Channel = None
            self.iothread = Thread(target=self.rabbit_connection.ioloop.start)
            self.iothread.daemon = True
            self.iothread.start()
            self.is_consuming = False
        except BaseException as e:
            self.logger.error("Failed to connect to rabbitmq: {}".format(e))
            return

    def _reconnect(self):
        # Prevent duplicate reconnect attempts (double close callback can fire)
        if not self._reconnecting.acquire(blocking=False):
            self.logger.warning("RabbitMQ reconnect already in progress, skipping")
            return
        try:
            self.logger.warning("RabbitMQ reconnecting in {} seconds...".format(self._reconnect_delay))
            time.sleep(self._reconnect_delay)

            try:
                self.rabbit_connection.ioloop.stop()
            except Exception:
                pass

            self.logger.info("RabbitMQ attempting reconnect...")
            self._connect()
        finally:
            self._reconnecting.release()

    def on_connection_open(self, connection):
        self.rabbit_connection.channel(on_open_callback=self.on_channel_open)
        self.rabbit_connection.add_on_close_callback(self.on_close)

    def on_channel_open(self, channel):
        raise NotImplementedError

    def on_close(self, channel, error):
        self.logger.error("RabbitMQ connection closed: {}".format(error))
        reconnect_thread = Thread(target=self._reconnect)
        reconnect_thread.daemon = True
        reconnect_thread.start()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        raise NotImplementedError
