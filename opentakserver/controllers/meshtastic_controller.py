import base64
import datetime
import json
import traceback
import uuid

import pika
import unishox2
import os

from meshtastic import mqtt_pb2, portnums_pb2, mesh_pb2, protocols, BROADCAST_NUM

from opentakserver.models.Meshtastic import MeshtasticChannel
from opentakserver.proto import atak_pb2
from google.protobuf.json_format import MessageToJson
from xml.etree.ElementTree import Element, SubElement, tostring

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from opentakserver.controllers.rabbitmq_client import RabbitMQClient
from opentakserver.models.EUD import EUD
from opentakserver.models.GeoChat import GeoChat
from opentakserver.models.Chatrooms import Chatroom
from opentakserver.models.CoT import CoT
from opentakserver.models.Point import Point

# >>> pipeline_trace instrumentation <<<
# Wire OTS-side pipeline trace events into InfluxDB. pipeline_trace.py
# is shipped to /app/scripts via the same bind-mount that this file
# uses; if it's not there, fall back to no-op so the controller still
# works.
import sys as _pt_sys
_pt_sys.path.insert(0, '/app/scripts')
try:
    from pipeline_trace import trace as _pt_trace, scope as _pt_scope
except ImportError:
    def _pt_trace(*args, **kwargs):
        pass
    class _PtNullScope:
        def __init__(self):
            self.tags = {}
            self.fields = {}
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
    def _pt_scope(*args, **kwargs):
        return _PtNullScope()
# <<< end pipeline_trace instrumentation >>>



class _JsonPayload:
    """Mimics protobuf attribute access from a JSON dict."""
    def __init__(self, data):
        object.__setattr__(self, '_data', data if isinstance(data, dict) else {})

    def __getattr__(self, name):
        val = self._data.get(name, 0)
        if isinstance(val, dict):
            return _JsonPayload(val)
        return val

    def HasField(self, name):
        return name in self._data

    def decode(self, encoding='utf-8', errors='strict'):
        return self._data.get('text', str(self._data))


class MeshtasticController(RabbitMQClient):
    def __init__(self, context):
        super().__init__(context)
        self.node_names = {}
        self.logger.info("Starting Meshtastic controller...")
        self.meshtastic_devices = {}
        self.get_euds()
        self.get_channels()

    def get_euds(self):
        with self.context:
            euds = self.db.session.execute(self.db.session.query(EUD)).scalars()
            for eud in euds:
                meshtastic_id = eud.meshtastic_id
                if not eud.meshtastic_id:
                    eud.meshtastic_id = int.from_bytes(os.urandom(4), 'big')
                    self.db.session.add(eud)
                    self.db.session.commit()
                self.meshtastic_devices[eud.uid] = {'hw_model': eud.device, 'long_name': eud.callsign, 'short_name': '',
                                                    'firmware_version': eud.version, 'last_lat': "0.0",
                                                    'last_lon': "0.0",
                                                    'battery': 0, 'meshtastic_id': '{:x}'.format(eud.meshtastic_id),
                                                    'voltage': 0,
                                                    'uptime': 0, 'last_alt': "9999999.0", 'course': '0.0',
                                                    'speed': '0.0', 'team': 'Cyan', 'role': 'Team Member',
                                                    'uid': eud.uid,
                                                    'macaddr': eud.meshtastic_macaddr}

    def get_channels(self):
        with self.context:
            channels = self.db.session.execute(self.db.session.query(MeshtasticChannel)).scalars()
            downlink_channels = []
            for channel in channels:
                if channel.downlink_enabled:
                    downlink_channels.append(channel.name)
            self.context.app.config.update({"OTS_MESHTASTIC_DOWNLINK_CHANNELS": downlink_channels})

    def on_channel_open(self, channel):
        self.rabbit_channel = channel
        self.rabbit_channel.queue_declare(queue='meshtastic')
        self.rabbit_channel.queue_bind(exchange='amq.topic', queue='meshtastic', routing_key="#")
        self.rabbit_channel.basic_consume(queue='meshtastic', on_message_callback=self.on_message, auto_ack=True)
        # Do NOT register on_close on channel — base class handles it on the connection
        # Double registration causes racing reconnect threads (bug #3)

    def try_decode(self, mp):
        # Get the channel key from the DB
        key_bytes = base64.b64decode("1PG7OiApB1nwvP+rz05pAQ==".encode('ascii'))

        nonce = getattr(mp, "id").to_bytes(8, "little") + getattr(mp, "from").to_bytes(8, "little")
        cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

        data = mesh_pb2.Data()
        data.ParseFromString(decrypted_bytes)
        mp.decoded.CopyFrom(data)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        # Extract the mesh channel name from the routing key so downstream
        # handlers (text_message in particular) can tag GeoChat rows with the
        # actual channel name instead of the legacy "All Chat Rooms" sink.
        # Routing key format: <mqtt_root>.2.e.<channel_name>.<gateway_id>
        rk_parts = basic_deliver.routing_key.split(".")
        self._current_mesh_channel = rk_parts[3] if len(rk_parts) >= 5 else None

        # >>> pipeline_trace step 20: decode.consume <<<
        try:
            _pt_trace("decode.consume",
                      ch=basic_deliver.routing_key.split(".")[-2] if "." in basic_deliver.routing_key else None,
                      bytes=len(body or b""), component="ots-meshtastic")
        except Exception:
            pass
        # Don't process outgoing message from TAK EUDs to the Meshtastic Network, only messages from the Meshtastic
        # network to TAK EUDs
        if basic_deliver.routing_key.endswith('outgoing'):
            return

        # NOTE (2026-05-08): The previous "mesh-source-gate" cross-channel
        # bridge has been removed. It re-published every mesh-side message to
        # every other configured channel as `<topic>/outgoing`, which caused:
        #   * a feedback storm (chip relays the outgoing on LoRa, hears its
        #     own rebroadcast, republishes to MQTT, we re-ingest, repeat),
        #   * 4x DB row inflation per mesh message,
        #   * messages appearing in the wrong channel tabs because every
        #     channel got a copy of every message.
        # If cross-channel broadcast is desired, that's the user's job from
        # the ALL tab in the web UI — which goes through the proper
        # broadcast_channels path in send_geochat (one DB row, one publish
        # per target channel, no chip rebroadcast loop).

        se = mqtt_pb2.ServiceEnvelope()
        try:
            se.ParseFromString(body)
            # >>> pipeline_trace step 21: decode.protobuf success <<<
            try:
                _pt_trace("decode.protobuf", pkt=getattr(se, 'packet', None) and se.packet.id or 0,
                          ch=basic_deliver.routing_key.split(".")[-2] if "." in basic_deliver.routing_key else None,
                          component="ots-meshtastic")
            except Exception:
                pass
            mp = se.packet
        except Exception as e:
            # Try JSON fallback for gateways with JSON mode enabled
            try:
                self._handle_json_message(body, basic_deliver)
                return
            except Exception:
                pass
            self.logger.error(f"ERROR: parsing service envelope: {str(e)}")
            self.logger.error(f"{body}")
            return

        meshtastic_id = getattr(mp, 'from')
        meshtastic_id = f"{meshtastic_id:08x}"
        to_id = mp.to
        if to_id == BROADCAST_NUM:
            to_id = 'all'
        else:
            to_id = f"{to_id:08x}"

        pn = portnums_pb2.PortNum.Name(mp.decoded.portnum)

        prefix = f"{mp.channel} [{meshtastic_id}->{to_id}] {pn}:"
        if mp.HasField("encrypted") and not mp.HasField("decoded"):
            try:
                self.try_decode(mp)
                pn = portnums_pb2.PortNum.Name(mp.decoded.portnum)
                prefix = f"{mp.channel} [{meshtastic_id}->{to_id}] {pn}:"
            except Exception as e:
                self.logger.warning(f"{prefix} could not be decrypted")
                return

        handler = protocols.get(mp.decoded.portnum)
        # >>> pipeline_trace step 22: decode.dispatch <<<
        try:
            _pt_trace("decode.dispatch", pkt=mp.id,
                      ch=basic_deliver.routing_key.split(".")[-2] if "." in basic_deliver.routing_key else None,
                      result="ok" if handler is not None else "no_handler",
                      portnum=int(mp.decoded.portnum), component="ots-meshtastic")
        except Exception:
            pass
        if handler is None:
            try:
                if portnums_pb2.PortNum.Name(mp.decoded.portnum) == "ATAK_PLUGIN":
                    tak_packet = atak_pb2.TAKPacket()
                    tak_packet.ParseFromString(mp.decoded.payload)
                    self.protobuf_to_cot(tak_packet, meshtastic_id, to_id, pn, meshtastic_id)
                    self.logger.info(tak_packet)
            except:
                self.logger.error(traceback.format_exc())

            return

        if handler.protobufFactory is None:
            self.logger.debug(f"{prefix} {mp}")
            self.protobuf_to_cot(mp.decoded.payload, meshtastic_id, to_id, pn, meshtastic_id)
            self.logger.info(mp.decoded.payload)
        else:
            try:
                pb = handler.protobufFactory()
                pb.ParseFromString(mp.decoded.payload)
                p = MessageToJson(pb)
                if mp.decoded.portnum == portnums_pb2.PortNum.NODEINFO_APP:
                    self.node_names[getattr(mp, "from")] = pb.short_name
                    prefix = f"{mp.channel} [{meshtastic_id}->{to_id}] {pn}:"
                    self.rabbit_channel.queue_declare(queue=meshtastic_id)
                self.logger.debug(f"{prefix} {p}")
                self.protobuf_to_cot(pb, meshtastic_id, to_id, pn, meshtastic_id)
                self.logger.info(pb)
            except:
                self.logger.error(traceback.format_exc())

    def _handle_json_message(self, body, basic_deliver):
        """Handle Meshtastic JSON messages (gateway JSON mode enabled)."""
        data = json.loads(body)

        msg_type = data.get('type', '')
        type_map = {
            'position': 'POSITION_APP',
            'text': 'TEXT_MESSAGE_APP',
            'nodeinfo': 'NODEINFO_APP',
            'telemetry': 'TELEMETRY_APP',
            'mapreport': 'MAP_REPORT_APP',
        }

        portnum = type_map.get(msg_type)
        if not portnum:
            return

        from_int = data.get('from', 0)
        # Some gateways send `from` as a decimal-stringified int in JSON mode
        # (e.g. "2663195777"), others as a real int, others as "!hexid". Normalize
        # all three to the same 8-char lowercase hex form the protobuf path uses,
        # so a chip publishing in both formats shows up under one identity.
        if isinstance(from_int, int):
            meshtastic_id = f"{from_int:08x}"
        else:
            s = str(from_int).lstrip('!')
            meshtastic_id = f"{int(s):08x}" if s.isdigit() else s.lower()

        to_int = data.get('to', 0)
        if to_int == BROADCAST_NUM:
            to_id = 'all'
        else:
            to_id = f"{to_int:08x}" if isinstance(to_int, int) else str(to_int)

        payload = data.get('payload', data)

        if portnum == 'TEXT_MESSAGE_APP':
            text = payload if isinstance(payload, str) else payload.get('text', str(payload))
            pb = text.encode('utf-8')
        else:
            pb = _JsonPayload(payload if isinstance(payload, dict) else data)

        self.logger.info(f"JSON [{meshtastic_id}] {portnum}")
        self.protobuf_to_cot(pb, meshtastic_id, to_id, portnum, meshtastic_id)

    def cot(self, pb, from_id, to_id, portnum, how='m-g', cot_type='a-f-G-U-C', uid=None):
        if not uid and from_id in self.meshtastic_devices and self.meshtastic_devices[from_id]['uid']:
            uid = self.meshtastic_devices[from_id]['uid']
        elif not uid:
            uid = from_id

        def _s(val, default=''):
            """Safely stringify a value for XML attributes."""
            return str(val) if val is not None else default

        dev = self.meshtastic_devices.get(from_id, {})

        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        stale = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

        event = Element('event', {'how': how, 'type': cot_type, 'version': '2.0',
                                  'uid': _s(uid, from_id), 'start': now, 'time': now, 'stale': stale})

        SubElement(event, 'point', {'ce': '9999999.0', 'le': '9999999.0',
                                    'hae': _s(dev.get('last_alt'), '9999999.0'),
                                    'lat': _s(dev.get('last_lat'), '0.0'),
                                    'lon': _s(dev.get('last_lon'), '0.0')})

        detail = SubElement(event, 'detail')
        if portnum == "TEXT_MESSAGE_APP" or (portnum == "ATAK_PLUGIN" and pb.HasField('chat')):
            return event, detail
        else:
            SubElement(detail, 'takv', {'device': _s(dev.get('hw_model')),
                                        'version': _s(dev.get('firmware_version')),
                                        'platform': 'Meshtastic', 'os': 'Meshtastic',
                                        'macaddr': _s(dev.get('macaddr')),
                                        'meshtastic_id': _s(dev.get('meshtastic_id'))})
            SubElement(detail, 'contact',
                       {'callsign': _s(dev.get('long_name'), from_id), 'endpoint': 'MQTT'})
            SubElement(detail, 'uid', {'Droid': _s(dev.get('long_name'), from_id)})
            SubElement(detail, 'precisionlocation', {'altsrc': 'GPS', 'geopointsrc': 'GPS'})
            SubElement(detail, 'status', {'battery': _s(dev.get('battery'), '0')})
            SubElement(detail, 'track', {'course': '0.0', 'speed': '0.0'})
            SubElement(detail, '__group', {'name': _s(dev.get('team'), 'Cyan'),
                                           'role': _s(dev.get('role'), 'Team Member')})
        return event

    def position(self, pb, from_id, to_id, portnum):
        try:
            if portnum == "MAP_REPORT_APP" and pb.firmware_version != self.meshtastic_devices[from_id]['firmware_version']:
                try:
                    with self.context:
                        eud = self.db.session.execute(self.db.session.query(EUD).filter_by(uid=from_id)).first()[0]
                        eud.version = pb.firmware_version
                        eud.device = pb.hw_model
                        # Don't downgrade to empty: only update callsign if device sent one.
                        if pb.long_name and str(pb.long_name).strip():
                            eud.callsign = pb.long_name
                        self.db.session.add(eud)
                        self.db.session.commit()
                        if from_id not in self.meshtastic_devices:
                            self.meshtastic_devices[from_id] = {'hw_model': '', 'long_name': '', 'short_name': '',
                                                                'macaddr': '',
                                                                'firmware_version': '', 'last_lat': "0.0",
                                                                'last_lon': "0.0",
                                                                'battery': 0, 'meshtastic_id': '',
                                                                'voltage': 0, 'uptime': 0, 'last_alt': "9999999.0",
                                                                'course': '0.0',
                                                                'speed': '0.0', 'team': 'Cyan', 'role': 'Team Member',
                                                                'uid': None}

                        self.meshtastic_devices[from_id]['firmware_version'] = pb.firmware_version
                        self.meshtastic_devices[from_id]['hw_model'] = mesh_pb2.HardwareModel.Name(pb.hw_model)
                        self.meshtastic_devices[from_id]['long_name'] = pb.long_name
                        self.meshtastic_devices[from_id]['short_name'] = pb.short_name
                except BaseException as e:
                    self.logger.error("Failed to update {}'s firmware version: {}".format(from_id, e))

            self.meshtastic_devices[from_id]['last_lat'] = pb.latitude_i * .0000001
            self.meshtastic_devices[from_id]['last_lon'] = pb.longitude_i * .0000001
            self.meshtastic_devices[from_id]['last_alt'] = pb.altitude
            if portnum == "POSITION_APP":
                self.meshtastic_devices[from_id]['course'] = pb.ground_track if pb.ground_track else "0.0"
                self.meshtastic_devices[from_id]['speed'] = pb.ground_speed if pb.ground_speed else "0.0"

            return self.cot(pb, from_id, to_id, portnum)
        except BaseException as e:
            self.logger.error("Failed to create CoT: {}".format(str(e)))
            self.logger.error(traceback.format_exc())
            return

    def text_message(self, pb, from_id, to_id, portnum):
        callsign = from_id
        if from_id in self.meshtastic_devices:
            callsign = self.meshtastic_devices[from_id]['long_name']

        # If the message was a direct message to a known peer, route it to that
        # peer's per-user DM "chatroom". Otherwise tag it with the mesh channel
        # name we captured in on_message — this is what makes channel-specific
        # tabs (ALLCALL / SECURITY / PRODUCTION / STAFF) actually contain their
        # own messages instead of dumping everything into a single sink.
        chatroom = None
        for meshtastic_device in self.meshtastic_devices:
            meshtastic_device = self.meshtastic_devices[meshtastic_device]
            if meshtastic_device['meshtastic_id'] == to_id:
                chatroom = meshtastic_device['uid']
                break
        if chatroom is None:
            chatroom = getattr(self, '_current_mesh_channel', None) or "All Chat Rooms"

        if from_id in self.meshtastic_devices and self.meshtastic_devices[from_id]['uid']:
            from_uid = self.meshtastic_devices[from_id]['uid']
        else:
            from_uid = from_id

        message_uid = str(uuid.uuid4())
        event, detail = self.cot(pb, from_uid, chatroom, portnum, how='h-g-i-g-o', cot_type='b-t-f',
                                 uid="GeoChat.{}.{}.{}".format(from_uid, chatroom, message_uid))

        chat = SubElement(detail, '__chat',
                          {'chatroom': chatroom, 'groupOwner': "false", 'id': chatroom,
                           'messageId': message_uid, 'parent': 'RootContactGroup',
                           'senderCallsign': callsign})
        SubElement(chat, 'chatgrp', {'id': chatroom, 'uid0': from_uid, 'uid1': chatroom})
        SubElement(detail, 'link', {'relation': 'p-p', 'type': 'a-f-G-U-C', 'uid': from_uid})
        remarks = SubElement(detail, 'remarks', {'source': 'BAO.F.ATAK.{}'.format(from_uid),
                                                 'time': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                 'to': chatroom})

        text = pb.decode('utf-8', 'replace')
        remarks.text = text

        # Save directly to geochat DB (cot_parser can't handle messages without GPS)
        self.save_chat_to_db(text, from_uid, chatroom, callsign)

        return event

    def save_chat_to_db(self, text, from_id, chatroom, callsign):
        """Insert chat message directly into geochat table, bypassing cot_parser."""
        try:
            with self.context:
                now = datetime.datetime.now(datetime.timezone.utc)

                # Dedupe: a chip publishing the same message in both protobuf
                # and JSON form (or a LoRa relay echo) hits this function twice
                # within ~1 second. Reject if an identical row already exists
                # in the last 10 seconds — same channel, same sender, same text.
                window_start = now - datetime.timedelta(seconds=10)
                dup = (self.db.session.query(GeoChat)
                       .filter(GeoChat.chatroom_id == chatroom,
                               GeoChat.sender_uid == from_id,
                               GeoChat.remarks == text,
                               GeoChat.timestamp >= window_start)
                       .first())
                if dup is not None:
                    self.logger.info("Skipping duplicate mesh chat: [{}] {}: {}".format(chatroom, callsign, text[:50]))
                    return

                msg_uid = "GeoChat.{}.{}.{}".format(from_id, chatroom, str(uuid.uuid4()))

                # Ensure sender EUD exists (idempotent: defaults empty callsign + tolerates concurrent inserts)
                eud = self.db.session.query(EUD).filter_by(uid=from_id).first()
                if not eud:
                    # Empty/whitespace callsigns collide on euds_callsign_key UNIQUE; use uid-derived default.
                    eud_callsign = callsign if callsign and str(callsign).strip() else 'meshtastic_{}'.format(from_id)
                    eud = EUD()
                    eud.uid = from_id
                    eud.callsign = eud_callsign
                    eud.device = 'Meshtastic'
                    eud.os = 'Meshtastic'
                    eud.platform = 'Meshtastic'
                    eud.version = '1.0'
                    eud.last_event_time = now
                    eud.last_status = 'Online'
                    try:
                        self.db.session.add(eud)
                        self.db.session.flush()
                    except Exception as _eud_race:
                        # Race with another concurrent insert for same uid — rollback and refetch.
                        self.db.session.rollback()
                        eud = self.db.session.query(EUD).filter_by(uid=from_id).first()
                        if eud is None:
                            raise

                # Ensure chatroom exists
                cr = self.db.session.query(Chatroom).filter_by(id=chatroom).first()
                if not cr:
                    cr = Chatroom()
                    cr.id = chatroom
                    cr.name = chatroom
                    cr.parent = 'RootContactGroup'
                    self.db.session.add(cr)
                    self.db.session.flush()

                # CoT record
                cot = CoT()
                cot.uid = msg_uid
                cot.type = 'b-t-f'
                cot.how = 'h-g-i-g-o'
                cot.sender_uid = from_id
                cot.sender_callsign = callsign
                cot.timestamp = now
                cot.start = now
                cot.stale = now + datetime.timedelta(minutes=5)
                cot.xml = ''
                self.db.session.add(cot)
                self.db.session.flush()

                # Point record
                pt = Point()
                pt.uid = from_id
                pt.device_uid = from_id
                pt.latitude = 0
                pt.longitude = 0
                pt.ce = 9999999
                pt.le = 9999999
                pt.hae = 0
                pt.timestamp = now
                self.db.session.add(pt)
                self.db.session.flush()

                # GeoChat record
                gc = GeoChat()
                gc.uid = msg_uid
                gc.chatroom_id = chatroom
                gc.sender_uid = from_id
                gc.remarks = text
                gc.timestamp = now
                gc.point_id = pt.id
                gc.cot_id = cot.id
                self.db.session.add(gc)
                self.db.session.commit()
                self.logger.info("Saved mesh chat: [{}] {}: {}".format(chatroom, callsign, text[:50]))
        except Exception as e:
            self.logger.error("Failed to save chat to DB: {}".format(e))
            try:
                self.db.session.rollback()
            except Exception:
                pass

    def node_info(self, pb, from_id, to_id, portnum):
        if portnum == "ATAK_PLUGIN":
            uid = unishox2.decompress(pb.contact.device_callsign, len(pb.contact.device_callsign))
            self.meshtastic_devices[from_id]['uid'] = uid
            self.meshtastic_devices[from_id]['long_name'] = unishox2.decompress(pb.contact.callsign,
                                                                                len(pb.contact.callsign))
            self.meshtastic_devices[from_id]['short_name'] = uid[-4:]
            self.meshtastic_devices[from_id]['battery'] = pb.status.battery
            if pb.group.team != 0:
                self.meshtastic_devices[from_id]['team'] = atak_pb2.Team.Name(pb.group.team)
            if pb.group.role != 0:
                self.meshtastic_devices[from_id]['role'] = atak_pb2.MemberRole.Name(pb.group.role)
        else:
            hw_model = mesh_pb2.HardwareModel.Name(pb.hw_model)
            self.meshtastic_devices[from_id]['hw_model'] = hw_model if hw_model else ""
            self.meshtastic_devices[from_id]['long_name'] = str(pb.long_name) if pb.long_name else ""
            self.meshtastic_devices[from_id]['short_name'] = str(pb.short_name) if pb.short_name else ""
            self.meshtastic_devices[from_id]['macaddr'] = base64.b64encode(pb.macaddr).decode(
                'ascii') if pb.macaddr else ""

        return self.cot(pb, from_id, to_id, portnum)

    def telemetry(self, pb, from_id, to_id, portnum):
        if pb.HasField('device_metrics'):
            self.meshtastic_devices[from_id]['battery'] = pb.device_metrics.battery_level
            self.meshtastic_devices[from_id]['voltage'] = pb.device_metrics.voltage
            self.meshtastic_devices[from_id]['uptime'] = pb.device_metrics.uptime_seconds
        elif pb.HasField('environment_metrics'):
            self.meshtastic_devices[from_id]['temperature'] = pb.environment_metrics.temperature
            self.meshtastic_devices[from_id]['relative_humidity'] = pb.environment_metrics.relative_humidity
            self.meshtastic_devices[from_id]['barometric_pressure'] = pb.environment_metrics.barometric_pressure
            self.meshtastic_devices[from_id]['gas_resistance'] = pb.environment_metrics.gas_resistance
            self.meshtastic_devices[from_id]['voltage'] = pb.environment_metrics.voltage
            self.meshtastic_devices[from_id]['current'] = pb.environment_metrics.current
            self.meshtastic_devices[from_id]['iaq'] = pb.environment_metrics.iaq

    def atak_plugin(self, pb, from_id, to_id, portnum):
        self.node_info(pb, from_id, to_id, portnum)

        if pb.HasField('status'):
            self.meshtastic_devices[from_id]['battery'] = pb.status.battery

        if pb.HasField('pli'):
            self.meshtastic_devices[from_id]['last_lat'] = pb.pli.latitude_i * .0000001
            self.meshtastic_devices[from_id]['last_lon'] = pb.pli.longitude_i * .0000001
            self.meshtastic_devices[from_id]['last_alt'] = pb.pli.altitude
            self.meshtastic_devices[from_id]['course'] = pb.pli.course
            self.meshtastic_devices[from_id]['speed'] = pb.pli.speed
            return self.cot(pb, from_id, to_id, portnum)
        elif pb.HasField('chat'):
            self.logger.debug(
                "Got chat: {} {}->{}: {}".format(unishox2.decompress(pb.chat.to, len(pb.chat.to)), from_id, to_id,
                                                 unishox2.decompress(pb.chat.message, len(pb.chat.message))))

            chatroom = unishox2.decompress(pb.chat.to, len(pb.chat.to))
            message_uid = str(uuid.uuid4())

            from_uid = sender_callsign = from_id
            if from_uid in self.meshtastic_devices:
                from_uid = self.meshtastic_devices[from_id]['uid']
                sender_callsign = self.meshtastic_devices[from_id]['long_name']

            uid = "GeoChat.{}.{}.{}".format(from_uid, chatroom, message_uid)

            event, detail = self.cot(pb, from_uid, to_id, portnum, how='h-g-i-g-o', cot_type='b-t-f', uid=uid)

            chat = SubElement(detail, '__chat',
                              {'chatroom': 'All Chat Rooms', 'groupOwner': "false", 'id': chatroom,
                               'messageId': message_uid, 'parent': 'RootContactGroup',
                               'senderCallsign': sender_callsign})
            SubElement(chat, 'chatgrp', {'id': chatroom, 'uid0': from_uid, 'uid1': chatroom})
            SubElement(detail, 'link', {'relation': 'p-p', 'type': 'a-f-G-U-C', 'uid': from_uid})
            remarks = SubElement(detail, 'remarks', {'source': 'BAO.F.ATAK.{}'.format(from_uid),
                                                     'time': datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                     'to': chatroom})
            remarks.text = unishox2.decompress(pb.chat.message, len(pb.chat.message))

            return event

    def protobuf_to_cot(self, pb, from_id, to_id, portnum, meshtastic_id):
        # >>> pipeline_trace step 24: cot.translate entry <<<
        try:
            _pt_trace("cot.translate", pkt=from_id if isinstance(from_id, int) else 0,
                      reason=str(portnum), component="ots-meshtastic")
        except Exception:
            pass
        self.logger.debug(from_id + " " + to_id + " " + portnum + " " + meshtastic_id)
        event = None

        if from_id not in self.meshtastic_devices:
            self.meshtastic_devices[from_id] = {'hw_model': '', 'long_name': '', 'short_name': '', 'macaddr': '',
                                                'firmware_version': '', 'last_lat': "0.0", 'last_lon': "0.0",
                                                'battery': 0, 'meshtastic_id': meshtastic_id,
                                                'voltage': 0, 'uptime': 0, 'last_alt': "9999999.0", 'course': '0.0',
                                                'speed': '0.0', 'team': 'Cyan', 'role': 'Team Member', 'uid': None}

        if portnum == "MAP_REPORT_APP" or (portnum == "POSITION_APP" and pb.latitude_i):
            event = self.position(pb, from_id, to_id, portnum)
        elif portnum == "NODEINFO_APP":
            event = self.node_info(pb, from_id, to_id, portnum)
        elif portnum == "TEXT_MESSAGE_APP":
            event = self.text_message(pb, from_id, to_id, portnum)
        elif portnum == "ATAK_PLUGIN":
            event = self.atak_plugin(pb, from_id, to_id, portnum)
        elif portnum == "TELEMETRY_APP":
            self.telemetry(pb, from_id, to_id, portnum)

        try:
            if event:
                uid = self.meshtastic_devices[from_id]['uid']
                if not uid:
                    uid = from_id
                message = json.dumps({'uid': uid, 'cot': tostring(event).decode('utf-8')})
                if portnum == "TEXT_MESSAGE_APP":
                    try:
                        # Publish to chatrooms for real-time delivery to TAK clients
                        if to_id == "all":
                            self.rabbit_channel.basic_publish(exchange='chatrooms', routing_key='All Chat Rooms',
                                                              body=message,
                                                              properties=pika.BasicProperties(expiration=self.context.app.config.get("OTS_RABBITMQ_TTL")))
                        else:
                            for meshtastic_device in self.meshtastic_devices:
                                meshtastic_device = self.meshtastic_devices[meshtastic_device]
                                if meshtastic_device['meshtastic_id'] == to_id:
                                    self.rabbit_channel.basic_publish(exchange='dms',
                                                                      routing_key=meshtastic_device['uid'],
                                                                      body=message,
                                                                      properties=pika.BasicProperties(expiration=self.context.app.config.get("OTS_RABBITMQ_TTL")))
                        # ALSO publish to cot_controller so cot_parser inserts into geochat DB
                        self.rabbit_channel.basic_publish(exchange='cot_controller', routing_key='', body=message,
                                                          properties=pika.BasicProperties(expiration=self.context.app.config.get("OTS_RABBITMQ_TTL")))
                    except BaseException as e:
                        self.logger.error("Failed to publish chat message: {}".format(e))
                elif portnum == "ATAK_PLUGIN" and pb.HasField('chat'):
                    try:
                        to = unishox2.decompress(pb.chat.to, len(pb.chat.to))
                        if to in self.meshtastic_devices:
                            self.rabbit_channel.basic_publish(exchange='dms', routing_key=to, body=message)
                        else:
                            self.rabbit_channel.basic_publish(exchange='chatrooms',
                                                              routing_key=to,
                                                              body=message,
                                                              properties=pika.BasicProperties(expiration=self.context.app.config.get("OTS_RABBITMQ_TTL")))
                        # ALSO publish to cot_controller for DB persistence
                        self.rabbit_channel.basic_publish(exchange='cot_controller', routing_key='', body=message,
                                                          properties=pika.BasicProperties(expiration=self.context.app.config.get("OTS_RABBITMQ_TTL")))
                    except BaseException as e:
                        self.logger.error("Failed to publish chat message to {}: {}".format(
                            unishox2.decompress(pb.chat.to, len(pb.chat.to)), e))
                        self.logger.error(traceback.format_exc())
                else:
                    self.rabbit_channel.basic_publish(exchange='cot_controller', routing_key='', body=message,
                                                      properties=pika.BasicProperties(expiration=self.context.app.config.get("OTS_RABBITMQ_TTL")))
        except BaseException as e:
            self.logger.error(str(e))
            self.logger.error(traceback.format_exc())
