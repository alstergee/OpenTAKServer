import datetime
import hashlib
import json
import os
import platform
import traceback
from shutil import copyfile
from urllib.parse import urlparse
from xml.sax.saxutils import escape as xml_escape, quoteattr as xml_quoteattr

import bleach
import pika
import psutil
import sqlalchemy.exc
import yaml
from flask import Blueprint
from flask import current_app as app
from flask import jsonify, request, send_from_directory, session
from flask_babel import gettext
from flask_ldap3_login import AuthenticationResponseStatus
from flask_security import auth_required, current_user, verify_password
from sqlalchemy import select

from opentakserver import __version__ as version
from opentakserver.certificate_authority import CertificateAuthority
from opentakserver.extensions import babel, db, ldap_manager, logger
from opentakserver.models.Alert import Alert
from opentakserver.models.APSchedulerJobs import APSchedulerJobs
from opentakserver.models.CasEvac import CasEvac
from opentakserver.models.Certificate import Certificate
from opentakserver.models.Chatrooms import Chatroom
from opentakserver.models.CoT import CoT
from opentakserver.models.GeoChat import GeoChat
from opentakserver.models.DataPackage import DataPackage
from opentakserver.models.EUD import EUD
from opentakserver.models.Group import Group
from opentakserver.models.GroupUser import GroupUser
from opentakserver.models.Icon import Icon
from opentakserver.models.Marker import Marker
from opentakserver.models.Point import Point
from opentakserver.models.RBLine import RBLine
from opentakserver.models.Token import Token
from opentakserver.models.user import User
from opentakserver.models.ZMIST import ZMIST

api_blueprint = Blueprint("api_blueprint", __name__)

p = psutil.Process()


def search(query, model, field):
    arg = request.args.get(field)
    if arg:
        # bleach.clean() was removed here — bleach is for HTML sanitization,
        # NOT SQL/equality. SQLAlchemy parameterizes the `==` comparison so
        # there's no injection risk; the bleach call only mangled legit
        # values containing '<', '>', '&' (e.g. callsigns "TM<2>"). Audit
        # 2026-05-08 finding M-S4. Server-side rendering uses xml_escape
        # (CoT XML) and React (UI) which both escape on output already.
        return query.where(getattr(model, field) == arg)
    return query


def paginate(query: db.Query, model=None):
    try:
        page = int(request.args.get("page")) if "page" in request.args else 1
        per_page = int(request.args.get("per_page")) if "per_page" in request.args else 10
    except ValueError:
        return (
            {"success": False, "error": "Invalid page or per_page number"},
            400,
            {"Content-Type": "application/json"},
        )

    try:
        if model:
            sort_by = request.args.get("sort_by")
            sort_direction = request.args.get("sort_direction")
            # Whitelist sort_by to actual table columns. Was getattr(model, x)
            # for any user-supplied x — accepts relationships, dunder methods,
            # private attrs, etc. Limited risk (sort would just behave oddly
            # or error) but unbounded surface. Audit M-S5.
            valid_columns = {c.key for c in model.__table__.columns}
            if sort_by and sort_by not in valid_columns:
                return (
                    jsonify({
                        "success": False,
                        "error": gettext(
                            "Invalid sort column: %(sort_by)s", sort_by=sort_by
                        ),
                    }),
                    400,
                )
            if sort_by and (sort_direction == "asc" or not sort_direction):
                query = query.order_by(getattr(model, sort_by).asc())
            elif sort_by and sort_direction == "desc":
                query = query.order_by(getattr(model, sort_by).desc())
    except BaseException as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": gettext(
                        "Invalid sort column: %(sort_by)s", sort_by=request.args.get("sort_by")
                    ),
                }
            ),
            400,
        )

    pagination = db.paginate(query, page=page, per_page=per_page)
    rows = pagination.items

    results = {
        "results": [],
        "total_pages": pagination.pages,
        "current_page": page,
        "per_page": per_page,
        "total": 0,
    }

    for row in rows:
        mission = row.to_json()
        # Filter out duplicate results caused by missions belonging to multiple groups
        if mission not in results["results"]:
            results["results"].append(row.to_json())

    results["total"] = pagination.total

    return jsonify(results)


def change_config_setting(setting, value):
    try:
        with open(
            os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), "r"
        ) as config_file:
            config = yaml.safe_load(config_file.read())

        config[setting] = value
        with open(
            os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), "w"
        ) as config_file:
            yaml.safe_dump(config, config_file)

    except BaseException as e:
        logger.error(
            "Failed to change setting {} to {} in config.yml: {}".format(setting, value, e)
        )


def route_cot(event: str, user: User):
    """Used by API endpoints such as ``/api/markers`` to route CoT messages to the correct groups that a user belongs to.

    :param event: The CoT to be routed as a string
    :param user: The user object
    :return: None
    """

    rabbit_credentials = pika.PlainCredentials(
        app.config.get("OTS_RABBITMQ_USERNAME"), app.config.get("OTS_RABBITMQ_PASSWORD")
    )
    rabbit_host = app.config.get("OTS_RABBITMQ_SERVER_ADDRESS")
    rabbit_connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_host, credentials=rabbit_credentials)
    )
    channel = rabbit_connection.channel()

    group_memberships = db.session.execute(
        db.session.query(GroupUser).filter_by(user_id=user.id, direction=Group.IN, enabled=True)
    ).all()
    if not group_memberships:
        # Default to the __ANON__ group if the user doesn't belong to any IN groups
        channel.basic_publish(
            exchange="groups",
            routing_key="__ANON__.OUT",
            body=json.dumps({"uid": app.config["OTS_NODE_ID"], "cot": str(event)}),
            properties=pika.BasicProperties(expiration=app.config.get("OTS_RABBITMQ_TTL")),
        )

    for membership in group_memberships:
        membership = membership[0]
        channel.basic_publish(
            exchange="groups",
            routing_key=f"{membership.group.name}.{Group.OUT}",
            body=json.dumps({"uid": app.config["OTS_NODE_ID"], "cot": str(event)}),
            properties=pika.BasicProperties(expiration=app.config.get("OTS_RABBITMQ_TTL")),
        )
    channel.close()
    rabbit_connection.close()


@api_blueprint.route("/files/api/config")
def cloudtak_config():
    """Required by CloudTAK, returns the following

    .. code-block:: json

        {"uploadSizeLimit": 400}
    """
    return jsonify({"uploadSizeLimit": 400})


# Simple health check for docker
@api_blueprint.route("/api/health")
def health():
    """Health check for Docker, returns the following

    .. code-block:: json
        {"status": "healthy"}
    """
    return jsonify({"status": "healthy"})


@api_blueprint.route("/api/status")
@auth_required()
def status():
    """Server status used on the Dashboard page of the web UI

    :rtype: dict
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    system_boot_time = datetime.datetime.fromtimestamp(
        psutil.boot_time(), datetime.datetime.now().astimezone().tzinfo
    )
    system_uptime = now - system_boot_time

    ots_uptime = now - app.start_time

    cpu_time = psutil.cpu_times()
    cpu_time_dict = {"user": cpu_time.user, "system": cpu_time.system, "idle": cpu_time.idle}

    vmem = psutil.virtual_memory()
    vmem_dict = {
        "total": vmem.total,
        "available": vmem.available,
        "used": vmem.used,
        "free": vmem.free,
        "percent": vmem.percent,
    }

    disk_usage = psutil.disk_usage("/")
    disk_usage_dict = {
        "total": disk_usage.total,
        "used": disk_usage.used,
        "free": disk_usage.free,
        "percent": disk_usage.percent,
    }

    try:
        os_release = platform.freedesktop_os_release()
    except:
        os_release = {"NAME": None, "PRETTY_NAME": None, "VERSION": None, "VERSION_CODENAME": None}

    uname = {
        "system": platform.system(),
        "node": platform.node(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
    }

    online_euds = db.session.execute(select(EUD).filter(EUD.last_status == "Connected")).all()

    response = {
        "online_euds": len(online_euds),
        "system_boot_time": system_boot_time.strftime("%Y-%m-%d %H:%M:%SZ"),
        "system_uptime": system_uptime.total_seconds(),
        "ots_start_time": app.start_time.strftime("%Y-%m-%d %H:%M:%SZ"),
        "ots_uptime": ots_uptime.total_seconds(),
        "cpu_time": cpu_time_dict,
        "cpu_percent": p.cpu_percent(),
        "load_avg": psutil.getloadavg(),
        "memory": vmem_dict,
        "disk_usage": disk_usage_dict,
        "ots_version": version,
        "uname": uname,
        "os_release": os_release,
        "python_version": platform.python_version(),
    }

    return jsonify(response)


@api_blueprint.route("/api/certificate", methods=["GET", "POST"])
@auth_required()
def certificate():
    if request.method == "POST" and "username" in request.json.keys():
        try:
            username = bleach.clean(request.json.get("username"))
            truststore_filename = os.path.join(
                app.config.get("OTS_CA_FOLDER"), "certs", "opentakserver", "truststore-root.p12"
            )
            user_filename = os.path.join(
                app.config.get("OTS_CA_FOLDER"), "certs", username, "{}.p12".format(username)
            )

            user = app.security.datastore.find_user(username=username)

            if not user:
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": gettext("Invalid username: %(username)s", username=username),
                        }
                    ),
                    400,
                )

            ca = CertificateAuthority(logger, app)
            filenames = ca.issue_certificate(username, False)

            for filename in filenames:
                file_hash = hashlib.sha256(
                    open(
                        os.path.join(app.config.get("OTS_CA_FOLDER"), "certs", username, filename),
                        "rb",
                    ).read()
                ).hexdigest()

                data_package = DataPackage()
                data_package.filename = filename
                data_package.keywords = "public"
                data_package.creator_uid = (
                    request.json["uid"] if "uid" in request.json.keys() else None
                )
                data_package.submission_time = datetime.datetime.now(datetime.timezone.utc)
                data_package.mime_type = "application/x-zip-compressed"
                data_package.size = os.path.getsize(
                    os.path.join(app.config.get("OTS_CA_FOLDER"), "certs", username, filename)
                )
                data_package.hash = file_hash
                data_package.submission_user = current_user.id

                try:
                    db.session.add(data_package)
                    db.session.commit()
                except sqlalchemy.exc.IntegrityError as e:
                    db.session.rollback()
                    logger.error(e)
                    return (
                        jsonify(
                            {
                                "success": False,
                                "error": gettext(
                                    "Certificate already exists for %(username)s", username=username
                                ),
                            }
                        ),
                        400,
                    )

                copyfile(
                    os.path.join(
                        app.config.get("OTS_CA_FOLDER"), "certs", username, "{}".format(filename)
                    ),
                    os.path.join(app.config.get("UPLOAD_FOLDER"), "{}.zip".format(file_hash)),
                )

                cert = Certificate()
                cert.common_name = username
                cert.username = username
                cert.expiration_date = datetime.datetime.today() + datetime.timedelta(
                    days=app.config.get("OTS_CA_EXPIRATION_TIME")
                )
                cert.server_address = urlparse(request.url_root).hostname
                cert.server_port = app.config.get("OTS_SSL_STREAMING_PORT")
                cert.truststore_filename = truststore_filename
                cert.user_cert_filename = user_filename
                cert.cert_password = app.config.get("OTS_CA_PASSWORD")
                cert.data_package_id = data_package.id if data_package else None

                db.session.add(cert)
                db.session.commit()

            return jsonify({"success": True}), 200
        except BaseException as e:
            logger.error(traceback.format_exc())
            return jsonify({"success": False, "error": str(e)}), 500
    elif request.method == "POST":
        return jsonify({"success": False, "error": gettext("Please specify a callsign")}), 400
    elif request.method == "GET":
        query = db.session.query(Certificate)
        query = search(query, Certificate, "callsign")
        query = search(query, Certificate, "username")

        return paginate(query)


@api_blueprint.route("/api/me")
@auth_required()
def me():
    """Get the details of the currently logged in user

    :rtype: User
    """
    return jsonify(current_user.to_json())


@api_blueprint.route("/api/cot", methods=["GET"])
@auth_required()
def query_cot():
    """Get CoT messages. All parameters are optional.

    :param how: The how attribute of the CoT message, i.e. ``m-g``, ``h-g-i-g-o``
    :param type: The type attribute of the CoT message, i.e. ``a-f-G-U-C``
    :param sender_callsign: The callsign of the EUD that sent the CoT message
    :param sender_uid: The UID of the EUD that sent the CoT message
    :param page: The page number
    :param per_page: The number of results per page
    """
    query = db.session.query(CoT)
    query = search(query, CoT, "how")
    query = search(query, CoT, "type")
    query = search(query, CoT, "sender_callsign")
    query = search(query, CoT, "sender_uid")

    return paginate(query, CoT)


@api_blueprint.route("/api/alerts", methods=["GET"])
@auth_required()
def query_alerts():
    """Get alerts. All parameters are optional.

    :param uid: The alert's UID
    :param sender_uid: The UID of the EUD that sent the alert
    :param alert_type: The type of alert
    :param page: The page number
    :param per_page: The number of results per page
    """
    query = db.session.query(Alert)
    query = search(query, Alert, "uid")
    query = search(query, Alert, "sender_uid")
    query = search(query, Alert, "alert_type")

    return paginate(query, Alert)


@api_blueprint.route("/api/point", methods=["GET"])
@auth_required()
def query_points():
    """Query points. All parameters are optional.

    :param uid: The point's UID
    :param callsign: The point's callsign
    :param page: The page number
    :param per_page: The number of results per page
    """
    query = db.session.query(Point)

    query = search(query, EUD, "uid")
    query = search(query, EUD, "callsign")

    return paginate(query, Point)


@api_blueprint.route("/api/rabbitmq/<path>", methods=["POST"])
def rabbitmq_auth(path):
    # https://github.com/rabbitmq/rabbitmq-server/tree/v3.13.x/deps/rabbitmq_auth_backend_http

    # Only allow requests to this route from the RabbitMQ server. The original
    # check compared `request.remote_addr` (always an IP) against
    # OTS_RABBITMQ_SERVER_ADDRESS (typically a hostname like 'rabbitmq') —
    # the comparison NEVER matched, so either every request was denied (auth
    # broken) or, if config was set to an IP, ANY container on that IP could
    # submit credentials. Audit 2026-05-08 finding C3 (IP/host comparison).
    # Resolve the configured host to its IP set at request time and compare
    # against that. socket.gethostbyname_ex returns (canonical, aliases, ips).
    import socket as _socket
    rabbit_host = app.config.get("OTS_RABBITMQ_SERVER_ADDRESS", "")
    allowed_ips = set()
    try:
        # Direct IP literal also works (gethostbyname_ex echoes it back)
        allowed_ips.update(_socket.gethostbyname_ex(rabbit_host)[2])
    except Exception:
        # Fall back to a plain string compare for environments where DNS is
        # unavailable but config IS already an IP literal.
        allowed_ips.add(rabbit_host)
    if request.remote_addr not in allowed_ips:
        return "deny", 200

    username = bleach.clean(request.form.get("username"))
    password = None
    if "password" in request.form.keys():
        password = bleach.clean(request.form.get("password"))

    if app.config.get("OTS_ENABLE_LDAP"):
        result = ldap_manager.authenticate(username, password)

        if result.status == AuthenticationResponseStatus.success:
            # Keep this import here to avoid a circular import when OTS is started
            from opentakserver.blueprints.ots_api.ldap_api import save_user

            save_user(result.user_dn, result.user_id, result.user_info, result.user_groups)

            for group in result.user_groups:
                if group["cn"] == app.config.get("OTS_LDAP_ADMIN_GROUP"):
                    return "allow administrator", 200

            return "allow", 200
        else:
            return "deny", 200

    user = None
    if "username" in request.form.keys():
        user = app.security.datastore.find_user(username=username)

    if user and "password" in request.form.keys():
        if user.active and verify_password(password, user.password):
            if user.has_role("administrator"):
                return "allow administrator", 200
            return "allow", 200
        else:
            return "deny", 200
    # Always allow when path is topic, resource, or vhost if the user exists.
    # This only occurs after the user has been successfully authenticated
    elif user:
        return "allow", 200
    else:
        return "deny", 200


@api_blueprint.route("/api/eud")
@auth_required()
def get_euds():
    """Query EUDS. All parameters are optional.

    :param callsign: The EUD's callsign
    :param uid: The EUD's callsign
    :param username: The username that the EUD belongs to
    :param page: The page number
    :param per_page: The number of results per page
    """

    if request.args.get("all"):
        all_euds = []
        euds = EUD.query.with_entities(EUD.uid, EUD.callsign).all()
        for eud in euds:
            if eud[0] and eud[1]:
                all_euds.append({"uid": eud[0], "callsign": eud[1]})
        return jsonify(all_euds)

    query = db.session.query(EUD)

    if "username" in request.args.keys():
        query = query.join(User, User.id == EUD.user_id)

    query = search(query, EUD, "callsign")
    query = search(query, EUD, "uid")
    query = search(query, User, "username")

    return paginate(query, EUD)


@api_blueprint.route("/api/eud/<uid>", methods=["DELETE"])
@auth_required()
def delete_eud(uid):
    """Delete an EUD and let SQLAlchemy cascade through points/CoT/etc."""
    if not current_user.has_role("administrator"):
        return jsonify({"success": False, "error": "Administrator role required"}), 403
    eud = db.session.query(EUD).filter_by(uid=uid).first()
    if not eud:
        return jsonify({"success": False, "error": f"EUD not found: {uid}"}), 404
    try:
        callsign = eud.callsign
        db.session.delete(eud)
        db.session.commit()
        logger.info(f"Deleted EUD {uid} ({callsign}) by {current_user.username}")
        return jsonify({"success": True, "uid": uid, "callsign": callsign})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to delete EUD {uid}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# Editable fields surfaced in the dashboard's EUDs page edit dialog. uid is
# the device's stable identity — changing it cascades through points, CoT,
# certs, chat, mesh, mission contents — so we deliberately do NOT accept it
# here. user_id (assign-to-user) has its own purpose-built endpoint at
# /api/user/assign_eud, with the user-search UX that change implies.
_EUD_EDITABLE_FIELDS = {
    "callsign",
    "device",
    "platform",
    "os",
    "version",
    "phone_number",
    "team_role",
}


@api_blueprint.route("/api/eud/<uid>", methods=["PATCH"])
@auth_required()
def update_eud(uid):
    """Update an EUD's editable metadata. Administrator only.

    Accepts a JSON body with any subset of _EUD_EDITABLE_FIELDS. Empty
    strings are coerced to NULL so admins can clear a field. Unknown keys
    are silently ignored — the UI may post the full record back.
    """
    if not current_user.has_role("administrator"):
        return jsonify({"success": False, "error": "Administrator role required"}), 403
    eud = db.session.query(EUD).filter_by(uid=uid).first()
    if not eud:
        return jsonify({"success": False, "error": f"EUD not found: {uid}"}), 404

    data = request.get_json(silent=True) or {}
    changed = []
    try:
        for k, v in data.items():
            if k not in _EUD_EDITABLE_FIELDS:
                continue
            if k == "phone_number":
                if v in (None, "", 0, "0"):
                    v = None
                else:
                    try:
                        v = int(str(v).strip())
                    except (TypeError, ValueError):
                        return jsonify({
                            "success": False,
                            "error": "phone_number must be numeric",
                        }), 400
            elif isinstance(v, str) and v.strip() == "":
                v = None
            setattr(eud, k, v)
            changed.append(k)

        if not changed:
            return jsonify({"success": True, "uid": uid, "callsign": eud.callsign, "changed": []})

        db.session.commit()
        logger.info(
            f"Updated EUD {uid} ({eud.callsign}) by {current_user.username}: {changed}"
        )
        return jsonify({
            "success": True,
            "uid": uid,
            "callsign": eud.callsign,
            "changed": changed,
        })
    except sqlalchemy.exc.IntegrityError as e:
        db.session.rollback()
        msg = str(getattr(e, "orig", e)).lower()
        if "callsign" in msg:
            return jsonify({"success": False, "error": "Callsign already in use"}), 409
        return jsonify({"success": False, "error": "Database constraint violation"}), 409
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to update EUD {uid}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@api_blueprint.route("/api/truststore")
@auth_required()
def get_truststore():
    """Downloads the server's truststore — gated to authenticated users.
    Was public; the audit (2026-05-08, finding C1) flagged this as a CA-bundle
    information leak. ATAK clients fetching this for enrollment have a session
    cookie by the time they reach this endpoint, so authed access is fine."""
    filename = f"truststore_root_{urlparse(request.url_root).hostname}.p12"
    return send_from_directory(
        app.config.get("OTS_CA_FOLDER"),
        "truststore-root.p12",
        download_name=filename,
        as_attachment=True,
    )


@api_blueprint.route("/api/map_state")
@auth_required()
def get_map_state():
    """Gets the latest data to be displayed on the web UI's map.

    Audit 2026-05-08 finding H-B5: was loading ALL EUDs (no recency filter) on
    every poll, then triggering lazy-load per row in `to_json()` — linear
    growth with EUD history; the polling map page bricks once you've ever had
    a few hundred EUDs through. Now:
      * filter EUDs by `last_event_time >= now - OTS_MAP_STATE_EUD_HOURS`
        (default 24h; configurable so historical-recall pages can override)
      * everything else already filters on CoT.stale, that part is fine
      * compute `now` once instead of per-query (was 4 calls), small win
    """
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        eud_recency_hours = app.config.get("OTS_MAP_STATE_EUD_HOURS", 24)
        eud_cutoff = now - datetime.timedelta(hours=eud_recency_hours)

        results = {"euds": [], "markers": [], "rb_lines": [], "casevacs": []}

        euds = db.session.execute(
            db.session.query(EUD).filter(EUD.last_event_time >= eud_cutoff)
        ).all()
        for eud in euds:
            results["euds"].append(eud[0].to_json())

        markers = db.session.execute(
            db.session.query(Marker).join(CoT).filter(CoT.stale >= now)
        ).all()
        for marker in markers:
            results["markers"].append(marker[0].to_json())

        rb_lines = db.session.execute(
            db.session.query(RBLine).join(CoT).filter(CoT.stale >= now)
        ).all()
        for rb_line in rb_lines:
            results["rb_lines"].append(rb_line[0].to_json())

        casevacs = db.session.execute(
            db.session.query(CasEvac).join(CoT).filter(CoT.stale >= now)
        ).all()
        for casevac in casevacs:
            results["casevacs"].append(casevac[0].to_json())

    except BaseException as e:
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

    return jsonify(results)


@api_blueprint.route("/api/icon")
@auth_required()
def get_icon():
    """Query map icons. All parameters are optional.

    :param filename:
    :param groupName:
    :param type2525b:
    """
    query = db.session.query(Icon)
    query = search(query, Icon, "filename")
    query = search(query, Icon, "groupName")
    query = search(query, Icon, "type2525b")

    return paginate(query, Icon)


@api_blueprint.route("/api/itak_qr_string")
@auth_required()
def get_settings():
    """The iTAK QR string in the following format:

    ``OpenTAKServer_SERVER-ADDRESS,SERVER-ADDRESS,8089,SSL``
    """
    url = urlparse(request.url_root).hostname
    return "OpenTAKServer_{},{},{},SSL".format(url, url, app.config.get("OTS_SSL_STREAMING_PORT"))
@api_blueprint.route('/api/chatrooms', methods=['GET'])
@auth_required()
def get_chatrooms():
    """List all chatrooms/channels the server knows about with their message
    counts. Single GROUP BY query — was N+1 (one COUNT(*) per chatroom in a
    Python for-loop). Audit 2026-05-08 finding H-B6."""
    try:
        from sqlalchemy import func
        rows = db.session.query(
            Chatroom,
            func.count(GeoChat.uid).label('message_count')
        ).outerjoin(
            GeoChat, GeoChat.chatroom_id == Chatroom.id
        ).group_by(Chatroom.id).all()

        results = []
        for chatroom, message_count in rows:
            data = chatroom.to_json()
            data['message_count'] = message_count
            results.append(data)

        return jsonify({'success': True, 'chatrooms': results})
    except BaseException as e:
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@api_blueprint.route('/api/geochat', methods=['GET'])
@auth_required()
def get_geochat():
    """
    Get chat messages with optional filters.
    Query params:
      - chatroom_id: filter by chatroom (e.g., "All Chat Rooms")
      - sender_uid: filter by sender
      - limit: max messages to return (default 100)
      - offset: pagination offset
    """
    try:
        chatroom_id = request.args.get('chatroom_id')
        sender_uid = request.args.get('sender_uid')
        limit = min(int(request.args.get('limit', 100)), 500)
        offset = int(request.args.get('offset', 0))

        query = db.session.query(GeoChat).join(
            EUD, GeoChat.sender_uid == EUD.uid
        ).add_columns(
            EUD.callsign.label('sender_callsign')
        ).order_by(GeoChat.timestamp.desc())

        if chatroom_id:
            query = query.filter(GeoChat.chatroom_id == chatroom_id)  # bleach removed — wrong tool, M-S4
        if sender_uid:
            query = query.filter(GeoChat.sender_uid == sender_uid)  # bleach removed — M-S4

        total = query.count()
        results = query.offset(offset).limit(limit).all()

        messages = []
        for geochat, sender_callsign in results:
            msg = {
                'uid': geochat.uid,
                'chatroom_id': geochat.chatroom_id,
                'sender_uid': geochat.sender_uid,
                'sender_callsign': sender_callsign,
                'remarks': geochat.remarks,
                'timestamp': geochat.timestamp.isoformat() if geochat.timestamp else None,
            }
            messages.append(msg)

        # Reverse so oldest first (for chat display)
        messages.reverse()

        return jsonify({
            'success': True,
            'messages': messages,
            'total': total,
            'limit': limit,
            'offset': offset
        })
    except BaseException as e:
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@api_blueprint.route('/api/geochat/send', methods=['POST'])
@auth_required()
def send_geochat():
    """
    Send a message to a chatroom/channel.
    Inserts directly into DB (bypasses cot_parser which can't handle web-originated messages).
    Also publishes to RabbitMQ for real-time TAK client delivery + Meshtastic bridge.
    """
    try:
        import pika
        import uuid
        from opentakserver.models.GeoChat import GeoChat
        from opentakserver.models.Chatrooms import Chatroom
        from opentakserver.models.CoT import CoT
        from opentakserver.models.Point import Point
        from opentakserver.models.EUD import EUD

        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'No JSON body provided'}), 400

        chatroom_id = data.get('chatroom_id', 'All Chat Rooms')
        message = data.get('message', '')
        to_meshtastic = data.get('to_meshtastic', True)
        # broadcast_channels: list of mesh channel names to fan out the SAME
        # message to. Used by the ALL tab in MeshChat. We still save ONE DB
        # row (with chatroom_id = "ALL"), but publish one mesh packet per
        # listed channel so radios on each channel pick it up.
        broadcast_channels = data.get('broadcast_channels') or []

        if not message:
            return jsonify({'success': False, 'error': 'Message cannot be empty'}), 400

        message = bleach.clean(message)
        chatroom_id = bleach.clean(chatroom_id)

        sender_uid = f"WebUI-{current_user.username}"
        sender_callsign = current_user.username
        now = datetime.datetime.now(datetime.timezone.utc)
        msg_uid = f"GeoChat.{sender_uid}.{chatroom_id}.{uuid.uuid4()}"

        # Ensure sender EUD exists
        eud = db.session.query(EUD).filter_by(uid=sender_uid).first()
        if not eud:
            eud = EUD()
            eud.uid = sender_uid
            eud.callsign = sender_callsign
            eud.device = 'WebUI'
            eud.os = 'WebUI'
            eud.platform = 'WebUI'
            eud.version = '1.0'
            eud.last_event_time = now
            eud.last_status = 'Online'
            db.session.add(eud)
            db.session.flush()

        # Ensure chatroom exists
        chatroom = db.session.query(Chatroom).filter_by(id=chatroom_id).first()
        if not chatroom:
            chatroom = Chatroom()
            chatroom.id = chatroom_id
            chatroom.name = chatroom_id
            chatroom.parent = 'RootContactGroup'
            db.session.add(chatroom)
            db.session.flush()

        # Insert CoT record
        time_str = now.strftime('%Y-%m-%dT%H:%M:%SZ')
        stale_str = (now + datetime.timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
        safe_message = xml_escape(message)
        safe_chatroom = xml_escape(chatroom_id)
        safe_sender_uid = xml_escape(sender_uid)
        safe_callsign = xml_escape(sender_callsign)
        safe_msg_uid = xml_escape(msg_uid)

        cot_xml = f'<event how="h-g-i-g-o" stale="{stale_str}" start="{time_str}" time="{time_str}" type="b-t-f" uid="{safe_msg_uid}" version="2.0"><point ce="9999999" hae="0" lat="0" le="9999999" lon="0"/><detail><__chat chatroom="{safe_chatroom}" groupOwner="false" id="{safe_chatroom}" senderCallsign="{safe_callsign}"><chatgrp id="{safe_chatroom}" uid0="{safe_sender_uid}" uid1="{safe_chatroom}"/></__chat><link relation="p-p" type="a-f-G-U-C" uid="{safe_sender_uid}"/><remarks source="{safe_sender_uid}" time="{time_str}" to="{safe_chatroom}">{safe_message}</remarks></detail></event>'

        cot = CoT()
        cot.uid = msg_uid
        cot.type = 'b-t-f'
        cot.how = 'h-g-i-g-o'
        cot.sender_uid = sender_uid
        cot.sender_callsign = sender_callsign
        cot.timestamp = now
        cot.start = now
        cot.stale = now + datetime.timedelta(minutes=5)
        cot.xml = cot_xml
        db.session.add(cot)
        db.session.flush()

        # Insert Point record (required FK, use 0,0 for web messages)
        point = Point()
        point.uid = sender_uid
        point.device_uid = sender_uid
        point.ce = 9999999
        point.le = 9999999
        point.hae = 0
        point.latitude = 0
        point.longitude = 0
        point.timestamp = now
        db.session.add(point)
        db.session.flush()

        # Insert GeoChat record
        geochat = GeoChat()
        geochat.uid = msg_uid
        geochat.chatroom_id = chatroom_id
        geochat.sender_uid = sender_uid
        geochat.remarks = message
        geochat.timestamp = now
        geochat.point_id = point.id
        geochat.cot_id = cot.id
        db.session.add(geochat)
        db.session.commit()

        logger.info(f"Chat message saved: [{chatroom_id}] {sender_callsign}: {message[:50]}")

        # Publish to RabbitMQ for real-time delivery to TAK clients via the
        # singleton AMQP publisher (was a per-call BlockingConnection — chat
        # send took ~80ms in connection overhead alone). Audit H-B4.
        try:
            import json as _json
            from opentakserver.amqp_publisher import publish as _amqp_publish
            wrapped = _json.dumps({'uid': sender_uid, 'cot': cot_xml})
            _amqp_publish(exchange='chatrooms', routing_key=chatroom_id, body=wrapped)
        except Exception as e:
            logger.error(f"RabbitMQ publish failed (message saved to DB): {e}")

        # If sending to Meshtastic, also publish to mesh channels.
        # In broadcast mode, mesh_targets is the list from the request;
        # otherwise it's just [the chatroom's matching mesh channel].
        if to_meshtastic and app.config.get('OTS_ENABLE_MESHTASTIC', False):
            try:
                from meshtastic.protobuf import mesh_pb2, portnums_pb2, mqtt_pb2
                import base64

                mesh_topic = app.config.get('OTS_MESHTASTIC_TOPIC', 'msh')
                # Map common chatroom names to Meshtastic channel names
                channel_map = {
                    'All Chat Rooms': 'ALLCALL',
                    'ALL': 'ALLCALL',  # ALL is the synthetic broadcast tab
                    'ALLCALL': 'ALLCALL',
                    'SECURITY': 'SECURITY',
                    'PRODUCTION': 'PRODUCTION',
                    'STAFF': 'STAFF',
                }
                # If broadcast_channels was passed, fan out to all of them.
                # Otherwise just publish to the chatroom's matching mesh channel.
                mesh_targets = broadcast_channels if broadcast_channels else [
                    channel_map.get(chatroom_id, 'ALLCALL')
                ]

                # Build Meshtastic protobuf for TEXT_MESSAGE_APP (shared across
                # all target channels — the per-channel Data is identical, only
                # the MeshPacket.channel index and ServiceEnvelope.channel_id
                # change per publish below).
                pb_data = mesh_pb2.Data()
                pb_data.portnum = portnums_pb2.TEXT_MESSAGE_APP
                pb_data.payload = message.encode('utf-8')

                # Channel name -> index map (matches the gateway's channel order:
                # 0=PRIMARY, 1=ALLCALL, 2=SECURITY, 3=PRODUCTION, 4=STAFF, 5=PKI).
                # This must agree with the order the chip's USERPREFS bake them in.
                channel_index_map = {
                    'ALLCALL': 1, 'SECURITY': 2, 'PRODUCTION': 3, 'STAFF': 4, 'PKI': 5,
                }

                # The 5 silent-drop gates from Meshtastic firmware MQTT.cpp
                # onReceiveProto (lines 66-156) — any one fails → silent drop, no log:
                #
                #   (1) Topic root must match chip's mqtt.root → OTS_MESHTASTIC_TOPIC.
                #   (2) channel.downlink_enabled = true on the chip (chip-side flag).
                #   (3) gateway_id MUST NOT equal chip's own node id (else "ignore
                #       downlink we sent"). Use a fixed synthetic id, NEVER hash a
                #       username because hashes can collide with real chip ids.
                #   (4) packet.from MUST NOT equal chip's nodenum (isFromUs check).
                #       Use the FE000000 reserved range with the low 24 bits derived
                #       from the ATAK user — stable per-user, but firmly outside any
                #       real chip's id space.
                #   (5) hop_start AND hop_limit both >0 and <=7. hop_start=0 = drop
                #       before any hop logic runs.

                # packet.from in the FE000000/8 reserved range (gate 4) — stable
                # per ATAK user, never collides with a real chip id.
                import hashlib as _hl
                import random as _rnd
                user_low24 = int(_hl.sha256(current_user.username.encode()).hexdigest()[:6], 16) & 0x00FFFFFF
                from_id = 0xFE000000 | user_low24

                # Use the singleton AMQP publisher — was a per-call BlockingConnection
                # opened just for the mesh fan-out. Audit H-B4.
                from opentakserver.amqp_publisher import publish as _amqp_publish

                for target_channel in mesh_targets:
                    ch_idx = channel_index_map.get(target_channel, 1)
                    # Each publish gets a fresh random packet id — firmware drops id=0.
                    pkt_id = _rnd.randint(1, 0xFFFFFFFF)

                    mesh_packet = mesh_pb2.MeshPacket()
                    mesh_packet.decoded.CopyFrom(pb_data)
                    mesh_packet.to = 0xFFFFFFFF  # Broadcast
                    mesh_packet.want_ack = False
                    mesh_packet.id = pkt_id
                    setattr(mesh_packet, 'from', from_id)  # 'from' is a Python reserved word
                    mesh_packet.channel = ch_idx
                    # Gate 5: BOTH hop_start and hop_limit must be set, non-zero, ≤7.
                    mesh_packet.hop_start = 7
                    mesh_packet.hop_limit = 7

                    service_envelope = mqtt_pb2.ServiceEnvelope()
                    service_envelope.packet.CopyFrom(mesh_packet)
                    service_envelope.channel_id = target_channel
                    # Gate 3: synthetic gateway_id, NEVER any real chip's id.
                    service_envelope.gateway_id = "!fffe0001"

                    routing_key = f"{mesh_topic}.2.e.{target_channel}.outgoing"
                    _amqp_publish(
                        exchange='amq.topic',
                        routing_key=routing_key,
                        body=service_envelope.SerializeToString(),
                    )
                    logger.info(f"Published text message to Meshtastic: {routing_key}")
            except ImportError:
                logger.warning("Meshtastic protobuf library not available - message not sent to mesh")
            except Exception as e:
                logger.warning(f"Failed to publish to Meshtastic: {e}")

        return jsonify({
            'success': True,
            'message': 'Message sent',
            'uid': msg_uid,
            'chatroom_id': chatroom_id
        })

    except BaseException as e:
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@api_blueprint.route('/api/gateway/health', methods=['GET'])
@auth_required()
def get_gateway_health():
    """Gateway health from monitor cron — authed only. Was public; the audit
    (2026-05-08, finding C2) flagged the response payload (container fleet,
    MQTT client count, last-chat timestamp) as fingerprinting/scheduling info
    a public attacker shouldn't have. The dashboard's chat-inject panel calls
    this with credentials anyway, so gating is transparent."""
    import sys, traceback as _tb
    try:
        stats_path = "/app/ots/gateway-stats.json"
        if os.path.exists(stats_path):
            with open(stats_path) as f:
                data = json.load(f)
            return jsonify(data)
        return jsonify({"connected": False, "error": "No stats yet — monitor cron may not have run"}), 200
    except Exception as e:
        sys.stderr.write(f"\n*** get_gateway_health EXC: {e}\n{_tb.format_exc()}\n***\n")
        sys.stderr.flush()
        return jsonify({"connected": False, "error": f"{type(e).__name__}: {e}"}), 500


@api_blueprint.route('/api/gateway/log', methods=['GET'])
@auth_required()
def get_gateway_log():
    """Last N lines of gateway health log."""
    try:
        lines = int(request.args.get('lines', 50))
        log_path = "/app/ots/gateway-health.log"
        if os.path.exists(log_path):
            with open(log_path) as f:
                all_lines = f.readlines()
            return jsonify({"lines": [l.strip() for l in all_lines[-lines:]]})
        return jsonify({"lines": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_blueprint.route('/api/meshtastic/channels', methods=['GET'])
@auth_required()
def get_meshtastic_channels():
    """Get configured Meshtastic channels from config."""
    try:
        config_path = os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f.read())

        channels = config.get('OTS_MESHTASTIC_DOWNLINK_CHANNELS', [])
        topic = config.get('OTS_MESHTASTIC_TOPIC', 'msh')
        enabled = config.get('OTS_ENABLE_MESHTASTIC', False)

        # Default channels if none configured
        if not channels:
            channels = ['ALLCALL', 'SECURITY', 'PRODUCTION', 'STAFF']

        return jsonify({
            'success': True,
            'enabled': enabled,
            'topic': topic,
            'channels': channels
        })
    except BaseException as e:
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500
