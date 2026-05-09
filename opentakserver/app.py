from gevent import monkey

monkey.patch_all()

import logging
import os
import platform
import sqlite3
import sys
import traceback
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

import colorlog
import flask_wtf
import pika
import pytz
import requests
import sqlalchemy

# Monkey-patch the `requests` library with a default timeout. Audit
# 2026-05-08 finding H-B7 / H8: every requests.* call in the codebase (15
# sites: app.py, scheduled_jobs.py, mediamtx_api.py) was made with NO
# `timeout=` argument, so a hung upstream (airplanes.live, AISHub, mediamtx)
# would block the apscheduler worker forever. (5s connect, 30s read) is
# conservative and lets each call site override locally if needed. Done at
# import time so it covers every later import of the requests module.
def _ots_default_request_timeout(orig_request):
    def wrapper(method, url, **kwargs):
        kwargs.setdefault("timeout", (5, 30))
        return orig_request(method, url, **kwargs)
    return wrapper
requests.api.request = _ots_default_request_timeout(requests.api.request)
# `requests.get` / `requests.post` etc. all funnel through requests.api.request
# so patching at that point covers every wrapper without touching them.
import yaml
from flask import Flask, current_app, g, request, session
from flask_cors import CORS
from flask_migrate import Migrate, upgrade
from flask_security import (
    Security,
    SQLAlchemyUserDatastore,
    hash_password,
    uia_email_mapper,
    uia_username_mapper,
)
from flask_security.models import fsqla_v3
from flask_security.models import fsqla_v3 as fsqla
from flask_security.signals import user_registered
from sqlalchemy import insert
from werkzeug.middleware.proxy_fix import ProxyFix

import opentakserver
from opentakserver.certificate_authority import CertificateAuthority
from opentakserver.controllers.meshtastic_controller import MeshtasticController
from opentakserver.defaultconfig import DefaultConfig
from opentakserver.EmailValidator import EmailValidator
from opentakserver.extensions import apscheduler, babel, db, ldap_manager, logger, mail, socketio
from opentakserver.models.Group import Group, GroupTypeEnum
from opentakserver.models.Icon import Icon
from opentakserver.models.role import Role
from opentakserver.models.WebAuthn import WebAuthn
from opentakserver.PasswordValidator import PasswordValidator
from opentakserver.plugins.Plugin import Plugin
from opentakserver.plugins.PluginManager import PluginManager
from opentakserver.sql_jobstore import SQLJobStore
from opentakserver.UsernameValidator import UsernameValidator

try:
    from opentakserver.mumble.mumble_ice_app import MumbleIceDaemon
except ModuleNotFoundError:
    print("Mumble auth not supported on this platform")


def get_locale():
    if "language" in session:
        return session["language"]
    return request.accept_languages.best_match(current_app.config.get("OTS_LANGUAGES").keys())


def get_timezone():
    # Always return UTC and let the frontend handle converting timezones
    return pytz.timezone("UTC")


def init_extensions(app):
    db.init_app(app)
    Migrate(app, db)

    logger.info(f"OpenTAKServer {opentakserver.__version__}")
    logger.info("Loading the database...")
    with app.app_context():
        upgrade(
            directory=os.path.join(
                os.path.dirname(os.path.realpath(opentakserver.__file__)), "migrations"
            )
        )
        # Flask-Migrate does weird things to the logger
        logger.disabled = False
        logger.parent.handlers.pop()
        if app.config.get("DEBUG"):
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    # Handle config options that can't be serialized to yaml
    app.config.update(
        {
            "SCHEDULER_JOBSTORES": {
                "default": SQLJobStore(url=app.config.get("SQLALCHEMY_DATABASE_URI"))
            }
        }
    )
    identity_attributes = [{"username": {"mapper": uia_username_mapper, "case_insensitive": True}}]

    # Don't allow registration unless email is enabled
    if app.config.get("OTS_ENABLE_EMAIL"):
        identity_attributes.append(
            {"email": {"mapper": uia_email_mapper, "case_insensitive": True}}
        )
        app.config.update(
            {
                "SECURITY_REGISTERABLE": True,
                "SECURITY_CONFIRMABLE": True,
                "SECURITY_RECOVERABLE": True,
                "SECURITY_TWO_FACTOR_ENABLED_METHODS": ["authenticator", "email"],
            }
        )
    else:
        app.config.update(
            {
                "SECURITY_REGISTERABLE": False,
                "SECURITY_CONFIRMABLE": False,
                "SECURITY_RECOVERABLE": False,
                "SECURITY_TWO_FACTOR_ENABLED_METHODS": ["authenticator"],
            }
        )

    if app.config.get("OTS_ENABLE_LDAP"):
        logger.info("Enabling LDAP")
        ldap_manager.init_app(app)
        identity_attributes.append({"ldap": {}})

    app.config.update({"SECURITY_USER_IDENTITY_ATTRIBUTES": identity_attributes})

    ca = CertificateAuthority(logger, app)
    ca.create_ca()

    # CORS allow-list. Was `origins="*"` for /api, /Marti, AND /* with
    # supports_credentials=True — Flask-CORS reflects the request Origin in
    # that combo, so any malicious site visited by a logged-in admin could
    # perform credentialed cross-site requests against this API. Audit
    # 2026-05-08 finding C3.
    #
    # Configured allow-list reads from app.config so deployments can add
    # their own origins (e.g. mobile-app webview, secondary FQDNs) without
    # editing source. Defaults to the canonical OTS_FQDN if the deployment
    # doesn't override.
    _cors_origins = app.config.get("OTS_CORS_ORIGINS")
    if not _cors_origins:
        fqdn = app.config.get("OTS_FQDN", "")
        # Common access shapes for the same deployment: HTTPS on the
        # canonical name (browser typed URL) and the dashboard port (8180).
        _cors_origins = [
            f"https://{fqdn}",
            f"https://{fqdn}:8180",
        ] if fqdn else []
    cors = CORS(
        app,
        resources={
            r"/api/*":   {"origins": _cors_origins},
            r"/Marti/*": {"origins": _cors_origins},
        },
        supports_credentials=True,
    )
    flask_wtf.CSRFProtect(app)

    socketio_logger = False
    if app.config.get("DEBUG"):
        socketio_logger = logger
    rabbitmq_user = app.config.get("OTS_RABBITMQ_USERNAME", "guest")
    rabbitmq_pass = app.config.get("OTS_RABBITMQ_PASSWORD", "guest")
    rabbitmq_host = app.config.get("OTS_RABBITMQ_SERVER_ADDRESS", "rabbitmq")
    socketio.init_app(
        app,
        logger=socketio_logger,
        ping_timeout=60,
        cors_allowed_origins="*",
        message_queue=f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}",
    )

    rabbit_credentials = pika.PlainCredentials(
        app.config.get("OTS_RABBITMQ_USERNAME"), app.config.get("OTS_RABBITMQ_PASSWORD")
    )
    rabbit_host = app.config.get("OTS_RABBITMQ_SERVER_ADDRESS")
    rabbit_connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_host, credentials=rabbit_credentials)
    )

    channel = rabbit_connection.channel()
    channel.exchange_declare("dms", durable=True, exchange_type="direct")
    channel.exchange_declare("cot_parser", durable=True, exchange_type="direct")
    channel.exchange_declare("chatrooms", durable=True, exchange_type="direct")
    channel.exchange_declare(
        "missions", durable=True, exchange_type="topic"
    )  # For Data Sync mission feeds
    channel.exchange_declare("groups", durable=True, exchange_type="topic")  # For channels/groups
    channel.exchange_declare(
        "firehose", durable=True, exchange_type="fanout"
    )  # A firehose of all CoT data
    channel.exchange_declare("flask-socketio", durable=False, exchange_type="fanout")
    channel.close()
    rabbit_connection.close()

    if not apscheduler.running:
        apscheduler.init_app(app)
        apscheduler.start(paused=False)

    try:
        fsqla.FsModels.set_db_info(db)
    except sqlalchemy.exc.InvalidRequestError:
        pass

    from opentakserver.models.role import Role
    from opentakserver.models.user import User

    user_datastore = SQLAlchemyUserDatastore(db, User, Role, WebAuthn)
    app.security = Security(
        app,
        user_datastore,
        mail_util_cls=EmailValidator,
        password_util_cls=PasswordValidator,
        username_util_cls=UsernameValidator,
    )

    mail.init_app(app)

    babel.init_app(app, locale_selector=get_locale, timezone_selector=get_timezone)


def setup_logging(app):
    level = logging.INFO
    if app.config.get("DEBUG"):
        level = logging.DEBUG
    logger.setLevel(level)

    if sys.stdout.isatty():
        color_log_handler = colorlog.StreamHandler()
        color_log_formatter = colorlog.ColoredFormatter(
            "%(log_color)s[%(asctime)s] - OpenTAKServer[%(process)d] - %(module)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %Z",
        )
        color_log_handler.setFormatter(color_log_formatter)
        logger.addHandler(color_log_handler)
        logger.info("Added color logger")

    os.makedirs(os.path.join(app.config.get("OTS_DATA_FOLDER"), "logs"), exist_ok=True)
    fh = TimedRotatingFileHandler(
        os.path.join(app.config.get("OTS_DATA_FOLDER"), "logs", "opentakserver.log"),
        when=app.config.get("OTS_LOG_ROTATE_WHEN"),
        interval=app.config.get("OTS_LOG_ROTATE_INTERVAL"),
        backupCount=app.config.get("OTS_BACKUP_COUNT"),
    )
    fh.setFormatter(
        logging.Formatter(
            "[%(asctime)s] - OpenTAKServer[%(process)d] - %(module)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s"
        )
    )
    logger.addHandler(fh)


def create_app(cli=True):
    app = Flask(__name__)
    app.config.from_object(DefaultConfig)
    setup_logging(app)

    # Treat /api/foo and /api/foo/ as the same route. Flask's default behavior
    # is to redirect the missing-slash form to the slashed form via Location
    # header — but behind this proxy chain, request.url_root sometimes parses
    # to an empty hostname (`https:///api/foo/`), which the browser then tries
    # to dial as host="api" and fails with ERR_NAME_NOT_RESOLVED. Disabling
    # strict_slashes makes Flask match either form directly, no redirect, no
    # malformed Location.
    app.url_map.strict_slashes = False

    if not cli:
        # Load config.yml if it exists
        if os.path.exists(os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml")):
            app.config.from_file(
                os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), load=yaml.safe_load
            )
        else:
            # First run, created config.yml based on default settings
            logger.info("Creating config.yml")
            with open(os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), "w") as config:
                conf = {}
                for option in DefaultConfig.__dict__:
                    # Don't save a list of languages to the config, use the list in defaultconfig.py instead
                    if option == "OTS_LANGUAGES":
                        continue

                    # Fix the sqlite DB path on Windows
                    if (
                        option == "SQLALCHEMY_DATABASE_URI"
                        and platform.system() == "Windows"
                        and DefaultConfig.__dict__[option].startswith("sqlite")
                    ):
                        conf[option] = (
                            DefaultConfig.__dict__[option].replace("////", "///").replace("\\", "/")
                        )
                    elif option.isupper():
                        conf[option] = DefaultConfig.__dict__[option]
                config.write(yaml.safe_dump(conf))

        # Try to set the MediaMTX token
        if app.config.get("OTS_MEDIAMTX_ENABLE"):
            try:
                new_conf = None
                with open(
                    os.path.join(app.config.get("OTS_DATA_FOLDER"), "mediamtx", "mediamtx.yml"), "r"
                ) as mediamtx_config:
                    conf = mediamtx_config.read()
                    if "MTX_TOKEN" in conf:
                        new_conf = conf.replace("MTX_TOKEN", app.config.get("OTS_MEDIAMTX_TOKEN"))
                if new_conf:
                    with open(
                        os.path.join(app.config.get("OTS_DATA_FOLDER"), "mediamtx", "mediamtx.yml"),
                        "w",
                    ) as mediamtx_config:
                        mediamtx_config.write(new_conf)
            except BaseException as e:
                logger.error("Failed to set MediaMTX token: {}".format(e))
        else:
            logger.info("MediaMTX disabled")

        init_extensions(app)

        from opentakserver.blueprints.marti_api import marti_blueprint

        app.register_blueprint(marti_blueprint)

        from opentakserver.blueprints.ots_api import ots_api

        app.register_blueprint(ots_api)

        # Plugin SDK v2: instantiate the loader and stash it on
        # app.extensions so the /api/plugins/v2/* admin endpoints (and
        # any other consumer) can find it. We do NOT call .discover()
        # here — that happens later, alongside the legacy
        # PluginManager.load_plugins() call, so vanilla and v2 plugins
        # boot together. If the loader import fails for any reason, the
        # v2 admin API gracefully degrades to "no v2 plugins known".
        if not hasattr(app, "extensions") or app.extensions is None:
            app.extensions = {}
        try:
            from opentakserver.sdk.loader_v2 import PluginManagerV2

            app.extensions["plugin_manager_v2"] = PluginManagerV2(app)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to initialise PluginManagerV2: {exc}")
            logger.debug(traceback.format_exc())
            app.extensions.setdefault("plugin_manager_v2", None)

        from opentakserver.blueprints.ots_socketio import ots_socketio_blueprint

        app.register_blueprint(ots_socketio_blueprint)

        from opentakserver.blueprints.scheduled_jobs import scheduler_blueprint

        app.register_blueprint(scheduler_blueprint)

        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1)

    else:
        from opentakserver.blueprints.cli import ots, translate

        app.cli.add_command(ots, name="ots")
        app.cli.add_command(translate, name="translate")

        if os.path.exists(os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml")):
            app.config.from_file(
                os.path.join(app.config.get("OTS_DATA_FOLDER"), "config.yml"), load=yaml.safe_load
            )
            db.init_app(app)
            Migrate(app, db)

        flask_wtf.CSRFProtect(app)

        try:
            fsqla.FsModels.set_db_info(db)
        except sqlalchemy.exc.InvalidRequestError:
            pass

        from opentakserver.models.role import Role
        from opentakserver.models.user import User

        user_datastore = SQLAlchemyUserDatastore(db, User, Role, WebAuthn)
        app.security = Security(
            app,
            user_datastore,
            mail_util_cls=EmailValidator,
            password_util_cls=PasswordValidator,
            username_util_cls=UsernameValidator,
        )

        # Register blueprints to properly import all the DB models without circular imports
        from opentakserver.blueprints.marti_api import marti_blueprint

        app.register_blueprint(marti_blueprint)

        from opentakserver.blueprints.ots_api import ots_api

        app.register_blueprint(ots_api)

        from opentakserver.blueprints.ots_socketio import ots_socketio_blueprint

        app.register_blueprint(ots_socketio_blueprint)

        from opentakserver.blueprints.scheduled_jobs import scheduler_blueprint

        app.register_blueprint(scheduler_blueprint)

    return app


def create_default_groups(app):
    with app.app_context():
        if not app.config.get("OTS_ENABLE_LDAP"):
            anon_group = db.session.execute(
                db.session.query(Group).filter_by(name="__ANON__")
            ).first()
            adsb_group = db.session.execute(
                db.session.query(Group).filter_by(name=app.config.get("OTS_ADSB_GROUP"))
            ).first()
            ais_group = db.session.execute(
                db.session.query(Group).filter_by(name=app.config.get("OTS_AIS_GROUP"))
            ).first()
            meshtastic_group = db.session.execute(
                db.session.query(Group).filter_by(name=app.config.get("OTS_MESHTASTIC_GROUP"))
            ).first()

            # Commit to DB after every one to ensure that get_next_bitpos works

            if not anon_group:
                logger.info("Creating the __ANON__ group")
                anon_group = Group()
                anon_group.name = "__ANON__"
                anon_group.type = GroupTypeEnum.SYSTEM
                anon_group.bitpos = 2
                db.session.add(anon_group)
                db.session.commit()

            if not adsb_group:
                logger.info(f"Creating the {app.config.get('OTS_ADSB_GROUP')} group")
                adsb_group = Group()
                adsb_group.name = app.config.get("OTS_ADSB_GROUP")
                adsb_group.type = GroupTypeEnum.SYSTEM
                adsb_group.bitpos = adsb_group.get_next_bitpos()
                db.session.add(adsb_group)
                db.session.commit()

            if not ais_group:
                logger.info(f"Creating the {app.config.get('OTS_AIS_GROUP')} group")
                ais_group = Group()
                ais_group.name = app.config.get("OTS_AIS_GROUP")
                ais_group.type = GroupTypeEnum.SYSTEM
                ais_group.bitpos = ais_group.get_next_bitpos()
                db.session.add(ais_group)
                db.session.commit()

            if not meshtastic_group:
                logger.info(f"Creating the {app.config.get('OTS_MESHTASTIC_GROUP')} group")
                meshtastic_group = Group()
                meshtastic_group.name = app.config.get("OTS_MESHTASTIC_GROUP")
                meshtastic_group.type = GroupTypeEnum.SYSTEM
                meshtastic_group.bitpos = meshtastic_group.get_next_bitpos()
                db.session.add(meshtastic_group)
                db.session.commit()


def main(app):
    with app.app_context():
        # Download the icon sets if they aren't already in the DB
        icons = db.session.query(Icon).count()
        if icons == 0:
            logger.info("Downloading icons...")
            try:
                r = requests.get(
                    "https://github.com/brian7704/OpenTAKServer-Installer/raw/master/iconsets.sqlite",
                    stream=True,
                )
                with open(
                    os.path.join(app.config.get("OTS_DATA_FOLDER"), "icons.sqlite"), "wb"
                ) as f:
                    f.write(r.content)

                def dict_factory(cursor, row):
                    d = {}
                    for idx, col in enumerate(cursor.description):
                        d[col[0]] = row[idx]
                    return d

                con = sqlite3.connect(
                    os.path.join(app.config.get("OTS_DATA_FOLDER"), "icons.sqlite")
                )
                con.row_factory = dict_factory
                cur = con.cursor()
                rows = cur.execute("SELECT * FROM icons")
                for row in rows:
                    db.session.execute(insert(Icon).values(**row))
                db.session.commit()
            except BaseException as e:
                logger.error("Failed to download icons: {}".format(e))
                logger.debug(traceback.format_exc())

        if app.config.get("DEBUG"):
            logger.debug("Starting in debug mode")
        else:
            logger.info("Starting in production mode")

        app.security.datastore.find_or_create_role(
            name="user", permissions={"user-read", "user-write"}
        )

        app.security.datastore.find_or_create_role(
            name="administrator", permissions={"administrator"}
        )

        # Make sure at least one admin user exists
        admin_user = db.session.execute(
            db.session.query(Role)
            .join(fsqla_v3.FsModels.roles_users)
            .where(Role.name == "administrator")
        ).scalar()
        if not admin_user:
            logger.info("Creating administrator account. The password is 'password'")
            app.security.datastore.create_user(
                username="administrator",
                password=hash_password("password"),
                roles=["administrator"],
            )
        db.session.commit()

    if app.config.get("OTS_ENABLE_MESHTASTIC"):
        mestastic_thread = MeshtasticController(app.app_context())
        app.mestastic_thread = mestastic_thread
    else:
        app.meshtastic_thread = None

    if app.config.get("OTS_ENABLE_MUMBLE_AUTHENTICATION"):
        try:
            logger.info("Starting Mumble authentication handler")
            mumble_daemon = MumbleIceDaemon(app, logger)
            mumble_daemon.daemon = True
            mumble_daemon.start()
        except BaseException as e:
            logger.error("Failed to enable Mumble authentication: {}".format(e))
            logger.error(traceback.format_exc())
    else:
        logger.info("Mumble authentication handler disabled")

    if app.config.get("OTS_ENABLE_PLUGINS"):
        try:
            app.plugin_manager = PluginManager(Plugin.group, app)
            app.plugin_manager.load_plugins()
            app.plugin_manager.activate(app)
        except BaseException as e:
            logger.error(f"Failed to load plugins: {e}")
            logger.debug(traceback.format_exc())

        # Plugin SDK v2: discover + register every v2 plugin so its mounts
        # appear in /api/plugins/v2/installed and /api/plugins/v2/mounts. The
        # vanilla loader above has already wired the legacy blueprints; the
        # v2 loader is purely additive (different URL prefix per slug).
        v2_mgr = app.extensions.get("plugin_manager_v2") if hasattr(app, "extensions") else None
        if v2_mgr is not None:
            try:
                discovered = v2_mgr.discover()
                for manifest in discovered:
                    try:
                        v2_mgr.register(manifest)
                    except BaseException as e:
                        logger.error(
                            "Failed to register v2 plugin %s: %s", manifest.slug, e
                        )
                        logger.debug(traceback.format_exc())
                logger.info(
                    "Plugin SDK v2: discovered %d, registered %d",
                    len(discovered),
                    len(v2_mgr.manifests()),
                )
            except BaseException as e:
                logger.error(f"Plugin SDK v2 discovery failed: {e}")
                logger.debug(traceback.format_exc())

    app.start_time = datetime.now(timezone.utc)

    create_default_groups(app)

    try:
        socketio.run(
            app,
            host=app.config.get("OTS_LISTENER_ADDRESS"),
            port=app.config.get("OTS_LISTENER_PORT"),
            debug=app.config.get("DEBUG"),
            log_output=app.config.get("DEBUG"),
            use_reloader=False,
        )
    except KeyboardInterrupt:
        logger.warning("Caught CTRL+C, exiting...")
        if app.config.get("OTS_ENABLE_PLUGINS"):
            app.plugin_manager.stop_plugins()


def start():
    app = create_app(cli=False)

    @user_registered.connect_via(app)
    def user_registered_sighandler(app, user, confirmation_token, **kwargs):
        default_role = app.security.datastore.find_or_create_role(
            name="user", permissions={"user-read", "user-write"}
        )
        app.security.datastore.add_role_to_user(user, default_role)

    main(app)
