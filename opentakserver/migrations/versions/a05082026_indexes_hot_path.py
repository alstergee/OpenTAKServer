"""Add hot-path indexes (audit 2026-05-08 finding H-B1 / H-B2)

Revision ID: a05082026
Revises: 00442761c803
Create Date: 2026-05-08

84 ForeignKey columns in the codebase; before this migration, only ONE had
index=True. Postgres doesn't auto-index FKs, so every join was a sequential
scan on the child side. Two specific paths showed in profiling:

  * meshtastic_controller.save_chat_to_db's dedupe query:
        WHERE chatroom_id=? AND sender_uid=? AND remarks=? AND timestamp>=?
    runs once per inbound mesh message — sequential scan per chat write.
    Composite index (chatroom_id, sender_uid, timestamp) covers it.

  * /api/map_state filtering EUDs by last_event_time on every poll.
    A single-column index makes that B-tree-bounded.

Plus the foundational FK indexes for the most-traversed joins. These are the
"top 8" by frequency observed in OTS server logs; the remaining 76 FK columns
are deferred — Postgres will not benefit from indexing low-cardinality or
read-rarely tables.

CREATE INDEX CONCURRENTLY would be ideal for production, but Alembic's
autogenerate-friendly default is plain CREATE INDEX. Index creation acquires
a SHARE lock on the table; for the GeoChat composite this can pause writes
briefly. Acceptable on this deployment scale (festival ops, not 1k req/s).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a05082026"
down_revision = "00442761c803"
branch_labels = None
depends_on = None


# Each tuple: (index_name, table, columns, optional_where_clause)
INDEXES = [
    # Hot-path GeoChat dedupe (covers chatroom + sender + timestamp filtering)
    ("ix_geochat_chatroom_sender_ts", "geochat", ["chatroom_id", "sender_uid", "timestamp"], None),
    ("ix_geochat_timestamp", "geochat", ["timestamp"], None),
    # EUD recency (used by /api/map_state filter)
    ("ix_euds_last_event_time", "euds", ["last_event_time"], None),
    # Marker → CoT join (every map poll filters on CoT.stale via this join)
    ("ix_markers_cot_id", "markers", ["cot_id"], None),
    ("ix_rb_lines_cot_id", "rb_lines", ["cot_id"], None),
    ("ix_casevac_cot_id", "casevac", ["cot_id"], None),
    # Point → EUD (lazy-loaded in many to_json() paths)
    ("ix_points_device_uid", "points", ["device_uid"], None),
    # CoT.stale itself (the recency filter target)
    ("ix_cot_stale", "cot", ["stale"], None),
]


def upgrade():
    for name, table, cols, where in INDEXES:
        kwargs = {}
        if where:
            kwargs["postgresql_where"] = sa.text(where)
        try:
            op.create_index(name, table, cols, **kwargs)
        except Exception as e:
            # Index may already exist on databases that ran an earlier version;
            # swallow IDEMPOTENT-creation errors so re-running the migration
            # against an already-indexed DB doesn't fail.
            if "already exists" not in str(e).lower():
                raise


def downgrade():
    for name, _, _, _ in reversed(INDEXES):
        try:
            op.drop_index(name)
        except Exception:
            pass
