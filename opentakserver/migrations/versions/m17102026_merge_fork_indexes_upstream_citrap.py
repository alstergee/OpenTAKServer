"""Merge fork hot-path indexes with upstream GeoAlchemy2/CITrap chain

Revision ID: m17102026
Revises: a05082026, 9e8eaa6c5a4d
Create Date: 2026-10-17

alstergee-fixes added a05082026 (hot-path indexes) on top of 00442761c803.
Upstream later added 640de7aafac2 -> 1a62289c9301 -> 9e8eaa6c5a4d on the same
parent. Both branches are independent (indexes vs. new points.point column and
citrap/groups_citrap tables), so this is a no-op merge point that gives
`flask db upgrade` (run on every app boot) a single head again.
"""

# revision identifiers, used by Alembic.
revision = "m17102026"
down_revision = ("a05082026", "9e8eaa6c5a4d")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
