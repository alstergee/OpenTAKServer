"""Merge the fork's head with upstream's federation chain

Revision ID: m21112026
Revises: m17102026, 434be4e1896a
Create Date: 2026-11-21

Upstream added 9b37b021e2e5 -> 8bc1fb003330 -> 434be4e1896a (federation tables)
on top of 9e8eaa6c5a4d, the same parent the fork's m17102026 merge point sits on.
A database at m17102026 gets the three federation revisions applied and then
lands here; this revision itself changes nothing. It gives `flask db upgrade`
(run on every app boot) a single head again.
"""

# revision identifiers, used by Alembic.
revision = "m21112026"
down_revision = ("m17102026", "434be4e1896a")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
