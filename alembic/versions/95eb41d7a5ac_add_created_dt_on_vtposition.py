"""add created_dt on vtposition

Revision ID: 95eb41d7a5ac
Revises: a28c60594937
Create Date: 2026-05-08 14:37:04.149176

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '95eb41d7a5ac'
down_revision: Union[str, None] = 'a28c60594937'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('vtposition', sa.Column('created_dt', sa.BigInteger(), nullable=True))

    # # 同步存量数据 对于已有的持仓记录  created_dt 默认设置为记录日期datetime
    # op.execute("UPDATE vtposition SET created_dt = datetime")
    
    # # nullable=False 在 PostgreSQL如果存在存量数据且没有默认值必须先执行 UPDATE
    # op.alter_column('vtposition', 'created_dt',
    #            existing_type=sa.BigInteger(),
    #            nullable=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('vtposition', 'created_dt')
