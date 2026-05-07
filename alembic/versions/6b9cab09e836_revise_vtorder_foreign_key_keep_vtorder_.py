"""revise vtorder foreign key keep vtorder_id

Revision ID: 30aae0684ec1
Revises: 616545e13eb4
Create Date: 2026-05-07 07:57:36.374978

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '30aae0684ec1'
# down_revision: Union[str, None] = '616545e13eb4'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = ('trade',)  
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    
    # ==========================================================
    # 1. drop (sid, created_dt, experiment_id) unique constraint
    # ==========================================================
    op.drop_constraint('uq_order_sid_created_dt_experiment_id', 'vtorder', type_='unique')
    
    # ==========================================================
    # 2. order_id / experiment_id
    # ==========================================================
    op.create_unique_constraint('uq_order_id_experiment_id', 'vtorder', ['order_id', "experiment_id"])


def downgrade() -> None:
    """Downgrade schema."""
    
    op.drop_constraint('uq_order_id_experiment_id', 'vtorder', type_='unique')
    
    op.create_unique_constraint(
        'uq_order_sid_created_dt_experiment_id', 
        'vtorder', 
        ['sid', 'created_dt', 'experiment_id']
    )
