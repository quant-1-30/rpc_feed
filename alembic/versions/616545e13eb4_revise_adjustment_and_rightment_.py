"""revise adjustment and rightment nullable from true to false

Revision ID: 616545e13eb4
Revises: 28cdc82de517
Create Date: 2025-12-29 15:40:47.671342

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '616545e13eb4'
down_revision: Union[str, None] = '28cdc82de517'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # NULL refresh to 0 / 0.0 
    op.execute("UPDATE adjustment SET bonus_share = 0 WHERE bonus_share IS NULL")
    op.execute("UPDATE adjustment SET transfer = 0 WHERE transfer IS NULL")
    op.execute("UPDATE adjustment SET bonus = 0 WHERE bonus IS NULL")
    op.execute("UPDATE rightment SET price = 0 WHERE price IS NULL")
    op.execute("UPDATE rightment SET ratio = 0 WHERE ratio IS NULL")
    
    # revise column constraint and set null
    # sever_default must string to sql on postgres /  default on python orm insert
    op.alter_column('adjustment', 'bonus_share',
               existing_type=sa.Float(),
               nullable=False,       
               server_default='0')   
    op.alter_column('adjustment', 'transfer',
               existing_type=sa.Float(),
               nullable=False,        
               server_default='0')     
    op.alter_column('adjustment', 'bonus',
               existing_type=sa.Float(),
               nullable=False,       
               server_default='0')    
    op.alter_column('rightment', 'price',
               existing_type=sa.Float(),
               nullable=False,        
               server_default='0')  
    op.alter_column('rightment', 'ratio',
               existing_type=sa.Float(),
               nullable=False,      
               server_default='0')   


def downgrade() -> None:
    """Downgrade schema."""
    pass
