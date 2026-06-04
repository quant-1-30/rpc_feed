"""change sid and name from str to bytes

Revision ID: 28cdc82de517
Revises: 
Create Date: 2025-12-29 13:10:57.564788

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '28cdc82de517'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = ('feed',)  
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # remove subidiary table foreign key
    op.drop_constraint('adjustment_sid_fkey', 'adjustment', type_='foreignkey')
    op.drop_constraint('rightment_sid_fkey', 'rightment', type_='foreignkey')
    
    # remove main primary / unique 
    op.drop_constraint('asset_sid_key', 'asset', type_='unique')
    op.drop_constraint('pk_id_sid', 'asset', type_='primary')

    # core str --->BYTEA and USING ensure bytes
    tables_to_fix = ['asset', 'adjustment', 'rightment', 'benchmark']
    for table in tables_to_fix:
        op.alter_column(table, 'sid',
                   existing_type=sa.String(),
                   type_=sa.LargeBinary(),
                   postgresql_using='sid::bytea')

    op.alter_column("asset", "name", existing_type=sa.String(), type_=sa.LargeBinary(), postgresql_using='sid::bytea')

    # rebuild main primary and unqiue
    op.create_primary_key('pk_id_sid', 'asset', ['id', 'sid'])
    op.create_unique_constraint('asset_sid_key', 'asset', ['sid'])

    # rebuild subsidiary foreign key
    op.create_foreign_key(
        'adjustment_sid_fkey', 'adjustment', 'asset', 
        ['sid'], ['sid'], onupdate='CASCADE', ondelete='CASCADE'
    )
    op.create_foreign_key(
        'rightment_sid_fkey', 'rightment', 'asset', 
        ['sid'], ['sid'], onupdate='CASCADE', ondelete='CASCADE'
    )

    # op.add_column('account', sa.Column('last_transaction_date', sa.DateTime))


def downgrade() -> None:
    """Downgrade schema."""
    
    # bytes --> str
    op.alter_column('assets', 'name',
               existing_type=sa.LargeBinary(),
               type_=sa.String(),
               postgresql_using="encode(name, 'escape')")
    
    op.alter_column('assets', 'sid',
               existing_type=sa.LargeBinary(),
               type_=sa.String(),
               postgresql_using="encode(sid, 'escape')")
