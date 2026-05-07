"""revise unqiue order_id in order_bit

Revision ID: 542a904f887a
Revises: 30aae0684ec1
Create Date: 2026-05-07 21:01:22.936570

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '542a904f887a'
down_revision: Union[str, None] = '30aae0684ec1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""revise unqiue order_id in order_bit

Revision ID: a28c60594937
Revises: 30aae0684ec1
Create Date: 2026-05-07 20:27:31.454968

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a28c60594937'
down_revision: Union[str, None] = '30aae0684ec1'
branch_labels: Union[str, Sequence[str], None] = None  
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint('order_bit_order_id_key', 'order_bit', type_='unique') # type_='foreignkey'
    

def downgrade() -> None:
    op.create_unique_constraint('order_bit_order_id_key', 'order_bit', ['order_id'])
