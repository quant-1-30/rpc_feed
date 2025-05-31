#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List
from typing import Optional
from sqlalchemy import func
from sqlalchemy import Integer, String, ForeignKey, BigInteger, UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.schema import PrimaryKeyConstraint, CreateTable, UniqueConstraint
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import Identity



# declarative base class
class Base(DeclarativeBase):
    
    def serialize(self, include_id=False):
        if include_id:
            return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
        else:
            return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs if c.key != "id"}


class Calendar(Base):

    __tablename__ = "calendar"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trading_date: Mapped[int] = mapped_column(Integer,unique=True, nullable=False, primary_key=True, use_existing_column=True)


class Asset(Base):


    __tablename__ = "asset"
    __table_args__ = (
        PrimaryKeyConstraint("id", "sid", name="pk_id_sid"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    sid: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(25), nullable=False)
    first_trading: Mapped[int] = mapped_column(Integer, nullable=False)
    delist: Mapped[int] = mapped_column(Integer, default=0)

    line: Mapped[List["Line"]] = relationship("Line", back_populates="asset", cascade="all, delete-orphan")
    adjustment: Mapped[List["Adjustment"]] = relationship("Adjustment", back_populates="asset", cascade="all, delete-orphan")
    rightment: Mapped[List["Rightment"]] = relationship("Rightment", back_populates="asset", cascade="all, delete-orphan")


class Line(Base):
   
    __tablename__ = "tick"
    __table_args__ = (
        PrimaryKeyConstraint("sid", "tick", name="pk_sid_tick_line"),
        {'postgresql_partition_by': 'RANGE (tick)', "extend_existing": True},
    )
    # 在 PostgreSQL 中，只有当某个字段是主键或有 SERIAL 或 IDENTITY 声明时，它才会自动自增。
    # id: Mapped[int] = mapped_column(Integer, primary=True, autoincrement=True)
    id: Mapped[int] = mapped_column(Integer, Identity(), nullable=False)
    sid: Mapped[str] = mapped_column(String(20), 
                                     ForeignKey("asset.sid", onupdate="CASCADE", ondelete="CASCADE"), 
                                     nullable=False, use_existing_column=True)
    tick: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, use_existing_column=True, index=True)
    open: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    high: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    low: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    close: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False, use_existing_column=True)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False, use_existing_column=True)

    asset: Mapped["Asset"] = relationship("Asset", back_populates="line")
    
    # def __init__(self, kwargs):
    #     valid_keys = [column.name for column in self.__table__.columns]
    #     for key, value in kwargs.items():
    #         # if hasattr(self, key):  # 只设置模型中定义的字段
    #         if key in valid_keys:
    #             setattr(self, key, value)


class Adjustment(Base):

    # register_date:登记日 ; ex_date:除权除息日 ; pay_date:除权除息日 ; effective_date:上市日期
    # 股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
    # 上交所证券的红股上市日为股权除权日的下一个交易日; 深交所证券的红股上市日为股权登记日后的第3个交易日
    # share --- 送股 / transfer --- 转股 / interest --- 股息

    __tablename__ = "adjustment"
    __table_args__ = (
        UniqueConstraint("sid", "register_date", name="uq_sid_register_date_adjustment"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sid: Mapped[str] = mapped_column(String(20), 
                                     ForeignKey("asset.sid", onupdate="CASCADE", ondelete="CASCADE"), 
                                     nullable=False, use_existing_column=True)
    register_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    ex_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    share: Mapped[int] = mapped_column(Integer, nullable=True, use_existing_column=True)
    transfer: Mapped[int] = mapped_column(Integer, nullable=True, use_existing_column=True)
    interest: Mapped[int] = mapped_column(Integer, nullable=True, use_existing_column=True)

    asset: Mapped["Asset"] = relationship("Asset", back_populates="adjustment")


class Rightment(Base):

    # register_date:登记日 ; ex_date:除权除息日; pay_date:除权除息日 ; effective_date:上市日期 
    # 股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
    # 上交所证券的红股上市日为股权除权日的下一个交易日; 深交所证券的红股上市日为股权登记日后的第3个交易日
    # price --- 配股价格 / ratio --- 配股比例

    __tablename__ = "rightment"
    __table_args__ = (
        UniqueConstraint("sid", "register_date", name="uq_sid_register_date_rightment"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sid: Mapped[str] = mapped_column(String(20), 
                                     ForeignKey("asset.sid", onupdate="CASCADE", ondelete="CASCADE"), 
                                     nullable=False, primary_key=True, use_existing_column=True)
    register_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    ex_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    # effective_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    price: Mapped[int] = mapped_column(Integer, nullable=True, use_existing_column=True)
    ratio: Mapped[int] = mapped_column(Integer, nullable=True, use_existing_column=True)

    asset: Mapped["Asset"] = relationship("Asset", back_populates="rightment")


# --- Partitioning Support ---
# compile create table with partition by or raw sql ddl
@compiles(CreateTable)
def compile_create_partition_table(element, compiler, **kw):
    table = element.element
    if "postgresql_partition_by" in table.dialect_options["postgresql"]:
        partition = table.dialect_options["postgresql"]["postgresql_partition_by"]
        ddl = compiler.visit_create_table(element)
        return ddl.replace("\n)", f"\n) PARTITION BY {partition}")
    return compiler.visit_create_table(element)


__all__ = ["Calendar", "Asset", "Line", "Adjustment", "Rightment"]
