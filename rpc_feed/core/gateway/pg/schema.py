#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List
from typing import Optional
from sqlalchemy import func
from sqlalchemy import Integer, String, ForeignKey, BigInteger, Float, LargeBinary
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.schema import PrimaryKeyConstraint, CreateTable, UniqueConstraint
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import Identity

__all__ = ["Asset", "Benchmark", "Adjustment", "Rightment"]


class Base(DeclarativeBase):
    # PostgreSQL id autoincrement when primary_key / SERIAL / IDENTITY 
    
    def serialize(self, include_id=False):
        if include_id:
            return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
        else:
            return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs if c.key != "id"}


class Asset(Base):


    __tablename__ = "asset"
    __table_args__ = (
        PrimaryKeyConstraint("id", "sid", name="pk_id_sid"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    sid: Mapped[bytes] = mapped_column(LargeBinary, unique=True, nullable=False)
    name: Mapped[bytes] = mapped_column(LargeBinary, nullable=False) # String(32, collation="c")
    first_trading: Mapped[int] = mapped_column(Integer, nullable=False)
    delist: Mapped[int] = mapped_column(Integer, default=0)

    adjustment: Mapped[List["Adjustment"]] = relationship("Adjustment", back_populates="asset", cascade="all, delete-orphan")
    rightment: Mapped[List["Rightment"]] = relationship("Rightment", back_populates="asset", cascade="all, delete-orphan")
    # line: Mapped[List["Line"]] = relationship("Line", back_populates="asset", cascade="all, delete-orphan")


class Benchmark(Base):
   
    __tablename__ = "benchmark"
    __table_args__ = (
        PrimaryKeyConstraint("sid", "date", name="pk_sid_date_bench"),
    )
    id: Mapped[int] = mapped_column(Integer, Identity(), nullable=False) # primary=True, autoincrement=True
    sid: Mapped[bytes] = mapped_column(LargeBinary, nullable=False, use_existing_column=True)
    date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True, index=True)
    open: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    high: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    low: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    close: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False, use_existing_column=True)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False, use_existing_column=True)


class Adjustment(Base):

    __tablename__ = "adjustment"
    __table_args__ = (
        UniqueConstraint("sid", "report_date", name="uq_sid_report_date_adjustment"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sid: Mapped[bytes] = mapped_column(LargeBinary, 
                                     ForeignKey("asset.sid", onupdate="CASCADE", ondelete="CASCADE"), 
                                     nullable=False, use_existing_column=True)
    report_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    register_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    ex_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    bonus_share: Mapped[float] = mapped_column(Float, nullable=False, use_existing_column=True) # 送股
    transfer: Mapped[float] = mapped_column(Float, nullable=False, use_existing_column=True) # 转股
    bonus: Mapped[float] = mapped_column(Float, nullable=False, use_existing_column=True) # 股息

    asset: Mapped["Asset"] = relationship("Asset", back_populates="adjustment")


class Rightment(Base):

    __tablename__ = "rightment"
    __table_args__ = (
        UniqueConstraint("sid", "ex_date", name="uq_sid_ex_date_rightment"),
        {"extend_existing": True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sid: Mapped[bytes] = mapped_column(LargeBinary, 
                                     ForeignKey("asset.sid", onupdate="CASCADE", ondelete="CASCADE"), 
                                     nullable=False, primary_key=True, use_existing_column=True)
    report_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    register_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    ex_date: Mapped[int] = mapped_column(Integer, nullable=False, use_existing_column=True)
    price: Mapped[float] = mapped_column(Float, nullable=False, use_existing_column=True)
    ratio: Mapped[float] = mapped_column(Float, nullable=False, use_existing_column=True)

    asset: Mapped["Asset"] = relationship("Asset", back_populates="rightment")


# # --- Partitioning Support ---

# # __table_args__ = (
# #    {'postgresql_partition_by': 'RANGE (tick)', "extend_existing": True},
# # )

# # compile create table with partition by or raw sql ddl
# @compiles(CreateTable)
# def compile_create_partition_table(element, compiler, **kw):
#     table = element.element
#     if "postgresql_partition_by" in table.dialect_options["postgresql"]:
#         partition = table.dialect_options["postgresql"]["postgresql_partition_by"]
#         ddl = compiler.visit_create_table(element)
#         return ddl.replace("\n)", f"\n) PARTITION BY {partition}")
#     return compiler.visit_create_table(element)

