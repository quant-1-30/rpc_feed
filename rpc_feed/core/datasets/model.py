#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import numpy as np
from functools import total_ordering
from pydantic import BaseModel, Field, field_validator, model_validator, field_serializer
from typing import List, Union, Optional, TypeVar, Type, Any

__all__ = ["Request", "CalendarModel", "AssetModel", "DailyModel", "LineModel", "CloseModel", "AdjustmentModel", "RightmentModel"]


class Request(BaseModel):

    start_date: int = Field(default=-np.inf)
    # end_date: int = Field(gt=0, default=np.inf)
    end_date: int = Field(default=np.inf)
    sid: List[str]=[]

    def __lt__(self, other):
        return True if max(other.start_date) <= max(self.start_date) else False
    
    def __repr__(self) -> str:
        # __str__ / __repr__ ; print 默认调用__str__  ; 如果__str__没有重写返回__repr__
        pass

    def range(self) -> List[int]:
        return [self.start_date, self.end_date]


class CalendarModel(BaseModel):

    trading_date: int = Field(gt=0, default=19900101)

    # def __eq__(self, _value: object) -> bool:
    #     if not isinstance(_value, self):
    #         raise TypeError
    #     return self.trading_date == _value.trading_data
    
    # def __lt__(self, _value: object) -> bool:
    #     if not isinstance(_value, self):
    #         raise TypeError
    #     return self.trading_date < _value.trading_date
    
    # # sorted(series, key=lambda x: getattr(x, by), reverse=not ascending)
    

class AssetModel(BaseModel):

    sid: str = Field(default="")
    name: str = Field(default="")
    first_trading: int = Field(gt=0)
    delist: int = Field(default=0)
    
    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.first_trading == _value.first_trading
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.first_trading < _value.first_trading


class DailyModel(BaseModel):

    sid: str = Field(default="")
    date: int = Field(ge=0)
    open: int = Field(ge=0)
    high: int = Field(ge=0)
    low: int = Field(ge=0)
    close: int = Field(ge=0)
    volume: int = Field(ge=0)
    amount: int = Field(ge=0)

    @field_validator('sid', mode='before')
    @classmethod
    def convert_sid_to_str(cls, v: Any) -> str:
        """强制将 sid 转换为字符串"""
        if v is None:
            return ""
        return str(v)


class LineModel(BaseModel):

    sid: str = Field(default="")
    tick: int = Field(ge=0)
    open: int = Field(ge=0)
    high: int = Field(ge=0)
    low: int = Field(ge=0)
    close: int = Field(ge=0)
    volume: int = Field(ge=0)
    amount: int = Field(ge=0)

    @field_validator('sid', mode='before')
    @classmethod
    def convert_sid_to_str(cls, v: Any) -> str:
        """强制将 sid 转换为字符串"""
        if v is None:
            return ""
        return str(v)
 

class CloseModel(BaseModel):

    sid: str = Field(default="")
    date: int = Field(ge=0)
    close: int = Field(ge=0)


class AdjustmentModel(BaseModel):

    sid: str = Field(default="")
    register_date: int = Field(ge=0)
    ex_date: int = Field(ge=0)
    bonus_share: float = Field(default=0.0)
    transfer: float = Field(default=0.0)
    bonus: float = Field(default=0.0)

    @field_serializer("bonus_share", "transfer", "bonus")
    def serialize_integer(self, v: float, info):
        return int(v*1000)

class RightmentModel(BaseModel):

    sid: str = Field(default="")
    register_date: int = Field(ge=0)
    ex_date: int = Field(ge=0)
    # effective_date: int
    price: float = Field(default=0.0)
    ratio: float = Field(default=0.0)

    @field_serializer("price", "ratio")
    def serialize_integer(self, v:float, info):
        return int(v*1000)
