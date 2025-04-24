#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import numpy as np
from functools import total_ordering
from pydantic import BaseModel, Field
# from functools import total_ordering
from typing import List, Union, Optional, Dict


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

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.trading_date == _value.trading_data
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.trading_date < _value.trading_date
    

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


class LineModel(BaseModel):

    sid: str = Field(default="")
    tick: int = Field(gt=0)
    open: int = Field(gt=0)
    high: int = Field(gt=0)
    low: int = Field(gt=0)
    close: int = Field(gt=0)
    volume: int = Field(gt=0)
    amount: int = Field(gt=0)

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.tick == _value.tick
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid > _value.sid or (self.sid == _value.sid and self.tick > _value.tick)


class AdjustmentModel(BaseModel):

    sid: str = Field(default="")
    register_date: int = Field(gt=0)
    ex_date: int = Field(gt=0)
    share: int = Field(default=0)
    transfer: int = Field(default=0)
    interest: int = Field(default=0)

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.register_date == _value.register_date
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.register_date < _value.register_date


class RightmentModel(BaseModel):

    sid: str = Field(default="")
    register_date: int = Field(gt=0)
    ex_date: int = Field(gt=0)
    # effective_date: int
    price: int = Field(default=0)
    ratio: int = Field(default=0)

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.register_date == _value.register_date
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.register_date < _value.register_date
