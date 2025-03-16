#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from dataclasses import dataclass
from functools import total_ordering
import numpy as np
from pydantic import BaseModel, Field
# from functools import total_ordering
from typing import List, Union, Optional, Dict


class Request(BaseModel):

    start_date: int = Field(default=-np.inf)
    end_date: int = Field(gt=0, default=np.inf)
    sid: List[str]=[]

    def serialize(self) -> str:
        return {"start_date": self.start_date, "end_date": self.end_date, "sid": self.sid}
    
    def range(self) -> List[int]:
        return [self.start_date, self.end_date]

    def __lt__(self, other):
        return True if max(other.start_date) <= max(self.start_date) else False
    
    def __repr__(self) -> str:
        # __str__ / __repr__ ; print 默认调用__str__  ; 如果__str__没有重写返回__repr__
        pass


@dataclass(frozen=True)
# @total_ordering
class Calendar:

    trading_date: str

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.trading_date == _value.trading_data
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.trading_date < _value.trading_date
    
    def serialize(self) -> str:
        return {"trading_date": self.trading_date}


@dataclass(frozen=True)
# @total_ordering
class Asset:

    sid: str
    name: str
    first_trading: int
    delist: int
    
    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.first_trading == _value.first_trading
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.first_trading < _value.first_trading

    def serialize(self) -> dict:
        return {"sid": self.sid, "name": self.name, 
                "first_trading": self.first_trading, "delist": self.delist}
    

@dataclass(frozen=True)
# @total_ordering
class Line:

    sid: str
    tick: int
    open: int
    high: int
    low: int
    close: int
    volume: int
    amount: int

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.tick == _value.tick
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid > _value.sid or (self.sid == _value.sid and self.tick > _value.tick)

    def serialize(self) -> dict:

        return {"sid": self.sid, "tick": self.tick, "open": self.open, "high": self.high, 
                "low": self.low, "close": self.close, "volume": self.volume, "amount": self.amount}
    

@dataclass(frozen=True)     
# @total_ordering
class Dividend:

    sid: str
    register_date: int
    ex_date: int
    share: int = 0
    transfer: int = 0
    interest: int = 0

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.register_date == _value.register_date
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.register_date < _value.register_date

    def serialize(self) -> dict:
        return {"sid": self.sid, "register_date": self.register_date, 
                "ex_date": self.ex_date, "share": self.share,
                "transfer": self.transfer, "interest": self.interest}


@dataclass(frozen=True)     
# @total_ordering
class Rgt:

    sid: str
    register_date: int
    ex_date: int
    # effective_date: int
    price: int
    ratio: int

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.register_date == _value.register_date
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.register_date < _value.register_date

    def serialize(self) -> dict:
        return {"sid": self.sid, "register_date": self.register_date, "ex_date": self.ex_date, 
                "price": self.price, "ratio": self.ratio}
