#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import numpy as np
from functools import total_ordering
from pydantic import BaseModel, Field, field_validator, model_validator, field_serializer
from typing import List, Union, Optional, TypeVar, Type, Any


__all__ = ["tuple_to_model", "Request", "CalendarModel", "AssetModel", "LineModel", "AdjustmentModel", "RightmentModel"]


T = TypeVar('T', bound=BaseModel)

def tuple_to_model(tuple_data: tuple, model_class: Type[T]) -> T:
    """
    将 SQL 查询返回的 tuple 转换为 Pydantic 对象
    
    Parameters
    ----------
    tuple_data : tuple
        SQL 查询返回的元组数据
    model_class : Type[T]
        Pydantic 模型类
        
    Returns
    -------
    T
        Pydantic 模型实例
        
    Examples
    --------
    >>> row = (1, "AAPL", 100)
    >>> model = tuple_to_model(row, StockModel)
    """
    # 获取模型字段名
    field_names = list(model_class.__annotations__.keys())
    
    # 创建字段名到值的映射
    data_dict = dict(zip(field_names, tuple_data))
    
    # 创建并返回模型实例
    return model_class(**data_dict)


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

    # gt / ge / lt / le / eq / ne
    sid: str = Field(default="")
    tick: int = Field(ge=0)
    open: int = Field(ge=0)
    high: int = Field(ge=0)
    low: int = Field(ge=0)
    close: int = Field(ge=0)
    volume: int = Field(ge=0)
    amount: int = Field(ge=0)

    # 验证器在模型初始化时执行
    @field_validator('sid', mode='before')
    @classmethod
    def convert_sid_to_str(cls, v: Any) -> str:
        """强制将 sid 转换为字符串"""
        if v is None:
            return ""
        return str(v)

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.tick == _value.tick
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid > _value.sid or (self.sid == _value.sid and self.tick > _value.tick)
    
    @classmethod
    def sort_series(cls, series: List['LineModel'], by: str = 'tick', ascending: bool = True) -> List['LineModel']:
        """
        对 LineModel 列表进行排序
        
        Parameters
        ----------
        series : List[LineModel]
            要排序的 LineModel 列表
        by : str
            排序字段，可选值: 'sid', 'tick', 'open', 'high', 'low', 'close', 'volume', 'amount'
        ascending : bool
            是否升序排序
            
        Returns
        -------
        List[LineModel]
            排序后的列表
            
        Examples
        --------
        >>> models = [LineModel(sid="AAPL", tick=100), LineModel(sid="GOOG", tick=200)]
        >>> sorted_models = LineModel.sort_series(models, by='tick', ascending=True)
        """
        if not series:
            return []
            
        # 验证排序字段
        valid_fields = ['sid', 'tick', 'open', 'high', 'low', 'close', 'volume', 'amount']
        if by not in valid_fields:
            raise ValueError(f"Invalid sort field: {by}. Must be one of {valid_fields}")
            
        # 使用内置的排序方法
        return sorted(series, key=lambda x: getattr(x, by), reverse=not ascending)


class AdjustmentModel(BaseModel):

    sid: str = Field(default="")
    register_date: int = Field(ge=0)
    ex_date: int = Field(ge=0)
    bonus_share: float = Field(default=0.0)
    transfer: float = Field(default=0.0)
    bonus: float = Field(default=0.0)

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.register_date == _value.register_date
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.register_date < _value.register_date
    
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

    def __eq__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.sid == _value.sid and self.register_date == _value.register_date
    
    def __lt__(self, _value: object) -> bool:
        if not isinstance(_value, self):
            raise TypeError
        return self.register_date < _value.register_date
    
    @field_serializer("price", "ratio")
    def serialize_integer(self, v:float, info):
        return int(v*1000)
