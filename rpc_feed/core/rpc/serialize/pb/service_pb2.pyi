from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Calendar(_message.Message):
    __slots__ = ("tz_info", "date")
    TZ_INFO_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    tz_info: str
    date: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tz_info: _Optional[str] = ..., date: _Optional[_Iterable[int]] = ...) -> None: ...

class Instrument(_message.Message):
    __slots__ = ("sid", "name", "first_trading", "delist")
    SID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FIRST_TRADING_FIELD_NUMBER: _ClassVar[int]
    DELIST_FIELD_NUMBER: _ClassVar[int]
    sid: str
    name: str
    first_trading: int
    delist: int
    def __init__(self, sid: _Optional[str] = ..., name: _Optional[str] = ..., first_trading: _Optional[int] = ..., delist: _Optional[int] = ...) -> None: ...

class Line(_message.Message):
    __slots__ = ("tick", "open", "high", "low", "close", "volume", "amount")
    TICK_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    tick: int
    open: int
    high: int
    low: int
    close: int
    volume: int
    amount: int
    def __init__(self, tick: _Optional[int] = ..., open: _Optional[int] = ..., high: _Optional[int] = ..., low: _Optional[int] = ..., close: _Optional[int] = ..., volume: _Optional[int] = ..., amount: _Optional[int] = ...) -> None: ...

class Adjustment(_message.Message):
    __slots__ = ("sid", "register_date", "bonus_share", "transfer", "bonus")
    SID_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATE_FIELD_NUMBER: _ClassVar[int]
    BONUS_SHARE_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    BONUS_FIELD_NUMBER: _ClassVar[int]
    sid: str
    register_date: int
    bonus_share: int
    transfer: int
    bonus: int
    def __init__(self, sid: _Optional[str] = ..., register_date: _Optional[int] = ..., bonus_share: _Optional[int] = ..., transfer: _Optional[int] = ..., bonus: _Optional[int] = ...) -> None: ...

class Rightment(_message.Message):
    __slots__ = ("sid", "register_date", "price", "ratio")
    SID_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    RATIO_FIELD_NUMBER: _ClassVar[int]
    sid: str
    register_date: int
    price: int
    ratio: int
    def __init__(self, sid: _Optional[str] = ..., register_date: _Optional[int] = ..., price: _Optional[int] = ..., ratio: _Optional[int] = ...) -> None: ...

class InstFrame(_message.Message):
    __slots__ = ("asset",)
    ASSET_FIELD_NUMBER: _ClassVar[int]
    asset: _containers.RepeatedCompositeFieldContainer[Instrument]
    def __init__(self, asset: _Optional[_Iterable[_Union[Instrument, _Mapping]]] = ...) -> None: ...

class TickFrame(_message.Message):
    __slots__ = ("sid", "line")
    SID_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    sid: str
    line: _containers.RepeatedCompositeFieldContainer[Line]
    def __init__(self, sid: _Optional[str] = ..., line: _Optional[_Iterable[_Union[Line, _Mapping]]] = ...) -> None: ...

class AdjFrame(_message.Message):
    __slots__ = ("ex_date", "adj")
    EX_DATE_FIELD_NUMBER: _ClassVar[int]
    ADJ_FIELD_NUMBER: _ClassVar[int]
    ex_date: int
    adj: _containers.RepeatedCompositeFieldContainer[Adjustment]
    def __init__(self, ex_date: _Optional[int] = ..., adj: _Optional[_Iterable[_Union[Adjustment, _Mapping]]] = ...) -> None: ...

class RightmentFrame(_message.Message):
    __slots__ = ("ex_date", "rgt")
    EX_DATE_FIELD_NUMBER: _ClassVar[int]
    RGT_FIELD_NUMBER: _ClassVar[int]
    ex_date: int
    rgt: _containers.RepeatedCompositeFieldContainer[Rightment]
    def __init__(self, ex_date: _Optional[int] = ..., rgt: _Optional[_Iterable[_Union[Rightment, _Mapping]]] = ...) -> None: ...

class QuoteRequest(_message.Message):
    __slots__ = ("start_date", "end_date", "sid")
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    start_date: int
    end_date: int
    sid: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, start_date: _Optional[int] = ..., end_date: _Optional[int] = ..., sid: _Optional[_Iterable[str]] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ("status", "error")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    status: int
    error: str
    def __init__(self, status: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...
