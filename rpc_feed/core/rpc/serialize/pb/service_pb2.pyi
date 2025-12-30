from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DailyFrame(_message.Message):
    __slots__ = ("sid", "date", "open", "high", "low", "close", "volume", "amount")
    SID_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    sid: bytes
    date: _containers.RepeatedScalarFieldContainer[int]
    open: _containers.RepeatedScalarFieldContainer[int]
    high: _containers.RepeatedScalarFieldContainer[int]
    low: _containers.RepeatedScalarFieldContainer[int]
    close: _containers.RepeatedScalarFieldContainer[int]
    volume: _containers.RepeatedScalarFieldContainer[int]
    amount: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, sid: _Optional[bytes] = ..., date: _Optional[_Iterable[int]] = ..., open: _Optional[_Iterable[int]] = ..., high: _Optional[_Iterable[int]] = ..., low: _Optional[_Iterable[int]] = ..., close: _Optional[_Iterable[int]] = ..., volume: _Optional[_Iterable[int]] = ..., amount: _Optional[_Iterable[int]] = ...) -> None: ...

class TickFrame(_message.Message):
    __slots__ = ("sid", "tick", "open", "high", "low", "close", "volume", "amount")
    SID_FIELD_NUMBER: _ClassVar[int]
    TICK_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    sid: bytes
    tick: _containers.RepeatedScalarFieldContainer[int]
    open: _containers.RepeatedScalarFieldContainer[int]
    high: _containers.RepeatedScalarFieldContainer[int]
    low: _containers.RepeatedScalarFieldContainer[int]
    close: _containers.RepeatedScalarFieldContainer[int]
    volume: _containers.RepeatedScalarFieldContainer[int]
    amount: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, sid: _Optional[bytes] = ..., tick: _Optional[_Iterable[int]] = ..., open: _Optional[_Iterable[int]] = ..., high: _Optional[_Iterable[int]] = ..., low: _Optional[_Iterable[int]] = ..., close: _Optional[_Iterable[int]] = ..., volume: _Optional[_Iterable[int]] = ..., amount: _Optional[_Iterable[int]] = ...) -> None: ...

class CloseFrame(_message.Message):
    __slots__ = ("sid", "date", "close")
    SID_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    sid: bytes
    date: _containers.RepeatedScalarFieldContainer[int]
    close: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, sid: _Optional[bytes] = ..., date: _Optional[_Iterable[int]] = ..., close: _Optional[_Iterable[int]] = ...) -> None: ...

class AdjFrame(_message.Message):
    __slots__ = ("sid", "ex_date", "register_date", "bonus_share", "transfer", "bonus")
    SID_FIELD_NUMBER: _ClassVar[int]
    EX_DATE_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATE_FIELD_NUMBER: _ClassVar[int]
    BONUS_SHARE_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    BONUS_FIELD_NUMBER: _ClassVar[int]
    sid: bytes
    ex_date: _containers.RepeatedScalarFieldContainer[int]
    register_date: _containers.RepeatedScalarFieldContainer[int]
    bonus_share: _containers.RepeatedScalarFieldContainer[int]
    transfer: _containers.RepeatedScalarFieldContainer[int]
    bonus: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, sid: _Optional[bytes] = ..., ex_date: _Optional[_Iterable[int]] = ..., register_date: _Optional[_Iterable[int]] = ..., bonus_share: _Optional[_Iterable[int]] = ..., transfer: _Optional[_Iterable[int]] = ..., bonus: _Optional[_Iterable[int]] = ...) -> None: ...

class RightmentFrame(_message.Message):
    __slots__ = ("sid", "ex_date", "register_date", "price", "ratio")
    SID_FIELD_NUMBER: _ClassVar[int]
    EX_DATE_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    RATIO_FIELD_NUMBER: _ClassVar[int]
    sid: bytes
    ex_date: _containers.RepeatedScalarFieldContainer[int]
    register_date: _containers.RepeatedScalarFieldContainer[int]
    price: _containers.RepeatedScalarFieldContainer[int]
    ratio: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, sid: _Optional[bytes] = ..., ex_date: _Optional[_Iterable[int]] = ..., register_date: _Optional[_Iterable[int]] = ..., price: _Optional[_Iterable[int]] = ..., ratio: _Optional[_Iterable[int]] = ...) -> None: ...

class InstFrame(_message.Message):
    __slots__ = ("sid", "name", "first_trading", "delist")
    SID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FIRST_TRADING_FIELD_NUMBER: _ClassVar[int]
    DELIST_FIELD_NUMBER: _ClassVar[int]
    sid: _containers.RepeatedScalarFieldContainer[bytes]
    name: _containers.RepeatedScalarFieldContainer[bytes]
    first_trading: _containers.RepeatedScalarFieldContainer[int]
    delist: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, sid: _Optional[_Iterable[bytes]] = ..., name: _Optional[_Iterable[bytes]] = ..., first_trading: _Optional[_Iterable[int]] = ..., delist: _Optional[_Iterable[int]] = ...) -> None: ...

class Calendar(_message.Message):
    __slots__ = ("tz_info", "date")
    TZ_INFO_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    tz_info: bytes
    date: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tz_info: _Optional[bytes] = ..., date: _Optional[_Iterable[int]] = ...) -> None: ...

class ArrowPayload(_message.Message):
    __slots__ = ("sid", "schema_type", "ipc_blob")
    SID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_TYPE_FIELD_NUMBER: _ClassVar[int]
    IPC_BLOB_FIELD_NUMBER: _ClassVar[int]
    sid: bytes
    schema_type: str
    ipc_blob: bytes
    def __init__(self, sid: _Optional[bytes] = ..., schema_type: _Optional[str] = ..., ipc_blob: _Optional[bytes] = ...) -> None: ...

class QuoteRequest(_message.Message):
    __slots__ = ("start_date", "end_date", "sid")
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    start_date: int
    end_date: int
    sid: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, start_date: _Optional[int] = ..., end_date: _Optional[int] = ..., sid: _Optional[_Iterable[bytes]] = ...) -> None: ...
