from google.protobuf import empty_pb2 as _empty_pb2
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
    __slots__ = ("sid", "open", "high", "low", "close", "volume", "amount")
    SID_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    sid: str
    open: int
    high: int
    low: int
    close: int
    volume: int
    amount: int
    def __init__(self, sid: _Optional[str] = ..., open: _Optional[int] = ..., high: _Optional[int] = ..., low: _Optional[int] = ..., close: _Optional[int] = ..., volume: _Optional[int] = ..., amount: _Optional[int] = ...) -> None: ...

class Adjustment(_message.Message):
    __slots__ = ("sid", "register_date", "ex_date", "share", "transfer", "interest")
    SID_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATE_FIELD_NUMBER: _ClassVar[int]
    EX_DATE_FIELD_NUMBER: _ClassVar[int]
    SHARE_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    INTEREST_FIELD_NUMBER: _ClassVar[int]
    sid: str
    register_date: int
    ex_date: int
    share: int
    transfer: int
    interest: int
    def __init__(self, sid: _Optional[str] = ..., register_date: _Optional[int] = ..., ex_date: _Optional[int] = ..., share: _Optional[int] = ..., transfer: _Optional[int] = ..., interest: _Optional[int] = ...) -> None: ...

class Rightment(_message.Message):
    __slots__ = ("sid", "register_date", "ex_date", "price", "ratio")
    SID_FIELD_NUMBER: _ClassVar[int]
    REGISTER_DATE_FIELD_NUMBER: _ClassVar[int]
    EX_DATE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    RATIO_FIELD_NUMBER: _ClassVar[int]
    sid: str
    register_date: int
    ex_date: int
    price: int
    ratio: int
    def __init__(self, sid: _Optional[str] = ..., register_date: _Optional[int] = ..., ex_date: _Optional[int] = ..., price: _Optional[int] = ..., ratio: _Optional[int] = ...) -> None: ...

class Order(_message.Message):
    __slots__ = ("sid", "order_id", "order_type", "created_at", "order_price", "order_volume")
    SID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_TYPE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    ORDER_PRICE_FIELD_NUMBER: _ClassVar[int]
    ORDER_VOLUME_FIELD_NUMBER: _ClassVar[int]
    sid: str
    order_id: str
    order_type: str
    created_at: int
    order_price: int
    order_volume: int
    def __init__(self, sid: _Optional[str] = ..., order_id: _Optional[str] = ..., order_type: _Optional[str] = ..., created_at: _Optional[int] = ..., order_price: _Optional[int] = ..., order_volume: _Optional[int] = ...) -> None: ...

class Transaction(_message.Message):
    __slots__ = ("sid", "created_at", "trade_price", "market_price", "volume", "cost")
    SID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    TRADE_PRICE_FIELD_NUMBER: _ClassVar[int]
    MARKET_PRICE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    sid: str
    created_at: int
    trade_price: int
    market_price: int
    volume: int
    cost: int
    def __init__(self, sid: _Optional[str] = ..., created_at: _Optional[int] = ..., trade_price: _Optional[int] = ..., market_price: _Optional[int] = ..., volume: _Optional[int] = ..., cost: _Optional[int] = ...) -> None: ...

class Experiment(_message.Message):
    __slots__ = ("user_id", "experiment_id", "account_id")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    experiment_id: str
    account_id: str
    def __init__(self, user_id: _Optional[str] = ..., experiment_id: _Optional[str] = ..., account_id: _Optional[str] = ...) -> None: ...

class Account(_message.Message):
    __slots__ = ("date", "positions", "portfolio", "cash")
    DATE_FIELD_NUMBER: _ClassVar[int]
    POSITIONS_FIELD_NUMBER: _ClassVar[int]
    PORTFOLIO_FIELD_NUMBER: _ClassVar[int]
    CASH_FIELD_NUMBER: _ClassVar[int]
    date: int
    positions: str
    portfolio: int
    cash: int
    def __init__(self, date: _Optional[int] = ..., positions: _Optional[str] = ..., portfolio: _Optional[int] = ..., cash: _Optional[int] = ...) -> None: ...

class InstFrame(_message.Message):
    __slots__ = ("asset",)
    ASSET_FIELD_NUMBER: _ClassVar[int]
    asset: _containers.RepeatedCompositeFieldContainer[Instrument]
    def __init__(self, asset: _Optional[_Iterable[_Union[Instrument, _Mapping]]] = ...) -> None: ...

class TickerFrame(_message.Message):
    __slots__ = ("ticker", "line")
    TICKER_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    ticker: int
    line: _containers.RepeatedCompositeFieldContainer[Line]
    def __init__(self, ticker: _Optional[int] = ..., line: _Optional[_Iterable[_Union[Line, _Mapping]]] = ...) -> None: ...

class AdjFrame(_message.Message):
    __slots__ = ("date", "adj")
    DATE_FIELD_NUMBER: _ClassVar[int]
    ADJ_FIELD_NUMBER: _ClassVar[int]
    date: int
    adj: _containers.RepeatedCompositeFieldContainer[Adjustment]
    def __init__(self, date: _Optional[int] = ..., adj: _Optional[_Iterable[_Union[Adjustment, _Mapping]]] = ...) -> None: ...

class RightmentFrame(_message.Message):
    __slots__ = ("date", "rgt")
    DATE_FIELD_NUMBER: _ClassVar[int]
    RGT_FIELD_NUMBER: _ClassVar[int]
    date: int
    rgt: _containers.RepeatedCompositeFieldContainer[Rightment]
    def __init__(self, date: _Optional[int] = ..., rgt: _Optional[_Iterable[_Union[Rightment, _Mapping]]] = ...) -> None: ...

class OrderFrame(_message.Message):
    __slots__ = ("date", "ord")
    DATE_FIELD_NUMBER: _ClassVar[int]
    ORD_FIELD_NUMBER: _ClassVar[int]
    date: int
    ord: _containers.RepeatedCompositeFieldContainer[Order]
    def __init__(self, date: _Optional[int] = ..., ord: _Optional[_Iterable[_Union[Order, _Mapping]]] = ...) -> None: ...

class TransactionFrame(_message.Message):
    __slots__ = ("date", "txn")
    DATE_FIELD_NUMBER: _ClassVar[int]
    TXN_FIELD_NUMBER: _ClassVar[int]
    date: int
    txn: _containers.RepeatedCompositeFieldContainer[Transaction]
    def __init__(self, date: _Optional[int] = ..., txn: _Optional[_Iterable[_Union[Transaction, _Mapping]]] = ...) -> None: ...

class AccountFrame(_message.Message):
    __slots__ = ("date", "account")
    DATE_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    date: int
    account: _containers.RepeatedCompositeFieldContainer[Account]
    def __init__(self, date: _Optional[int] = ..., account: _Optional[_Iterable[_Union[Account, _Mapping]]] = ...) -> None: ...

class QuoteRequest(_message.Message):
    __slots__ = ("start_date", "end_date", "sid")
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    start_date: int
    end_date: int
    sid: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, start_date: _Optional[int] = ..., end_date: _Optional[int] = ..., sid: _Optional[_Iterable[str]] = ...) -> None: ...

class TradeRequest(_message.Message):
    __slots__ = ("start_date", "end_date", "sid", "experiment")
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    start_date: int
    end_date: int
    sid: _containers.RepeatedScalarFieldContainer[str]
    experiment: Experiment
    def __init__(self, start_date: _Optional[int] = ..., end_date: _Optional[int] = ..., sid: _Optional[_Iterable[str]] = ..., experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class PersistRequest(_message.Message):
    __slots__ = ("body", "experiment")
    class BodyEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    BODY_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    body: _containers.ScalarMap[str, bytes]
    experiment: Experiment
    def __init__(self, body: _Optional[_Mapping[str, bytes]] = ..., experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ("status", "error")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    status: int
    error: str
    def __init__(self, status: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...
