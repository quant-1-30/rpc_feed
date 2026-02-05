from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class QuoteRequest(_message.Message):
    __slots__ = ("start_date", "end_date", "sid")
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    SID_FIELD_NUMBER: _ClassVar[int]
    start_date: int
    end_date: int
    sid: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, start_date: _Optional[int] = ..., end_date: _Optional[int] = ..., sid: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ArrowFrame(_message.Message):
    __slots__ = ("payload",)
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    def __init__(self, payload: _Optional[bytes] = ...) -> None: ...
