# Copyright 2020 The gRPC Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pdb
import grpc
import logging
from typing import Iterator, Iterable
from concurrent.futures import ThreadPoolExecutor
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict
from serialize.pb import service_pb2, service_pb2_grpc


class RpcClient:

    def __init__(self, host="localhost", port=50051, MAX_MESSAGE_LENGTH=1024 * 1024 * 100):
        """
        grpc.keepalive_time_ms: The period (in milliseconds) after which a keepalive ping is
            sent on the transport.
        grpc.keepalive_timeout_ms: The amount of time (in milliseconds) the sender of the keepalive
            ping waits for an acknowledgement. If it does not receive an acknowledgment within this
            time, it will close the connection.
        grpc.keepalive_permit_without_calls: If set to 1 (0 : false; 1 : true), allows keepalive
            pings to be sent even if there are no calls in flight.
        grpc.http2.max_pings_without_data: How many pings can the client send before needing to
            send a data/header frame.
        For more details, check: https://github.com/grpc/grpc/blob/master/doc/keepalive.md
        """
        channel_options = [
        ("grpc.keepalive_time_ms", 8000),
        ("grpc.keepalive_timeout_ms", 5000),
        ("grpc.http2.max_pings_without_data", 5),
        ("grpc.keepalive_permit_without_calls", 1),
        # byte
        ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
        ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
        ]
        _channel = grpc.insecure_channel(f"{host}:{port}", compression=grpc.Compression.Gzip, options=channel_options)
        self._stub = service_pb2_grpc.btSimulatorStub(_channel)
        self._channel = _channel
        self._executor = ThreadPoolExecutor()

    def _stream_response(self, response_iterator, on_callback, stream=True) -> Iterator:
        if not stream:
            response_meta = on_callback(response)
            # print("response_meta")
            return response_meta
        try:
            for response in response_iterator:
                print("_stream_response ", response)
                # NOTE: All fields in Proto3 are optional. This is the recommended way
                # to check if a field is present or not, or to exam which one-of field is
                # fulfilled by this message.
                response_meta = on_callback(response)
                yield response_meta
        except Exception as e:
            print("error ", e)

    def _calendarCall(self, stub_req) -> None:
        # iter((request,))
        # Instead of consuming the response on current thread, spawn a consumption thread.


        def on_handle(meta):
            # RepeatedScalarContainer to list
            metadata = {"tz_info": meta.tz_info, "trading_days": list(meta.date)} 
            return metadata

        request = empty_pb2.Empty()
        response_iterator = iter((self._stub.CalendarCall(request, wait_for_ready=True),))
        # _calendar_future = self._stream_response(response_iterator, on_callback=on_handle) 
        _calendar_future = self._executor.submit(
            self._stream_response, response_iterator, on_handle
        )
        return _calendar_future.result() 

    def _instrumentCall(self, stub_req) -> None:

        def on_handle(meta):
            metadata = [MessageToDict(item) for item in meta.asset]
            return metadata

        response_iterator = iter((self._stub.InstrumentCall(stub_req, wait_for_ready=True),))
        # _instrument_future = self._stream_response(response_iterator, on_callback=on_handle) 
        _instrument_future = self._executor.submit(
            self._stream_response, response_iterator, on_handle
        )
        return _instrument_future.result()

    def _lineCall(self, stub_req) -> None:

        def on_handle(meta):
            lines = [MessageToDict(item) for item in meta.line]
            metadata = {"tick": meta.tick, "line": lines}
            return metadata

        response_iterator = self._stub.DatasetStreamCall(stub_req, wait_for_ready=True)
        # _dataset_future = self._stream_response(response_iterator, on_callback=on_handle) 
        _dataset_future = self._executor.submit(
            self._stream_response, response_iterator, on_handle
        )
        return _dataset_future.result() 

    def _adjustmentCall(self, stub_req) -> None:

        def on_handle(meta):
            adj = [MessageToDict(item) for item in meta.adjustment]
            metadata = {"date": meta.date, "adjustment": adj}
            return metadata

        response_iterator = self._stub.AdjustmentStreamCall(stub_req, wait_for_ready=True)
        # _adjustment_future = self._stream_response(response_iterator, on_callback=on_handle) 
        _adjustment_future = self._executor.submit(
            self._stream_response, response_iterator, on_handle
        )
        return _adjustment_future.result()

    def _rightmentCall(self, stub_req) -> None:

        def on_handle(meta):
            rgts = [MessageToDict(item) for item in meta.right]
            metadata = {"date": meta.date, "right": rgts}
            return metadata

        response_iterator = self._stub.RightStreamCall(stub_req, wait_for_ready=True)
        # _calendar_future = self._stream_response(response_iterator, on_callback=on_handle) 
        _rightment_future = self._executor.submit(
            self._stream_response, response_iterator, on_handle
        )
        return _rightment_future.result()
    
    def on_reflect(self, request, rpc_type):
        internal_call = f"_{rpc_type}Call"
        rpc = getattr(self, internal_call)
        future = rpc(request)
        return future

    def on_exit(self):
        self._channel.close()


rpc_client = RpcClient()
