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
from typing import Iterator, Iterable, AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict
from core.serialize.pb import service_pb2_grpc


class RpcClient:

    def __init__(self):
        self._channel = None
        self._stub = None
        # self._executor = ThreadPoolExecutor()
        
    async def initialize(self, host="localhost", port=50051, MAX_MESSAGE_LENGTH=1024 * 1024 * 100):
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
        if self._channel is not None:
            return
            
        channel_options = [
            ("grpc.keepalive_time_ms", 8000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
        ]
        
        self._channel = grpc.aio.insecure_channel(
            f"{host}:{port}",
            compression=grpc.Compression.Gzip,
            options=channel_options
        )
        self._stub = service_pb2_grpc.btDataFeedStub(self._channel)

    async def ensure_initialized(self):
        if self._channel is None:
            await self.initialize()

    async def _stream_response(self, response_iterator, on_callback):
        try:
            async for response in response_iterator:
                print("_stream_response ", response)
                # NOTE: All fields in Proto3 are optional. This is the recommended way
                # to check if a field is present or not, or to exam which one-of field is
                # fulfilled by this message.
                response_meta = on_callback(response)
                yield response_meta
        except Exception as e:
            print("error ", e)
    
    def _get_handler(self, rpc_type):
        """Returns the appropriate handler function for the RPC type"""
        def calendar_handler(meta):
            return {"tz_info": meta.tz_info, "trading_days": list(meta.date)}
            
        def instrument_handler(meta):
            return [MessageToDict(item) for item in meta.asset]
            
        def dataset_handler(meta):
            lines = [MessageToDict(item) for item in meta.line]
            return {"tick": meta.tick, "line": lines}
        
        def adjustment_handler(meta):
            adj = [MessageToDict(item) for item in meta.adjustment]
            return {"date": meta.date, "adjustment": adj}
        
        def right_handler(meta):
            rgts = [MessageToDict(item) for item in meta.right]
            return {"date": meta.date, "right": rgts}
            
        handlers = {
            "calendar": calendar_handler,
            "instrument": instrument_handler,
            "dataset": dataset_handler,
            "adjustment": adjustment_handler,
            "right": right_handler
        }
        return handlers.get(rpc_type)

    async def _calendarCall(self, stub_req) -> AsyncIterator[dict]:
        # iter((request,))
        # request = empty_pb2.Empty()
        print("stub_req ", stub_req)
        response_iterator = self._stub.CalendarCall(stub_req, wait_for_ready=True)
        
        # Instead of consuming the response on current thread, spawn a consumption thread.
        # _calendar_future = self._executor.submit(
        #     self._stream_response, response_iterator, on_handle
        # )
        # return _calendar_future.result()
        on_handle = self._get_handler("calendar")
        
        async for response in response_iterator:
            print("response ", response)
            yield on_handle(response)
    
    async def _instrumentCall(self, stub_req) -> AsyncIterator[dict]:

        def on_handle(meta):
            metadata = [MessageToDict(item) for item in meta.asset]
            return metadata

        response_iterator = self._stub.InstrumentCall(stub_req, wait_for_ready=True)
        # _instrument_future = self._executor.submit(
        #     self._stream_response, response_iterator, on_handle
        # )
        # return _instrument_future.result()
        on_handle = self._get_handler("instrument")
        
        async for response in response_iterator:
            print("response ", response)
            yield on_handle(response)

    async def _tickCall(self, stub_req) -> AsyncIterator[dict]:

        def on_handle(meta):
            lines = [MessageToDict(item) for item in meta.line]
            metadata = {"tick": meta.tick, "line": lines}
            return metadata

        response_iterator = self._stub.LineStreamCall(stub_req, wait_for_ready=True)
        # _dataset_future = self._stream_response(response_iterator, on_callback=on_handle) 
        on_handle = self._get_handler("dataset")
        async for response in response_iterator:
            print("response ", response)
            yield on_handle(response)
        # _dataset_future = self._executor.submit(
        #     self._stream_response, response_iterator, on_handle
        # )
        # return _dataset_future.result() 

    async def _adjustmentCall(self, stub_req) -> AsyncIterator[dict]:

        def on_handle(meta):
            adj = [MessageToDict(item) for item in meta.adjustment]
            metadata = {"date": meta.date, "adjustment": adj}
            return metadata

        response_iterator = self._stub.AdjustmentStreamCall(stub_req, wait_for_ready=True)
        # _adjustment_future = self._stream_response(response_iterator, on_callback=on_handle) 
        on_handle = self._get_handler("adjustment")
        async for response in response_iterator:
            print("response ", response)
            yield on_handle(response)
        # _adjustment_future = self._executor.submit(
        #     self._stream_response, response_iterator, on_handle
        # )
        # return _adjustment_future.result()

    async def _rightmentCall(self, stub_req) -> AsyncIterator[dict]:

        def on_handle(meta):
            rgts = [MessageToDict(item) for item in meta.right]
            metadata = {"date": meta.date, "right": rgts}
            return metadata

        response_iterator = self._stub.RightStreamCall(stub_req, wait_for_ready=True)
        # _calendar_future = self._stream_response(response_iterator, on_callback=on_handle) 
        on_handle = self._get_handler("right")
        async for response in response_iterator:
            print("response ", response)
            yield on_handle(response)
        # _rightment_future = self._executor.submit(
        #     self._stream_response, response_iterator, on_handle
        # )
        # return _rightment_future.result()
    
    async def on_delegate(self, request, rpc_type):

        # initialize channel in event loop
        await self.ensure_initialized()

        internal_call = f"_{rpc_type}Call"
        rpc = getattr(self, internal_call)
        # response_iterator = rpc(request)
        # async for response in self._stream_response(response_iterator, on_callback=self._get_handler(rpc_type)):
        #     print("response ", response)
        #     yield response
        async for response in rpc(request):
            print("on_delegate response ", response)
            # Convert response to dictionary before yielding
            yield response
    
    def on_exit(self):
        self._channel.close()


rpc_client = RpcClient()
