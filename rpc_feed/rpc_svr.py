#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import grpc
import signal
import logging
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor
from google.protobuf.json_format import MessageToDict
from google.protobuf import empty_pb2

from rpc_feed.core.feed import *
from core.datasets.model import Request
from core.rpc.serialize.pb import service_pb2, service_pb2_grpc
from core.middleware.interceptors import RateLimitInterceptor


class RpcServer(service_pb2_grpc.btDataFeedServicer):

    def __init__(self):
        self._id_counter = 0

    async def _set_context(self, context: grpc.ServicerContext) -> None:

        # NoCompression / Gzip
        context.set_compression(grpc.Compression.Deflate)
        context.set_trailing_metadata(
            (
                ("checksum-bin", b"I agree"),
                ("retry", "false"),
            )
        )
        # context is_active to check if the request is cancelled
        
    async def CalendarCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.Calendar: # type: ignore
        
        await self._set_context(context)

        logging.info("Received calendar")

        for key, value in context.invocation_metadata():
            print("Received initial metadata: key=%s value=%s" % (key, value))

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True
        )
        response_iterator = bt_feed("calendar", Request(**obj_map))
        # 
        response = service_pb2.Calendar()
        response.tz_info = "Asia/shanghai"
        trading_days = []
        async for resp in response_iterator:
            trading_days.append(resp["trading_date"])
        response.date.extend(trading_days)
        print("calendar repsonse ", response.ByteSize())
        yield response

    async def InstrumentCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.Calendar: # type: ignore
        
        await self._set_context(context)

        logging.info("Received InstrumentCall %s" % request.SerializeToString())

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("asset", Request(**obj_map))
        # response = service_pb2.InstFrame()
        # assets = []
        # async for resp in response_iterator:
        #     obj = service_pb2.Instrument(**resp)
        #     assets.append(obj)
        # response.asset.extend(assets)
        # print("instrument repsonse ", response.ByteSize())
        # yield response

        async for resp in response_iterator:
            response = service_pb2.InstFrame()
            obj = service_pb2.Instrument(**resp)
            response.asset.extend([obj])
            print("instrument repsonse ", response.ByteSize())
            yield response
    
    async def LineStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.TickFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received dataset")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("line", Request(**obj_map))
        async for resp in response_iterator:
            response = service_pb2.TickFrame()  
            response.sid = resp.pop("sid")
            line = service_pb2.Line(**resp)
            response.line.extend([line])
            print("DatasetStreamCall ticker repsonse size ", response.ByteSize())
            yield response

    async def AdjustmentStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.AdjFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received adjustment")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("adjust", Request(**obj_map))
        async for adjs in response_iterator:
            response = service_pb2.AdjFrame()
            response.date = adjs["ex_date"]
            adjustments = service_pb2.Adjustment(**adjs)
            response.adj.extend([adjustments])
            print("AdjustmentStreamCall ticker repsonse size ", response.ByteSize())
            yield response

    async def RightStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.RightmentFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received right")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("right", Request(**obj_map))
        async for rgts in response_iterator:
            response = service_pb2.RightmentFrame()
            response.date = rgts["ex_date"]
            rights = service_pb2.Rightment(**rgts)
            response.rgt.extend([rights])
            print("RightStreamCall ticker repsonse size ", response.ByteSize())
            yield response
    
  
async def serve() -> None:
    """
    grpc.keepalive_time_ms: The period (in milliseconds) after which a keepalive ping is
        sent on the transport.
    grpc.keepalive_timeout_ms: The amount of time (in milliseconds) the sender of the keepalive
        ping waits for an acknowledgement. If it does not receive an acknowledgment within
        this time, it will close the connection.
    grpc.http2.min_ping_interval_without_data_ms: Minimum allowed time (in milliseconds)
        between a server receiving successive ping frames without sending any data/header frame.
    grpc.max_connection_idle_ms: Maximum time (in milliseconds) that a channel may have no
        outstanding rpcs, after which the server will close the connection.
    grpc.max_connection_age_ms: Maximum time (in milliseconds) that a channel may exist.
    grpc.max_connection_age_grace_ms: Grace period (in milliseconds) after the channel
        reaches its max age.
    grpc.http2.max_pings_without_data: How many pings can the client send before needing to
        send a data/header frame.
    grpc.keepalive_permit_without_calls: If set to 1 (0 : false; 1 : true), allows keepalive
        pings to be sent even if there are no calls in flight.
    For more details, check: https://github.com/grpc/grpc/blob/master/doc/keepalive.md
    """
    address = os.getenv("RPC_FEED_ADDRESS", "localhost:50051")
    MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", 1024 * 1024 * 1024))

    server_options = [
        ("grpc.keepalive_time_ms", 20000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.http2.min_ping_interval_without_data_ms", 5000),
        ("grpc.max_connection_idle_ms", 10000),
        ("grpc.max_connection_age_ms", 30000),
        ("grpc.max_connection_age_grace_ms", 5000),
        ("grpc.http2.max_pings_without_data", 5),
        ("grpc.keepalive_permit_without_calls", 1),
        ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
        ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
    ]

    server = grpc.aio.server(ThreadPoolExecutor(), compression=grpc.Compression.Gzip, 
                             options=server_options, interceptors=[])
    service_pb2_grpc.add_btDataFeedServicer_to_server(RpcServer(), server)
    server.add_insecure_port(address)
    await server.start()
    logging.info("Server serving at %s", address)

    stop_event = asyncio.Event()

    async def shutdown():
        await server.stop(0)    
        stop_event.set()

    loop = asyncio.get_running_loop()
    
    def handle_signal():
        print("Received signal", signal.SIGINT)
        asyncio.create_task(shutdown())

    loop.add_signal_handler(signal.SIGINT, handle_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_signal)

    await stop_event.wait()
    logging.info("Server has been shut down.")

    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())
