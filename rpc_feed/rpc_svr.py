#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import grpc
import signal
import logging
import threading
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor
from google.protobuf.json_format import MessageToDict
from google.protobuf import empty_pb2
from core.rpc.serialize.pb import service_pb2, service_pb2_grpc
from feed import *
from core.datasets.model import Request


class QuoteServer(service_pb2_grpc.btDataFeedServicer):

    def __init__(self):
        self._id_counter = 0
        self._lock = threading.RLock()

    # def _clean_call_session(self, call_info: service_pb2.CallInfo) -> None:
    #     logging.info("Call session cleaned [%s]", MessageToJson(call_info))
        
    async def CalendarCall(
        self,
        # request: empty_pb2.Empty,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.Calendar: # type: ignore
        
        # try:
        #     request = next(request_iterator)
        #     logging.info(
        #         "Received a phone call request for number [%s]",
        #         request.phone_number,
        #     )
        # except StopIteration:
        #     raise RuntimeError("Failed to receive call request")
        
        # context.set_compression(grpc.Compression.NoCompression)
        logging.info("Received calendar")

        for key, value in context.invocation_metadata():
            print("Received initial metadata: key=%s value=%s" % (key, value))

        context.set_trailing_metadata(
            (
                ("checksum-bin", b"I agree"),
                ("retry", "false"),
            )
        )

        # context.add_callback(lambda: self._clean_call_session(call_info))
        # trading_days = [c.trading_date for c in bt_feed.get] 
        response = service_pb2.Calendar()
        response.tz_info = "Asia/shanghai"
        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True
        )
        print("obj_map ", obj_map)
        response_iterator = bt_feed.replay("calendar", Request(**obj_map))
        # import pdb; pdb.set_trace()
        trading_days = []
        async for resp in response_iterator:
            print("resp ", resp)
            trading_days.append(resp.trading_date)
        response.date.extend(trading_days)
        print("calendar repsonse ", response)
        yield response

    async def InstrumentCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.Calendar: # type: ignore
        
        logging.info("Received InstrumentCall %s" % request.SerializeToString())

        response = service_pb2.InstFrame()
        # context.add_callback(lambda: self._clean_call_session(call_info))
        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed.replay("asset", Request(**obj_map))
        assets = []
        async for resp in response_iterator:
            print("resp ", resp)
            obj = service_pb2.Instrument(**resp)
            print("obj ", obj)
            assets.append(obj)
        response.asset.extend(assets)
        print("instrument repsonse ", response)
        print("InstrumentCall repsonse size ", response.ByteSize())
        yield response
    
    async def LineStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.TickFrame: # type: ignore
        
        logging.info("Received dataset")
        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed.replay("line", Request(**obj_map))
        # context.add_callback(lambda: self._clean_call_session(call_info))
        async for resp in response_iterator:
            # pdb.set_trace()
            response = service_pb2.TickFrame()  
            # response.tick = resp.pop("tick")
            response.sid = resp.pop("sid")
            line = service_pb2.Line(**resp)
            response.line.extend([line])
            print("dataset repsonse ", response)
            print("DatasetStreamCall ticker repsonse size ", response.ByteSize())
            yield response

    async def AdjustmentStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.AdjFrame: # type: ignore
        
        # context.set_compression(grpc.Compression.NoCompression)
        logging.info("Received adjustment")
        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed.replay("adjustment", Request(**obj_map))
        # context.add_callback(lambda: self._clean_call_session(call_info))
        async for adjs in response_iterator:
            response = service_pb2.AdjFrame()
            response.date = adjs["ex_date"]
            adjustments = service_pb2.Adjustment(**adjs)
            response.adj.extend([adjustments])
            print("adjustment repsonse ", response)
            print("AdjustmentStreamCall ticker repsonse size ", response.ByteSize())
            yield response

    async def RightStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.RightmentFrame: # type: ignore
        
        logging.info("Received right")
        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed.replay("right", Request(**obj_map))
        # context.add_callback(lambda: self._clean_call_session(call_info))
        async for rgts in response_iterator:
            response = service_pb2.RightmentFrame()
            response.date = rgts["ex_date"]
            rights = service_pb2.Rightment(**rgts)
            response.rgt.extend([rights])
            print("rightment repsonse ", response)
            print("RightStreamCall ticker repsonse size ", response.ByteSize())
            yield response

  
async def serve(address: str, MAX_MESSAGE_LENGTH=1024 * 1024 * 1024) -> None:
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

    server = grpc.aio.server(ThreadPoolExecutor(), compression=grpc.Compression.Gzip, options=server_options)
    service_pb2_grpc.add_btDataFeedServicer_to_server(QuoteServer(), server)
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
    # loop = asyncio.get_event_loop()
    # loop.add_signal_handler(signal.SIGINT, handler)
    asyncio.run(serve("localhost:50051"))
