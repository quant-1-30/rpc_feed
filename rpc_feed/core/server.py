#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import grpc
import logging
from google.protobuf.json_format import MessageToDict

from rpc_feed.core.feed import *
from core.datasets.model import Request
from core.rpc.serialize.pb import service_pb2, service_pb2_grpc


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

        logging.info("Received Calendar")

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
        print("CalendarCall repsonse size ", response.ByteSize())
        yield response

    async def InstrumentCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.Calendar: # type: ignore
        
        await self._set_context(context)

        logging.info("Received Instrument %s" % request.SerializeToString())

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("asset", Request(**obj_map))

        async for resp in response_iterator:
            response = service_pb2.InstFrame()
            obj = service_pb2.Instrument(**resp)
            response.asset.extend([obj])
            print("InstrumentCall repsonse size ", response.ByteSize())
            yield response
    
    async def IndexStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.DailyFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received Index")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("index", Request(**obj_map))
        async for resp in response_iterator:
            response = service_pb2.DailyFrame()  
            response.sid = resp.pop("sid")
            print("resp " , resp)
            line = service_pb2.Daily(**resp)
            response.line.extend([line])
            print("IndexStreamCall repsonse size ", response.ByteSize())
            yield response
    
    async def TickStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.TickFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received Line")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("tick", Request(**obj_map))
        async for resp in response_iterator:
            response = service_pb2.TickFrame()  
            response.sid = resp.pop("sid")
            print("resp " , resp)
            line = service_pb2.Line(**resp)
            response.line.extend([line])
            print("LineStreamCall repsonse size ", response.ByteSize())
            yield response

    async def CloseStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.CloseFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received Close")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("close", Request(**obj_map))
        async for resp in response_iterator:
            response = service_pb2.CloseFrame()  
            response.sid = resp.pop("sid")
            close = service_pb2.Close(**resp)
            response.close.extend([close])
            print("CloseStreamCall repsonse size ", response.ByteSize())
            yield response

    async def AdjustmentStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.AdjFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received Adjustment")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("adjust", Request(**obj_map))
        async for adjs in response_iterator:
            response = service_pb2.AdjFrame()
            response.ex_date = adjs.pop("ex_date")
            # import pdb; pdb.set_trace()
            adjustments = service_pb2.Adjustment(**adjs)
            response.adj.extend([adjustments])
            print("AdjustmentStreamCall repsonse size ", response.ByteSize())
            yield response

    async def RightStreamCall(
        self,
        request: service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> service_pb2.RightmentFrame: # type: ignore
        
        await self._set_context(context)

        logging.info("Received Right")

        obj_map = MessageToDict(
            request, 
            preserving_proto_field_name=True, 
            always_print_fields_with_no_presence=True,
        )
        response_iterator = bt_feed("right", Request(**obj_map))
        async for rgts in response_iterator:
            response = service_pb2.RightmentFrame()
            response.ex_date = rgts.pop("ex_date")
            rights = service_pb2.Rightment(**rgts)
            response.rgt.extend([rights])
            print("RightStreamCall repsonse size ", response.ByteSize())
            yield response
 