#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import grpc
import logging
from google.protobuf.json_format import MessageToDict

from core.rpc.feed import bt_feed
from bt_protocol.serialize.pb import bt_service_pb2, bt_service_pb2_grpc 

class RpcServer(bt_service_pb2_grpc.btDataFeedServicer):

    def __init__(self):
        self._id_counter = 0

    async def _set_context(self, context: grpc.ServicerContext) -> None:

        # NoCompression / Gzip
        # context is_active to check if the request is cancelled
        # context.set_compression(grpc.Compression.Deflate)
        context.set_trailing_metadata(
            (
                ("checksum-bin", b"I agree"),
                ("retry", "false"),
            )
        )
        
    async def CalendarCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Calendar")

        for key, value in context.invocation_metadata():
            print("Received initial metadata: key=%s value=%s" % (key, value))

        # obj_map = MessageToDict(
        #     request, 
        #     preserving_proto_field_name=True, 
        #     always_print_fields_with_no_presence=True
        # )
        # google._upb._message.RepeatedScalarContainer -> list
        response_iterator = bt_feed.fetch("calendar", request.start_date, request.end_date, list(request.sid))
        async for response in response_iterator:
            # print("CalendarCall repsonse size ", response.ByteSize())
            yield response

    async def InstrumentCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Instrument %s" % request.SerializeToString())

        response_iterator = bt_feed.fetch("asset", request.start_date, request.end_date, list(request.sid)) 

        async for response in response_iterator:
            # print("InstrumentCall repsonse size ", response.ByteSize())
            yield response
    
    async def IndexStreamCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Index")

        response_iterator = bt_feed.fetch("index", request.start_date, request.end_date, list(request.sid))
        async for response in response_iterator:
            # print("IndexStreamCall repsonse size ", response.ByteSize())
            yield response
    
    async def TickStreamCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Tick")

        response_iterator = bt_feed.fetch("tick", request.start_date, request.end_date, list(request.sid))
        async for response in response_iterator:
            # print("LineStreamCall repsonse size ", response.ByteSize())
            yield response

    async def CloseStreamCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Close")

        response_iterator = bt_feed.fetch("close", request.start_date, request.end_date, list(request.sid))
        async for response in response_iterator:
            # print("CloseStreamCall repsonse size ", response.ByteSize())
            yield response

    async def AdjustmentStreamCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Adjustment")

        response_iterator = bt_feed.fetch("adjust", request.start_date, request.end_date, list(request.sid))
        async for response in response_iterator:
            # print("AdjustmentStreamCall repsonse size ", response.ByteSize())
            yield response

    async def RightStreamCall(
        self,
        request: bt_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        # logging.info("Received Right")

        response_iterator = bt_feed.fetch("right", request.start_date, request.end_date, list(request.sid))
        async for response in response_iterator:
            # print("RightStreamCall repsonse size ", response.ByteSize())
            yield response
 