#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '0' 

from dotenv import load_dotenv

load_dotenv()

import sys
import gc
import atexit
import asyncio
import grpc
import signal
import logging
import uvloop
from concurrent.futures import ThreadPoolExecutor
from core.rpc.server import RpcServer
from core.gateway import async_ops

from bt_protocol.serialize.pb import bt_protocol_service_pb2_grpc


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

    async with async_ops as ctx: 
        pass

    address = os.getenv("GRPC_SERVER")
    MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", 512 * 1024 * 1024))

    server_options = [
        # message 512MB same as client
        ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
        ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),

        # tcp stream control http2 
        ("grpc.http2.initial_window_size", 64 * 1024 * 1024),       # 64MB Stream Window
        ("grpc.http2.initial_connection_window_size", 128 * 1024 * 1024), # 128MB Connection Window 

        # ⏱ Keepalive 
        ("grpc.keepalive_time_ms", 30000),             # Active Ping (30s)
        ("grpc.keepalive_timeout_ms", 10000),          # Ping Wait 10s
        ("grpc.keepalive_permit_without_calls", 1),    # 
        ("grpc.http2.min_time_between_pings_ms", 10000),# 10s Client Ping
        ("grpc.http2.max_pings_without_data", 0),      # ideal Ping 

        # avoid break idle connection,
        ("grpc.max_connection_idle_ms", 86400000),       # 24h
        ("grpc.max_connection_age_ms", 86400000),        # 24h
        ("grpc.max_connection_age_grace_ms", 86400000),  # 24h
    ]

    server = grpc.aio.server(ThreadPoolExecutor(), compression=grpc.Compression.Gzip, 
                             options=server_options, interceptors=[])
    bt_protocol_service_pb2_grpc.add_btDataFeedServicer_to_server(RpcServer(), server)
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


@atexit.register
def cleanup_before_exit(): # sys.exit(0)# SystemExit ---> atexit
    
    sys.stdout.flush()
    sys.stderr.flush()

    print("gc atexit") 
    gc.collect()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(serve())
