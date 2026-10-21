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
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


# avoid core.* and rpc_feed.core.* being loaded as two separate instances, 
# which would cause @singleton to split (two sets of AsyncOps / two sets of DuckDB connection pools), 
# and the shutdown resource cleanup would act on the wrong instance, and the actual service connection pool would never be closed.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from rpc_feed.core.rpc.server import RpcServer
from rpc_feed.core.gateway import async_ops, get_duckdb_manager
from rpc_feed.core.rpc.middleware.interceptors import AuthInterceptor, RateLimitInterceptor
from rpc_feed.utils.wrapper import env_int

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
    # intialize postgres
    async with async_ops as ctx: 
        pass

    # initialize duckdb manager
    duck_mgr = get_duckdb_manager()

    # initialize grpc server
    address = os.getenv("GRPC_SERVER")
    if not address:
        raise RuntimeError("GRPC_SERVER env var is not set (expected host:port)")

    MAX_MESSAGE_LENGTH = env_int("MAX_MESSAGE_LENGTH", 64 * 1024 * 1024)
    STREAM_WINDOW_SIZE = env_int("GRPC_STREAM_WINDOW_SIZE", 64 * 1024 * 1024)
    CONNECTION_WINDOW_SIZE = env_int("GRPC_CONNECTION_WINDOW_SIZE", 512 * 1024 * 1024)
    KEEPALIVE_TIME_MS = env_int("GRPC_KEEPALIVE_TIME_MS", 30000)
    KEEPALIVE_TIMEOUT_MS = env_int("GRPC_KEEPALIVE_TIMEOUT_MS", 10000)
    MIN_PING_INTERVAL_MS = env_int("GRPC_MIN_PING_INTERVAL_MS", 5000)
    MAX_CONN_IDLE_MS = env_int("GRPC_MAX_CONN_IDLE_MS", 86400000)
    MAX_CONN_AGE_MS = env_int("GRPC_MAX_CONN_AGE_MS", 86400000)
    MAX_CONN_AGE_GRACE_MS = env_int("GRPC_MAX_CONN_AGE_GRACE_MS", 86400000)
    SHUTDOWN_GRACE = env_int("GRPC_SHUTDOWN_GRACE", 5)

    server_options = [
        ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
        ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),

        # HTTP/2 flow control
        # 64MB transfer >= apply layer otherwise large message will be blocked in HTTP/2 flow control layer
        ("grpc.http2.initial_window_size", STREAM_WINDOW_SIZE),            
        # connection window >= concurrency * stream window 
        ("grpc.http2.initial_connection_window_size", CONNECTION_WINDOW_SIZE), 

        # ⏱ Keepalive
        ("grpc.keepalive_time_ms", KEEPALIVE_TIME_MS),             # Active Ping (30s)
        ("grpc.keepalive_timeout_ms", KEEPALIVE_TIMEOUT_MS),       # Ping Wait 10s
        ("grpc.keepalive_permit_without_calls", 1),    #
        ("grpc.http2.min_ping_interval_without_data_ms", MIN_PING_INTERVAL_MS),  # 5s < client 30s keepalive
        # 0 means abandon 
        ("grpc.http2.max_pings_without_data", 0x7fffffff),
        ("grpc.http2.max_ping_strikes", 0x7fffffff),

        # avoid break idle connection,
        ("grpc.max_connection_idle_ms", MAX_CONN_IDLE_MS),         # 24h
        ("grpc.max_connection_age_ms", MAX_CONN_AGE_MS),           # 24h
        ("grpc.max_connection_age_grace_ms", MAX_CONN_AGE_GRACE_MS),  # 24h
    ]

    # logging.info("server_options (grpc %s):", grpc.__version__)
    # for k, v in server_options:
    #     logging.info("  %s = %r", k, v)

    max_workers = env_int("GRPC_MAX_WORKERS", 16)
    # Arrow IPC payload is already lz4-compressed; a server-wide gzip pass costs
    # CPU for ~zero ratio. Opt in with GRPC_COMPRESSION=gzip if a WAN link needs it.
    compression = (
        grpc.Compression.Gzip
        if os.getenv("GRPC_COMPRESSION", "none").lower() == "gzip"
        else grpc.Compression.NoCompression
    )
    interceptors = []
    if os.getenv("GRPC_ENABLE_AUTH", "0") == "1":
        interceptors.append(AuthInterceptor())
    if os.getenv("GRPC_ENABLE_RATELIMIT", "0") == "1":
        interceptors.append(RateLimitInterceptor())

    server = grpc.aio.server(
        ThreadPoolExecutor(max_workers=max_workers),
        compression=compression,
        options=server_options,
        interceptors=interceptors,
    )
    bt_protocol_service_pb2_grpc.add_btDataFeedServicer_to_server(RpcServer(), server)
    bound_port = server.add_insecure_port(address)
    if bound_port == 0:
        # 0 means bind failure; without this check the server "starts" but never listens
        raise RuntimeError(f"gRPC failed to bind {address!r} (port in use or bad address?)")
    await server.start()
    logging.info("Server serving at %s", address)

    # ---------------------------------------- recycle --------------------------------------

    shutdown_task = None
    stop_event = asyncio.Event()

    async def shutdown(grace: int = SHUTDOWN_GRACE):
        logging.info(f"Stopping server with grace period ({grace}s)...")
        # 1. wait for gRPC/Web Server finish
        try:
            await server.stop(grace=grace)
        except Exception as e:
            logging.warning(f"Server stop error: {e}")

        # 2. release async conn
        logging.info("Cleaning up database connections...")
        try:
            await async_ops.cleanup()
        except Exception as e:
            logging.warning(f"AsyncOps cleanup error: {e}")
            
        try:
            duck_mgr.close()
        except Exception as e:
            logging.warning(f"DuckDB cleanup error: {e}")

        # 3. wait for event
        stop_event.set()

    def handle_signal(sig):
        nonlocal shutdown_task
        sig_name = getattr(sig, 'name', str(sig))
        logging.info(f"Received signal {sig_name}")

        # second time
        if shutdown_task is not None and not shutdown_task.done():
            logging.warning("Forcing immediate shutdown...")
            force_task = asyncio.create_task(server.stop(grace=0))
            return

        # first_time
        shutdown_task = asyncio.create_task(shutdown())

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: handle_signal(signal.SIGINT))
    loop.add_signal_handler(signal.SIGTERM, lambda: handle_signal(signal.SIGTERM))

    await stop_event.wait()
    logging.info("Server has been shut down.")

    await server.wait_for_termination()


@atexit.register
def cleanup_before_exit(): # sys.exit(0)# SystemExit ---> atexit
    sys.stdout.flush()
    sys.stderr.flush()
    gc.collect()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(serve())
