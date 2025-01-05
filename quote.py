#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import logging
import socket
import signal
import pickle
import asyncio
from core.rpc.client import rpc_client
from core.serialize.pb import service_pb2


class EchoServerProtocol:

    def __init__(self, chunk_size) -> None:
        self.chunk_size = 1024

    def connection_made(self, transport):
        self.transport = transport
        self.checksum = bytes("sentinel", 'utf-8')
    
    def datagram_received(self, data, addr):
        loop = asyncio.get_running_loop()
        loop.create_task(self.async_handle_received(data, addr))

    async def async_handle_received(self, data, addr):
        # message = data.decode()
        # print("udp receive data ", message)
        # print('Received %r from %s' % (message, addr))
        # print('Send %r to %s' % (message, addr))
        # data = self.request[0].strip()
        req_map = pickle.loads(data)
        # parse req
        rpc_type = req_map.pop("rpc_type")
        meta = req_map["meta"]
        request = service_pb2.QuoteRequest(**meta)
        print("request ", request)
        response_iterator = rpc_client.on_delegate(request=request, rpc_type=rpc_type)
        async for res in response_iterator:
            print("res ", res)
            serialize = pickle.dumps(res)
            for idx in range(0, len(serialize), self.chunk_size):
                self.transport.sendto(serialize[idx: idx+self.chunk_size], addr)
            self.transport.sendto(self.checksum, addr)
            print("send serialize ", len(serialize))
        print("send shutdown to client")
        self.transport.sendto(bytes("shutdown", 'utf-8') , addr)
    
    def connection_lost(self, exc):
        print("UDP server connection lost.")
        # Perform any necessary cleanup here
        # super().connection_lost(exc)

async def main():
    while True:
        print("Starting UDP server")

        # Get a reference to the event loop as we plan to use
        # low-level APIs.
        loop = asyncio.get_running_loop()
        # 
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 9999))

        # One protocol instance will be created to serve all
        # client requests.
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: EchoServerProtocol(1024),
            sock=s)
        
        # set event handle
        stop_event = asyncio.Event()
 
        async def shutdown():
            print("Received signal to stop")
            transport.close()
            print("transport close")
            stop_event.set()
            print("stop event set")
            sys.exit(0)

        def handle_signal():
            print("ctrl + c handler and value", signal.SIGINT.value)
            task = asyncio.create_task(shutdown())
            task.add_done_callback(lambda t: logging.error(f"Shutdown task failed: {t.exception()}") if t.exception() else None)
            
        loop.add_signal_handler(signal.SIGINT, handle_signal)
        loop.add_signal_handler(signal.SIGTERM, handle_signal)

        await stop_event.wait()


if __name__ == "__main__":
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle Ctrl + C on Windows or when signal handlers aren't set
        print("Received KeyboardInterrupt. Shutting down server...")
        # Since `asyncio.run()` will handle shutdown, no additional actions are needed here
    except Exception as e:
        print(f"Server encountered an error: {e}")