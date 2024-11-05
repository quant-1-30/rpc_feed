#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket
import sys
import signal
import atexit
import pdb
import pickle
import asyncio
import socketserver
from core.rpc.client import rpc_client
from core.serialize.pb import service_pb2


# class MyUDPHandler(socketserver.BaseRequestHandler):
#     """
#     This class works similar to the TCP handler class, except that
#     self.request consists of a pair of data and client socket, and since
#     there is no connection the client address must be given explicitly
#     when sending data back via sendto().
#     """
#     chunk_size = 1024

#     def __init__(self, request, client_address, server):
#         super().__init__(request, client_address, server)
    

#     def handle(self):
#         data = self.request[0].strip()
#         req_map = pickle.loads(data)
#         print("udp receive data ", data)
#         # parse req
#         rpc_type = req_map.pop("rpc_type")
#         meta = req_map["meta"]
#         request = service_pb2.QuoteRequest(**meta)
#         # cur_thread = threading.current_thread()
#         # response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')
#         response_iterator = rpc_client.on_delegate(request=request, rpc_type=rpc_type)
#         socket = self.request[1]
#         print("{} wrote:".format(self.client_address[0]))
#         checksum = bytes("sentinel", 'utf-8')
#         # pdb.set_trace()

#         for res in response_iterator:
#             serialize = pickle.dumps(res)
#             for idx in range(0, len(serialize), self.chunk_size):
#                 socket.sendto(serialize[idx: idx+self.chunk_size], self.client_address)
#             socket.sendto(checksum, self.client_address)
#             print("send serialize ", len(serialize))
#         print("send shutdown to client")
#         socket.sendto(bytes("shutdown", 'utf-8') , self.client_address)


# def exit_handler(server):
#     server.socket.close()

# def quit_handler(server):
#     server.socket.close()

# def on_handler(signum, frame):
#     print("ctrl + c handler and value", signal.SIGINT.value)
#     sys.exit(0)


# # ctrl + c
# signal.signal(signal.SIGINT, on_handler)

# if __name__ == "__main__":
#     # HOST, PORT = "localhost", 9999
#     HOST, PORT = "127.0.0.1", 9999
#     # with socketserver.UDPServer((HOST, PORT), MyUDPHandler) as server:
#     with socketserver.ThreadingUDPServer((HOST, PORT), MyUDPHandler) as server:
#     # sock = server.socket
#     # getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
#     # getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
#     # Exit the server thread when the main thread terminates
#         server.serve_forever()
#         atexit.register(quit_handler, server)

#         ip, port = server.server_address
#         # Start a thread with the server -- that thread will then start one
#         # more thread for each request
#         server_thread = threading.Thread(target=server.serve_forever)

#         server_thread.daemon = False
#         server_thread.start()
#         print("start_thread")
#         # server.shutdown()
        

class EchoServerProtocol:

    def __init__(self, chunk_size) -> None:
        self.chunk_size = 1024

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # message = data.decode()
        # print('Received %r from %s' % (message, addr))
        # print('Send %r to %s' % (message, addr))
        # data = self.request[0].strip()
        req_map = pickle.loads(data)
        print("udp receive data ", data)
        # parse req
        rpc_type = req_map.pop("rpc_type")
        meta = req_map["meta"]
        request = service_pb2.QuoteRequest(**meta)
        # cur_thread = threading.current_thread()
        # response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')
        response_iterator = rpc_client.on_reflect(request=request, rpc_type=rpc_type)
        checksum = bytes("sentinel", 'utf-8')

        for res in response_iterator:
            serialize = pickle.dumps(res)
            for idx in range(0, len(serialize), self.chunk_size):
                self.transport.sendto(serialize[idx: idx+self.chunk_size], addr)
            self.transport.sendto(checksum, addr)
            print("send serialize ", len(serialize))
        print("send shutdown to client")
        self.transport.sendto(bytes("shutdown", 'utf-8') , addr)


async def main():
    while True:
        print("Starting UDP server")

        # Get a reference to the event loop as we plan to use
        # low-level APIs.
        loop = asyncio.get_running_loop()
        # 
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, socket.SO_REUSEPORT)
        s.bind(('127.0.0.1', 9999))

        # One protocol instance will be created to serve all
        # client requests.
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: EchoServerProtocol(1024),
            # local_addr=('127.0.0.1', 9999),
            sock=s)

        try:
            await asyncio.sleep(3600)  # Serve for 1 hour.
        finally:
            loop.close()
            transport.close()


asyncio.run(main())
