import atexit
import socketserver
import pdb
import pickle
import threading
import signal
import sys



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
          # cur_thread = threading.current_thread()
          # response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')

#         server_thread.daemon = False
#         server_thread.start()
#         print("start_thread")
#         # server.shutdown()
        