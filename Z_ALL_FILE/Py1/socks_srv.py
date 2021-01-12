#ref: https://rushter.com/blog/python-socks-server/
from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler

class ThreadingTCPServer(ThreadingMixIn, TCPServer):
    pass

class SocksProxy(StreamRequestHandler):
    def handle(self):
        # Our main logic will be here
        pass

if __name__ == '__main__':
    with ThreadingTCPServer(('127.0.0.1', 9011), SocksProxy) as server:
        server.serve_forever()