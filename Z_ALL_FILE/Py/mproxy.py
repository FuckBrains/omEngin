import asyncio
import pproxy

ipport = "185.183.98.136:5030"

server = pproxy.Server('socks5://173.0.54.188:6888')
remote = pproxy.Connection('socks5://45.72.6.167:8000#HsQ1hf:jEcN8w')
args = dict( rserver = [remote],verbose = print )

loop = asyncio.get_event_loop()
handler = loop.run_until_complete(server.start_server(args))
try:
    loop.run_forever()
except KeyboardInterrupt:
    print('exit!')

handler.close()
loop.run_until_complete(handler.wait_closed())
loop.run_until_complete(loop.shutdown_asyncgens())
loop.close()
