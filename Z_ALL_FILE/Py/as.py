import asyncio
import aiosocks

reader, writer = await aiosocks.open_connection(proxy= '24.249.199.14' , proxy_auth='', dst= '57335', remote_resolve=True)
data = await reader.read(10)
writer.write('data')