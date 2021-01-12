import asyncio
import main as m
from datetime import *

async def periodic():
    while True:
        nww = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("start at ", nww)
        xx = m.main()
        print("ends at " , xx)
        await asyncio.sleep(30)

def stop():
    task.cancel()

loop = asyncio.get_event_loop()
task = loop.create_task(periodic())

try:
    loop.run_until_complete(task)
except asyncio.CancelledError:
    pass