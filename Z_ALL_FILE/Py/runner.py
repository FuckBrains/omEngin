import asyncio

async def periodic():
    while True:
        print('periodic')
        await asyncio.sleep(60)

def stop():
    task.cancel()

loop = asyncio.get_event_loop()
task = loop.create_task(periodic())

try:
    loop.run_until_complete(task)
except asyncio.CancelledError:
    pass