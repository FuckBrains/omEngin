from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from pprint import pprint
import os

api_id = 628127
api_hash = 'db7fa09d585d6eedddd0df5973f3239b'
phone = '+8801817184338'
client = TelegramClient(phone, api_id, api_hash)
client.connect()
if not client.is_user_authorized():
    client.send_code_request(phone)
    client.sign_in(phone, input('Enter the code: '))


async def main():
    st = ""
    async for dialog in client.iter_dialogs():
        try:
            st1 = str(dialog.id)
            st2 = st1[0]
            if st2 == chr(45):
                st = st + "\n" + str(dialog.id) + ',' + str(dialog.name)
        except:
            pass
    return st

def wrt_content(st):
    try:
        file = os.getcwd() + "//omgroup//omgrp.txt"
        fl = open(file, "w+", encoding="utf-8")
        fl.write(st)
        fl.close()
    except:
        file = os.getcwd() + "//omgrp.txt"
        fl = open(file, "w+", encoding="utf-8")
        fl.write(st)
        fl.close()
    return file

def client_run():
    with client:
        sx = client.loop.run_until_complete(main())
        fl = wrt_content(sx)
        return fl
