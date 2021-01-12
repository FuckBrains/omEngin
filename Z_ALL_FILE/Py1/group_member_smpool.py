from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from pprint import pprint
import os


api_id = 1621549
api_hash = '6b06c3cf6e7004803b11c79f80e1b8bf'
phone = '+8801817183680'
client = TelegramClient(phone, api_id, api_hash)
client.connect()

if not client.is_user_authorized():
    client.send_code_request(phone)
    client.sign_in(phone, input('Enter the code: '))
    print("success")

def group_participant(target_group):
    all_participants = client.get_participants(target_group, aggressive=True)
    print('GroupName: ', target_group, "total member: ", len(all_participants))
    print("serial | firstname | lastname | phone")
    n = 0
    for i in all_participants:
        n = n + 1
        if i.last_name == 'none':
            print(n, '. name: ' , i.first_name, ' | ' , "Phone: ", i.phone)
        else:
            print (n, '. name: ', i.first_name, i.last_name , ' | ', "Phone: ", i.phone)

x = group_participant()