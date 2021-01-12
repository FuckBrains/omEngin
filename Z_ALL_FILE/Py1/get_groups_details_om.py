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
    print("success")



def group_participant(target_group):
    all_participants = client.get_participants(target_group, aggressive=True)
    print('GroupName: ', target_group, "total member: ", len(all_participants))
    print("serial | firstname | lastname | phone")
    n = 0
    for i in all_participants:
        n = n + 1
        x = 'adduser, halim, 23421435, 01817123123'
        x1 = 'adduser,' + str(i.first_name) + "," + str(i.id) + "," + str(i.phone)
        print(x1)
        #if i.last_name == 'none':
            #print(n, '. name: ' , i.first_name, ' | ' , "Phone: ", i.phone, i.id)
        #else:
            #print (n, '. name: ', i.first_name, i.last_name , ' | ', "Phone: ", i.phone, 'ID - ', i.id)
    #print(all_participants)

x = group_participant('VIP Sites Update')