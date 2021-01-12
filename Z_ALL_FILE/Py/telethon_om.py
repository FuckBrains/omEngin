from telethon.sync import TelegramClient

def omi():
    api_id = 628127
    api_hash = 'db7fa09d585d6eedddd0df5973f3239b'
    phone = '+8801817184338'
    client = TelegramClient(phone, api_id, api_hash)
    client.connect()
    if not client.is_user_authorized():
        client.send_code_request(phone)
        client.sign_in(phone, input('Enter the code: '))
        print(client.get_me().stringify())
    
def smpool():
    api_id = 1621549
    api_hash = '6b06c3cf6e7004803b11c79f80e1b8bf'
    phone = '+8801817183680'
    client = TelegramClient(phone, api_id, api_hash)

omi()