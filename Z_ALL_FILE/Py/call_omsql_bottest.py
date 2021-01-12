import csv, os
import telepot
from telepot.loop import MessageLoop
from pprint import pprint
from call_omsql import *
import time as tm
from datetime import *


TOKEN = '1055749951:AAG9J4nV8thnnSPSKNkvf-G1lcWmH5QMyjA'
bot = telepot.Bot(TOKEN)

def handle(msg):
    pprint(msg)
    content_type, chat_type, chat_id = telepot.glance(msg)
    if content_type == 'text':
        txt = msg['text']
        chtid = msg['chat']['id']
        if 'periodic' in txt.lower():
            x = periodic_contacts(txt.lower())
            if len(x) < 1:
                bot.sendMessage(chat_id, 'please send correctly')
            else:
                bot.sendMessage(chat_id, x)
        else:
            bot.sendMessage(chat_id, txt + ' ' + str(chtid))
    else:
        try:
            x = msg['document']['file_id']
            bot.sendMessage(chat_id, x.replace('%0a','/n'))
        except:
            bot.sendMessage(chat_id, 'can not read file id')

MessageLoop(bot, handle).run_as_thread()
print('Listening ...')

while 1:
    tm.sleep(10)