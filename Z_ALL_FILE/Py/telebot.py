import sys
import time
import telepot
from telepot.loop import MessageLoop
import json

TOKEN = '1184517046:AAFBnQe_HRMx4ANWbebp8W8rzQMlRb07nG4'
bot = telepot.Bot(TOKEN)

def query_hanndler(code):
    return code

def handle(msg):
    content_type, chat_type, chat_id = telepot.glance(msg)
    print(content_type, chat_type, chat_id)
    if content_type == 'text':
        ctype = content_type
        txt = msg['text']
        if txt == "Clean and clear":
            bot.sendMessage(chat_id, "price 150TK")
        elif len(txt) == 7:
            getval = query_hanndler(txt)
            bot.sendMessage(chat_id, getval)
        else:
            bot.sendMessage(chat_id, 'what is name of facewash?')
            
        
MessageLoop(bot, handle).run_as_thread()
print ('Listening ...')

while 1:
    time.sleep(10)
