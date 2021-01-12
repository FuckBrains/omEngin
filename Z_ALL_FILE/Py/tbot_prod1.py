import time
import telepot
from telepot.loop import MessageLoop
from pprint import pprint


TechSOCBOT = '1228075595:AAGYj2ck_yErfmVQFfW1xb7pzpSjTfVpadE'
bot = telepot.Bot(TechSOCBOT)

def handle(msg):
    pprint(msg)
    content_type, chat_type, chat_id = telepot.glance(msg)
    if (content_type == 'text'):
        print('ok')

MessageLoop(bot, handle).run_as_thread()
print('Listening ...')

while 1:
    time.sleep(10)