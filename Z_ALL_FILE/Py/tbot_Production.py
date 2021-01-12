import time
import telepot
from telepot.loop import MessageLoop
from pprint import pprint
import tbot.tbot_prod1 as techsoc

akomibot = "1054945336:AAGv-B9ojejhA0ohDwRltTs5mYnOX8lK55M" #akomibot
bot = telepot.Bot(akomibot)

def handle(msg):
    pprint(msg)
    content_type, chat_type, chat_id = telepot.glance(msg)
    if (content_type == 'text'):
        bot.sendMessage(chat_id, "hello")

MessageLoop(bot, handle).run_as_thread()
print('Listening ...')

while 1:
    time.sleep(10)