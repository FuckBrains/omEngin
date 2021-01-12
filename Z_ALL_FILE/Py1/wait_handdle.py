import time as tm
from datetime import *

ts= tm.time()
n = datetime.now()
td = date.today()

def wait_handdle(ex_time):
    Mn = int(n.strftime("%M"))
    if ex_time==55:
        if Mn>=55:
            wt = (60-Mn)*60
            print('Waiting for second: ', str(wt))
            tm.sleep(wt)
            return "EX"
        elif Mn >= 0 and Mn <= 15:
            return "EX"
        elif Mn >= 16 and Mn < 25:
            return "STOP"
        else:
            return "STOP"
    elif ex_time==25:
        if Mn>=25 and Mn<30:
            tm.sleep((30-Mn)*60)
            return "EX"
        elif Mn >= 30 and Mn <= 45:
            return "EX"
        elif Mn >= 45 and Mn < 55:
            return "STOP"
        else:
            return "STOP"
    else:
        return "EX"





