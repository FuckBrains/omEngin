import subprocess
import time
import runpy
#def prnt():
   # print("running.....")
   # runpy.run_module('F:\PG\PgOnOFF\PGMAIN1406.py')
   # return "waiting"
#call("F:\\PG\\PgOnOFF\\Scripts\\python.exe","F:\\PG\\PgOnOFF\\pgmain_2.py")
#os.system('‪F:\PG\PgOnOFF\Scripts\python.exe pgmain_2.py')

def mcall():
    print("running.....")
    subprocess.call([r"F:\Python\Proj1\runbat.bat"])
    return "waiting"

while True:
    print(mcall())
    time.sleep(45)