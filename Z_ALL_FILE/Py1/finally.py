import subprocess
import time
import threading
import urllib
import requests
import queue

#def output_reader(proc, outq):
    #for line in iter(proc.stdout.readline, b''):
        #print('got line: {0}'.format(line.decode('utf-8')), end='')

def output_reader(proc, outq):
    for line in iter(proc.stdout.readline, b''):
        outq.put(line.decode('utf-8'))
        try:
            ln = outq.get(block=False)
            print('got line from outq: {0}'.format(ln), end='')
        except queue.Empty:
            print('could not get line from queue')

def main():
    #['python3', '-u', '-m', 'http.server', '8070']
    proc = subprocess.Popen(["gost","-L","socks5://0.0.0.0:11126","-D"],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    outq = queue.Queue()
    t = threading.Thread(target=output_reader, args=(proc,outq))
    t.start()

    try:
        proc.wait(timeout=0.2)
        print('== subprocess exited with rc =', proc.returncode)
    except subprocess.TimeoutExpired:
        print('subprocess did not terminate in time')
    t.join()
    
main()