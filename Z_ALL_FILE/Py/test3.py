
import socket
import Queue
import threading
import time
import os
import sys
from random import *
from struct import *





def getSocksVersion(self, proxy):
        host = proxy.split(":")[0]
        try:
            port = int(proxy.split(":")[1])
            if port < 0 or port > 65536:
                print "Invalid: " + proxy
                return 0
        except:
            print "Invalid: " + proxy
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        try:
            s.connect((host, port))
            if(self.isSocks4(host, port, s)):
                s.close()
                return 5
            elif(self.isSocks5(host, port, s)):
                s.close()
                return 4
            else:
                ("Not a SOCKS: " + proxy)
                s.close()
                return 0
        except socket.timeout:
            print "Timeout: " + proxy
            s.close()
            return 0
        except socket.error:
            print "Connection refused: " + proxy
            s.close()
            return 0

getSocksVersion('45.72.6.167:8000')