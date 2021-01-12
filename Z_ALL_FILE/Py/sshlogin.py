import paramiko
import sys
import time
import os

def Write2TxtList(dr,filename,content):
    flpath = dr + '\\' + filename
    print(flpath)
    fl = open(flpath, 'w')
    cont = "".join(content)
    fl.write(cont + '\n')
    fl.close()

class server():
    def __init__(self,HOST):
        self.USER = "ak2986"
        self.PASS = "Robi@123"
        self.PORT = 22
        self.c1 = paramiko.SSHClient()
        self.c1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.c1.connect(HOST,port=self.PORT,username=self.USER,password=self.PASS)
        print ("SSH connection to %s established" %HOST)
    def excmd(self,cmd):
        c1 = self.c1
        stdin, stdout, stderr = c1.exec_command(cmd)        
        output = stdout.readlines()
        opt = "".join(output)
        print(opt)
        return opt
    def conn_cls(self):
        c1 = self.c1
        c1.close()
        print('connection close successfully')

#Write2TxtList(os.getcwd(), 'OutputLog.txt', opt)
ossip = "10.16.214.103"
x = server(ossip)
#y = x.excmd('ls')
#Write2TxtList(os.getcwd(), 'OutputLog.txt', y)


