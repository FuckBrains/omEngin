import os
import wmi
import winreg
import subprocess

ip = '173.0.54.190'
username = 'OMI'
password = '1q2w3eaz$'
from socket import *
#connection = wmi.WMI(ip, user=username, password=password)

r = wmi.Registry()
result, names = r.EnumKey (hDefKey=0x80000001,sSubKeyName=r"Software")
#for ky in names:
    #print(ky)

def netsh_proxy(ip,port):
    x = 'netsh winhttp set proxy ' + ip + ':' + port
    print(x)
    os.system(x)

def addkey(ip,prt):
    x1 = "reg add 'HKLM\Software\Microsoft\Windows\CurrentVersion\Internet Settings / v ProxyEnable / t REG_DWORD / d 1'"
    x2 = "reg add 'HKLM\Software\Microsoft\Windows\CurrentVersion\Internet Settings /v ProxyServer /t REG_SZ /d '" + ip + ':' + prt
    os.system(x1)
    os.system(x2)

#addkey('23.160.192.180','2016')


def read_reg(arg1,arg2):
    command = os.popen('cscript regrd.vbs "' +  arg1 + '"' + ' "' + arg2 + '"').read()
    print(command)

def addky(kpath,kname,ktype,kvalue):
    x = 'reg add' + ' "' + kpath + '" /v "' +  kname + '" /t "' + ktype + '" /d "' + kvalue + '"'
    print(x)
    os.system(x)

def editky(kpath,kname,ktype,kvalue):
    x = 'reg add' + ' "' + kpath + '" /v "' +  kname + '" /t "' + ktype + '" /d "' + kvalue + '" /F'
    print(x)
    os.system(x)

kp = "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings"
#knm = "ProxyEnable"
#kty = "REG_DWORD"
#kvl ="1"

knm = "ProxyServer"
kty = "REG_SZ"
kvl = "23.160.192.180:2016"


read_reg("Software\Microsoft\Windows\CurrentVersion\Internet Settings","HKCU")
#netsh_proxy('23.160.192.180','2016')
#addky(kp, knm, kty, kvl)
editky(kp, knm, kty, kvl)


def qryky(pth,keyname):
    x = 'reg query \HKLU\Software\"  /ve'


#REG QUERY HKLM\SOFTWARE /ve