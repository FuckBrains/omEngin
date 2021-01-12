import os
import wmi
import winreg

ip = '173.0.54.190'
username = 'OMI'
password = '1q2w3eaz$'
from socket import *
#connection = wmi.WMI(ip, user=username, password=password)

r = wmi.WMI(ip, user=username, password=password).Registry()
result, names = r.EnumKey (hDefKey=0x80000001,sSubKeyName=r"Software")
for ky in names:
    print(ky)


def netsh_proxy(ip,port):
    x = "netsh winhttp set proxy " + ip + ':' + port
    os.system(x)

def addkey(ip,prt):
    x1 = "reg add 'HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings / v ProxyEnable / t REG_DWORD / d 1'"
    x2 = "reg add 'HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings /v ProxyServer /t REG_SZ /d '" + ip + ':' + prt

addkey('23.160.192.180','2016')