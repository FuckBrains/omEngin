
#http://timgolden.me.uk/python/wmi/tutorial.html

import wmi
import os
import subprocess

def ex(cmd):
    command = os.popen(cmd).read()
    return command

def os_version():
    c = wmi.WMI()
    for os in c.Win32_OperatingSystem():
        print(os.Caption)
  
#x = os.system('ipconfig')
#os.system("netsh interface show interface")
#x = subprocess.call('netsh interface ipv4 show interface')
#y = ex('powershell "get-wmiobject win32_networkadapter | select netconnectionid, name, InterfaceIndex, netconnectionstatus"')
y = ex('powershell "Get-NetConnectionProfile"')

