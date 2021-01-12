import subprocess

def ex(cmd):
    command = subprocess.popen(cmd).read()
    return command


def set_proxy(proxy_ip):
   print('x')






