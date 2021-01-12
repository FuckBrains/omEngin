import sys
import os
import wmi
import subprocess

def ex(cmd):
    command = os.popen(cmd).read()
    return command