import time
from datetime import *
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import *
from dateutil.relativedelta import *

n = datetime.now()
td = date.today()

def Now():
    return n

def nw():
    nw_str = n.strftime("%Y-%m-%d %H:%M:%S")
    return nw_str

def min_plus(diff):
    d = n + timedelta(minutes=diff)
    str_d = d.strftime("%Y-%m-%d %H:%M:%S")
    return str_d


def min_minus(diff):
    d = n - timedelta(minutes=diff)
    str_d = d.strftime("%Y-%m-%d %H:%M:%S")
    return str_d


def hr_plus(diff):
    d = n + timedelta(hours=diff)
    str_d = d.strftime("%Y-%m-%d %H:%M:%S")
    return str_d

def hr_minus(diff):
    d = n - timedelta(hours=diff)
    str_d = d.strftime("%Y-%m-%d %H:%M:%S")
    return str_d

def curr_day():
    return td.strftime('%d')

def curr_month():
    return td.strftime('%m')

def curr_year():
    return td.strftime('%Y')

def curr_date():
    return td.strftime('%Y-%m-%d')

def date_between(date1, date2):
    d1 = datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
    return abs(d2 - d1).days


def aging(date1, date2):
    d1 = datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
    d2= datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
    mn = abs(d2 - d1)
    return mn


def deltamonth(dt, diff):
    dx = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
    delt = dx + relativedelta(months=diff)
    return delt


def day_minus(diff):
    d = td - timedelta(days=diff)
    str_d = d.strftime("%d-%b-%Y")
    return str_d


def day_plus(diff):
    d = td + timedelta(days=diff)
    str_d = d.strftime("%d-%b-%Y")
    return str_d

# def date_str(dt):
# def fmt_to_datetime():
# def fmt_to_str():
# delta_month(nw(),-4)
# def month_delta(dt,diff):
# d1 = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
# def day_delta(dt,diff):
# def date_minus(dt, diff):
# def month_minus(dt, diff):
# def year_minus(dt, diff):
# print(aging(nw(),'2020-06-13 00:00:00'))
# print(min_plus(500))
# print(min_minus(500))
# print(hr_plus(2))






