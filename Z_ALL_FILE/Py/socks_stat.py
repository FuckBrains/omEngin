#ref: https://stem.torproject.org/api/connection.html

import requests as r

def sockchk_1(ip):
    proxycheck_io = "830284-700030-f06940-3c6410"
    apilink = "http://proxycheck.io/v2/" + ip + "?key=830284-700030-f06940-3c6410&asn=1"
    rs = r.get(apilink)
    jsonResponse = rs.json()
    print(jsonResponse)
    
    
sockchk_1('45.72.6.167')