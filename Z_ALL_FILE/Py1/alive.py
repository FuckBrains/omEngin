#from proxy_checker import ProxyChecker
import nmap


#checker = ProxyChecker()
#checker.check_proxy('45.72.6.167:8000')
       

#checkProxy('45.72.6.167:8000')
#--proxy 45.72.6.167:8000
#nmap 45.72.6.167 -p8000
#nmap -p T:8000 45.72.6.167
nm = nmap.PortScanner()
x = nm.scan('45.72.6.167', '8000')
print(x)
