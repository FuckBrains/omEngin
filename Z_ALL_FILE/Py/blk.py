import socks


s = socks.socksocket()
s.set_proxy(socks.SOCKS5, "134.122.36.167", 1080)
x = s.connect(("www.google.com", 80))
print(x)
