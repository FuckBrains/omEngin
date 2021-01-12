import requests


def rocsms(ms,text):
    url = "https://web1.robi.com.bd/apiresponse.php?user=robircouser&pass=Gqras@3789291&from=10144&to=" + str(ms) + "&text=" + text
    rs = requests.get(url)
    print(rs)

rocsms('+8801817184338','from py xx')