import requests
import threading
import time
r = requests.post("http://localhost:7777/api/priceManage/getFeeGroupOne",json = [{"feeType":"installFee","name":"标准"}])
def getOne1():
    r = requests.post("http://192.168.3.98:7777/api/priceManage/getStandFees", json=[{"startTime":"2019-04-04 00:00:00","endTime":"2019-04-06 00:00:00"}])
threading.Thread(target=getOne1,name=("thread-%s",1)).start();

def getOne2():
    r = requests.post("http://192.168.3.98:7777/api/priceManage/getStandFees", json=[{"startTime":"2019-04-06 00:00:00","endTime":"2019-04-08 00:00:00"}])
threading.Thread(target=getOne2,name=("thread-%s",2)).start();







