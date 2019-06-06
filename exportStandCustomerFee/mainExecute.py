from exportStandCustomerFee import sc_localhost,exportStandCustomer
import requests
time = "2019-04-22"

def toTemp(time):
    r = requests.post("http://192.168.3.98:7777/api/priceManage/getStandFees", json=[{"startTime":time+" 00:00:00","endTime":time+" 23:59:59"}])

def jisuanAll(time):
    r = requests.post("http://192.168.3.98:7777/api/priceManage/jisuanAll", json=[{"time": time}])

def jisuanNotCombo(time):
    r = requests.post("http://192.168.3.98:7777/api/priceManage/jisuanNotCombo", json=[{"time": time}])

sc_localhost.transport_sc_localhost(time)

toTemp(time)

jisuanAll(time)

jisuanNotCombo(time)

exportStandCustomer.exportNC(time)

exportStandCustomer.exportAll(time)

