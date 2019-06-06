import mongo_client as mc
import pandas as pd
import decimal

def timeToArrTime(time):
    preStr = time[:-2]
    num = int(time[-2:])
    numArr = []
    for i in range(1,num+1):
        if i<10:
            i = "0"+str(i)
        numArr.append((preStr+"%s" % i))
    return numArr

def timeToArrTimeNC(time):
    preStr = time[:-2]
    num = int(time[-2:])
    numArr = []
    for i in range(1,num+1):
        if i<10:
            i = "0"+str(i)
        numArr.append((preStr+"%sNC" % i))
    return numArr

def exportAll(time):
    cpOp = mc.get_col_op("priceManage","customerPrice");
    list = cpOp.find({"recordTime":{"$in":timeToArrTime(time)}})
    dataList = [];
    i = 0;
    for x in list:
        i = i+1;
        print(i)
        data = {};
        data["客户名称"] = x.get("name")
        data["日期"] = x.get("recordTime")
        data["开单总金额"] = x.get("feeValue")
        data["标准价格开单总金额"] = x.get("standFeeValue")
        data["标准价格执行率"] = x.get("executeRate")
        dataList.append(data)
    headModle = ["客户名称","日期","开单总金额","标准价格开单总金额","标准价格执行率"]
    df = pd.DataFrame(dataList)
    # df["开单总金额"] = df["开单总金额"].astype(decimal)
    # df["标准价格开单总金额"] = df["标准价格开单总金额"].astype(decimal)
    # df["标准价格执行率"] = df["标准价格执行率"].astype(decimal)
    df.to_excel(time+"标准客户价格执行率（包含套餐）.xlsx",index=False,columns=headModle)

def exportNC(time):
    cpOp = mc.get_col_op("priceManage","customerPrice");
    list = cpOp.find({"recordTime":{"$in":timeToArrTimeNC(time)}})
    dataList = [];
    for x in list:
        data = {};
        data["客户名称"] = x.get("name")
        recordTime = x.get("recordTime")
        data["日期"] = recordTime[:-2]
        data["开单总金额"] = x.get("feeValue")
        data["标准价格开单总金额"] = x.get("standFeeValue")
        data["标准价格执行率"] = x.get("executeRate")
        dataList.append(data)
    headModle = ["客户名称", "日期", "开单总金额", "标准价格开单总金额", "标准价格执行率"]
    df = pd.DataFrame(dataList)
    # df["开单总金额"] = df["开单总金额"].astype(decimal)
    # df["标准价格开单总金额"] = df["标准价格开单总金额"].astype(decimal)
    # df["标准价格执行率"] = df["标准价格执行率"].astype(decimal)
    df.to_excel(time+"标准客户价格执行率（不包含套餐）.xlsx",index=False,columns=headModle)
exportAll("2019-04-11")

exportNC("2019-04-11")
