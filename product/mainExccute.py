import copy
import threading
import pandas as pd
from bson import Decimal128

import utils
import mongo_client as mc
import datetime

def execute(startTime,endTime,fileName):
    billOp = mc.get_col_op_prod("order", "waybillMain");

    queryParam = {
        "$and": [
            # {"service.serviceType": {"$in": ["distributionInstallation", "install", "cityDistributionInstallation"]}},
            # {"shipper.clientCode": {"$in": ["KH2017106444", "KH20170824585", "KH18122500000002"]}},
            {"lastStatus": {'$ne': ["cancel"]}},
            {'billingTime':
                 {'$gt': utils.dateTo8(datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")),
                  '$lt': utils.dateTo8(datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S"))}}
        ]
    }
    list = billOp.find(queryParam, no_cursor_timeout=True);

    resultList = []
    # resultListF = {}
    for bill in list:

        product = bill.get("product")
        hasOn = [];
        for p in product:
            goodsId = "";
            name = ""
            busGoodsId = p.get("busGoodsId");
            if busGoodsId :
                goodsId = busGoodsId;
                name = p.get("busName")
            else:
                goodsId = p.get("standGoodsId")
                name = p.get("standName")

            if goodsId and goodsId not in hasOn:
                idxs = [i for i,v in enumerate(resultList) if v['goodsId'] == goodsId]
                installPackages = p.get("installPackages")
                if isinstance(installPackages, Decimal128):
                    installPackages = float(installPackages.to_decimal())
                if isinstance(installPackages, str):
                    installPackages = float(installPackages)
                installFee = p.get("installFee")
                if isinstance(installFee, Decimal128):
                    installFee = float(installFee.to_decimal())
                if isinstance(installFee, str):
                    installFee = float(installFee)
                print(type(installFee))
                if idxs:
                    idx = idxs[0]
                    resultList[idx]["ip"] = resultList[idx]["ip"] + installPackages
                    resultList[idx]["ify"] = resultList[idx]["ify"] + installFee
                else:
                    resultList.append({"goodsId":goodsId,"name":name,"ip":installPackages,"ify":installFee})
            else:
                continue;
            hasOn.append(goodsId)
    df = pd.DataFrame(resultList)
    df.to_csv(fileName)

# execute("2019-05-01 00:00:00", "2019-06-01 00:00:00")


def execute1():
    execute("2019-04-01 00:00:00", "2019-05-01 00:00:00","1904.csv")
    execute("2019-03-01 00:00:00", "2019-04-01 00:00:00", "1903.csv")
threading.Thread(target=execute1, name=("thread-%s", 1)).start();


def execute2():
    execute("2019-02-01 00:00:00", "2019-03-01 00:00:00", "1902.csv")
    execute("2019-01-01 00:00:00", "2019-02-01 00:00:00", "1901.csv")
threading.Thread(target=execute2, name=("thread-%s", 2)).start();

def execute3():
    execute("2018-12-01 00:00:00", "2019-01-01 00:00:00", "1812.csv")
    execute("2018-11-01 00:00:00", "2018-12-01 00:00:00", "1811.csv")
threading.Thread(target=execute3, name=("thread-%s", 3)).start();

def execute4():
    execute("2018-10-01 00:00:00", "2018-11-01 00:00:00", "1810.csv")
    execute("2018-09-01 00:00:00", "2018-10-01 00:00:00", "1809.csv")
    execute("2018-08-01 00:00:00", "2018-09-01 00:00:00", "1808.csv")
    execute("2018-07-01 00:00:00", "2018-08-01 00:00:00", "1807.csv")
threading.Thread(target=execute4, name=("thread-%s", 4)).start();









def aa():
    hasOn = [{"name":"aa" ,"num":1},{"name":"bb"},{"name":"cc"}]
    value = [i for i,v in enumerate(hasOn) if v['name']=='aa']
    print(value)
    # if {"num":1,"name":"aa"} not in hasOn:
    #     print("----")
    # else:
    #     print("cccccc")
    # one = {}
    # one["895232"] = 5;
aa()
# print('2' if '' else '1')