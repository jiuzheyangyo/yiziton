import mongo_client as mc
import pandas as pd
import decimal
import copy
import threading

import utils
import mongo_client as mc
import datetime
def export(startTime,endTime):
    billOp_localhost = mc.get_col_op("order", "waybillMains")

    list = billOp_localhost.find({'opDate':
             {'$gt': utils.dateTo8(datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")),
              '$lt': utils.dateTo8(datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S"))}});
    dataList = [];
    i = 0;
    for x in list:
        i = i + 1;
        print(i)
        data = {};
        data["一智通单号"] = x.get("waybillId")
        data["服务类型"] = x.get("serviceType")
        data["签收时间"] = x.get("opDate")
        data["安维单号"] = x.get("customerId")
        data["发货人"] = x.get("clientName")
        data["发货商家"] = x.get("contacts")
        data["单品安装单价"] = x.get("installFee")
        data["单品安装件数"] = x.get("installPackages")
        data["单品安装总价"] = x.get("totalInstallFeeNew")
        data["开单总安装费"] = x.get("totalInstallFeeOld")
        data["品名"] = x.get("standName")
        data["商家编号"] = x.get("customerProductCode")
        data["商家品名"] = x.get("customerProductName")
        dataList.append(data)
    headModle = ["一智通单号","服务类型","签收时间","安维单号","发货人","发货商家","单品安装单价","单品安装件数","单品安装总价","开单总安装费","品名","商家编号","商家品名"]
    df = pd.DataFrame(dataList)
    df.to_excel("aa1.xlsx", index=False, columns=headModle)
export("2019-01-01 00:00:00", "2019-04-01 00:00:00")