
import copy
import threading

import utils
import mongo_client as mc
import datetime
def delete(startTime,endTime):
    billOp = mc.get_col_op_prod("order", "waybillMain")
    billOp_localhost = mc.get_col_op("order", "waybillMains")
    queryParam = {
        "$and": [
            {"service.serviceType": {"$in": ["distributionInstallation", "install", "cityDistributionInstallation"]}},
            {"shipper.clientCode": {"$in": ["KH2017106444", "KH20170824585", "KH18122500000002"]}},
            {"lastStatus": {'$in': ["signed"]}},
            {'billingTime':
                 {'$gt': utils.dateTo8(datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")),
                  '$lt': utils.dateTo8(datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S"))}},

        {
            "product.standGoodsId": ""
        }
        ]
    }
    list = billOp.find(queryParam, no_cursor_timeout=True);

    i = 0
    for x in list:
        i = i+1;
        print(i)
        billOp_localhost.delete_many({"waybillId":x.get("waybillId")})

delete("2018-03-01 00:00:00", "2019-04-01 00:00:00")