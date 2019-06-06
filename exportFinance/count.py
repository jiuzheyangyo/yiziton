import copy
import threading

import utils
import mongo_client as mc
import datetime
def count(startTime,endTime):
    billOp = mc.get_col_op_prod("order", "waybillMain")
    queryParam = {
        "$and": [
            {"service.serviceType": {"$in": ["distributionInstallation", "install", "cityDistributionInstallation"]}},
            {"shipper.clientCode": {"$in": ["KH2017106444", "KH20170824585", "KH18122500000002"]}},
            {"lastStatus": {'$in': ["signed"]}},
            {'billingTime':
                 {'$gt': utils.dateTo8(datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")),
                  '$lt': utils.dateTo8(datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S"))}}
        ]
    }
    list = billOp.find(queryParam, no_cursor_timeout=True);
    return list.count()

print(count("2018-11-01 00:00:00", "2018-11-10 00:00:00"));

print(count("2018-11-10 00:00:00", "2018-11-15 00:00:00"));

print(count("2018-11-15 00:00:00", "2018-11-20 00:00:00"));

print(count("2018-11-20 00:00:00", "2018-11-25 00:00:00"));

print(count("2018-11-25 00:00:00", "2018-12-01 00:00:00"));