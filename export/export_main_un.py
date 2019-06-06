import datetime

import datetime as datetime

import config
import pandas as pd
import mongo_client
import utils

mysqlClient = config.get_mysql_client()
cursor = mysqlClient.cursor()
cursor.execute("SELECT ew.waybill_id_,ewn.node_time_ from ewaybill_ ew, ewaybill_node_ ewn where ewn.waybill_id_ = ew.id_ and ewn.disabled_ = 0 and ewn.node_type_ = 'TMS_SIGN' and ewn.node_time_ >= '2019-01-01 00:00:00';")

col_op_waybillLogisticsLog = mongo_client.get_col_op("order","waybillLogisticsLog")
col_op_waybillMain = mongo_client.get_col_op("order","waybillMain")

waybillIds = []
map = {}
for x in cursor:
    waybillIds.append(x[0])
    map[x[0]] = x[1]
print(len(waybillIds))


list = col_op_waybillMain.find(
    # {'waybillId':{'$in':waybillIds}},
    {'billingTime':{'$lt':datetime.datetime.strptime("2019-01-01 00:00:00","%Y-%m-%d %H:%M:%S")},"lastStatusName" : {'$in':["待发车","待干线结束","待签收","待确认到达","待入库","待中转发车"]}},
    {"company": 1, "goods": 1, "receiver": 1, "serviceFee": 1, "product": 1, 'billingTime': 1, 'service.serviceTypeName': 1,"shipper.clientName": 1, 'waybillId': 1,'lastStatusName':1})
dataList = []
dataList2 = []
i = 0
print("start......")
for waybillMain in list:
    # ----------------------------判断这个单是否已经签收了，签收了过掉
    flag1 = 0;
    mysqlClient = config.get_mysql_client()
    cursor = mysqlClient.cursor()
    cursor.execute(
        '''SELECT ew.waybill_id_, ewn.node_time_ from ewaybill_ ew, ewaybill_node_ ewn
            where ewn.waybill_id_ = ew.id_ and ewn.disabled_ = 0
            and ewn.node_type_ = 'TMS_SIGN'
            and ew.waybill_id_ in (%s);
        ''',waybillMain['waybillId'])
    inmysqlWaybill = cursor.fetchall();
    print("len:-----%s",len(inmysqlWaybill))
    if(len(inmysqlWaybill)>0):
        flag1 = 1;
    i+=1
    print(i)
    print(waybillMain['waybillId'])
    data = {}
    waybillId = waybillMain.get('waybillId')
    data['waybillId'] = waybillId
    data['deptName'] = waybillMain.get('company').get('deptName')
    billingTime = waybillMain.get('billingTime')
    dt = datetime.datetime.strftime(utils.dateTo8(billingTime), "%Y-%m-%d %H:%M:%S") if (utils.dateTo8(billingTime)) else ""
    data['billingTime'] = dt
    data['serviceTypeName'] = waybillMain.get('service').get('serviceTypeName') if (waybillMain.get('service')) else ""
    data['packages'] = waybillMain.get('goods').get('packages')
    data['volumes'] = waybillMain.get('goods').get('volumes')
    data['endAreaName'] = waybillMain.get('receiver').get('endAreaName')
    data['receiveAddress'] = waybillMain.get('receiver').get('receiveAddress')
    data['clientName'] = waybillMain.get('shipper').get('clientName')
    serviceFee = waybillMain.get('serviceFee')
    for fee in serviceFee:
        feeType = fee.get('feeType')
        num = fee.get('amount')
        flag = isinstance(num, int) or isinstance(num, float)
        if (flag):
            data[feeType] = num
        else:
            data[feeType] = num.to_decimal() if (num) else 0
    product = waybillMain.get('product')
    product_name = ""
    installPackages = 0
    if (product):
        for g in product:
            product_name = product_name + (
                g.get('standName') if (g.get('standName')) else g.get('busName')) + "*" + str(
                g.get("installPackages")) + ";"
            if (isinstance(g.get('installPackages'), str)):
                installPackages = installPackages + int(g.get('installPackages'))
            elif (isinstance(g.get('installPackages'), int)):
                installPackages = installPackages + g.get('installPackages')
            elif(isinstance(g.get('installPackages'), float)):
                installPackages = installPackages + g.get('installPackages')
            else:
                installPackages = installPackages + (
                    g.get('installPackages').to_decimal() if (g.get('installPackages')) else 0)

    data['installPackages'] = installPackages
    data['product_name'] = product_name
    data['status'] = waybillMain.get('lastStatusName')
    if (waybillId):
        logs = col_op_waybillLogisticsLog.find({'waybillId': waybillId, 'nodeType': {"$in": ['normal', 'abnormal']}},
                                               {'opDate': 1}).sort([('opDate', -1)])
        if not (logs.count() == 0):
            log = logs[0]
            data['opDate'] = log.get('opDate')
    # opDate = map[waybillId];
    # if(opDate):
    #     op_date = datetime.datetime.strftime(utils.dateTo8(opDate), "%Y-%m-%d %H:%M:%S") if (utils.dateTo8(opDate)) else ""
    #     data['opDate'] = op_date
    if(flag1 == 1):
        dataList2.append(data)
    else:
        dataList.append(data)
df = pd.DataFrame(dataList)
df2 = pd.DataFrame(dataList2)
df.to_csv("未签收的2.csv")
df2.to_csv("未签收的ips已签收2.csv")
print("finished.........")
