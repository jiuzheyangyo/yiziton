import config
import pandas as pd
import mongo_client

mysqlClient = config.get_mysql_client()
cursor = mysqlClient.cursor()
cursor.execute("SELECT ew.waybill_id_ from ewaybill_ ew, ewaybill_node_ ewn where ewn.waybill_id_ = ew.id_ and ewn.disabled_ = 0 and ewn.node_type_ = 'TMS_SIGN' and ewn.node_time_ >= '2019-01-01 00:00:00';")

col_op_waybillLogisticsLog = mongo_client.get_col_op("order","waybillLogisticsLog")
col_op_waybillMain = mongo_client.get_col_op("order","waybillMain")

waybillIds = []
for x in cursor:
    waybillIds.append(x[0])
print(len(waybillIds))


list = col_op_waybillMain.find(
    {'waybillId':{'$in':waybillIds}},
    {"company": 1, "goods": 1, "receiver": 1, "serviceFee": 1, "product": 1, 'billingTime': 1, 'service.serviceTypeName': 1,"shipper.clientName": 1, 'waybillId': 1})
dataList = []
i = 0
print("start......")
for waybillMain in list:
    i+=1
    print(i)
    print(waybillMain['waybillId'])
    data = {}
    waybillId = waybillMain.get('waybillId')
    data['waybillId'] = waybillId
    data['deptName'] = waybillMain.get('company').get('deptName')
    data['billingTime'] = waybillMain.get('billingTime')
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
    dataList.append(data)
df = pd.DataFrame(dataList)
df.to_csv("cc.csv")
print("finished.........")
