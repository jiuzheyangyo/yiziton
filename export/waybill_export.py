import mongo_client
from source_data import data_11
from source_data import data_l
from source_data import data_12
from source_data import data_s
import utils
import xlwt
import xlrd
import records
import pymongo
import bson
import datetime
col_op = mongo_client.get_col_op('order','orderMain')
col_op_waybillMain = mongo_client.get_col_op("order","waybillMain")
col_op_waybillLogisticsLog = mongo_client.get_col_op("order","waybillLogisticsLog")
# col_op.insert({"name":"xinxin",'classId':1,'age':14,'createDate':datetime.datetime.timestamp()})

dataList = []
list = col_op_waybillMain.find(
    #{'createTime':{'$gt':bson.timestamp.Timestamp(1538323200,000),'$lt':bson.Timestamp(1543593600,000)},"lastStatus" : "signed","shipper.clientName":{'$in':["深圳市前海洋臣科技发展有限公司","京东物流"]}},
    #{'billingTime':{'$gt':datetime.datetime.strptime("2018-10-01 00:00:00","%Y-%m-%d %H:%M:%S"),'$lt':datetime.datetime.strptime("2018-12-01 00:00:00","%Y-%m-%d %H:%M:%S")},"lastStatus" : "signed","shipper.clientName":{'$in':["深圳市前海洋臣科技发展有限公司","京东物流"]}},
    {'waybillId':{'$in':data_s.data_s}},
    {"company":1,"goods":1,"receiver":1,"serviceFee":1,"product":1,'billingTime':1,'service.serviceTypeName':1,"shipper.clientName":1,'waybillId':1}
    # {"service.customerId":1,'waybillId':1,'orderNo':1,'stateName':1}
)
i = 0
for waybillMain in list:
    data = {}

    # ---------------------运单信息----------------------
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
    waybillId = waybillMain.get('waybillId')
    data['waybillId'] = waybillId
    #如果运单不为空的时候去查询
    i = i+1
    print(i)
    # -------------------------------------订单信息---------------------------------------

    if(waybillId):
        x = col_op.find_one({'waybillId':waybillId},{"service.customerId":1,'waybillId':1,'orderNo':1,'stateName':1})
        # print(waybillMain)
        if(x):
            data['customerId'] = x.get('service').get('customerId')

            data['orderNo'] = x.get('orderNo')
            data['stateName'] = x.get('stateName')

    # -------------------------------------签收信息---------------------------------------
    if(waybillId):
        logs = col_op_waybillLogisticsLog.find({'waybillId':waybillId,'nodeType':{"$in":['normal','abnormal']}},{'opDate':1}).sort([('opDate',-1)])
        if not (logs.count() == 0):
            log = logs[0]
            data['opDate'] = log.get('opDate')
    dataList.append(data)

book = xlwt.Workbook(encoding="utf-8",style_compression=0)
sheet = book.add_sheet("test",cell_overwrite_ok=True)
# d = bson.decimal128.Decimal128("0.5");
# print(d.to_decimal())
row = 1
for val in dataList:
    sheet.write(row, 0, val.get('customerId'))
    sheet.write(row, 1, val.get('waybillId'))
    sheet.write(row, 2, val.get('orderNo'))
    sheet.write(row, 3, val.get('stateName'))
    dt = datetime.datetime.strftime(utils.dateTo8(val.get('billingTime')), "%Y-%m-%d %H:%M:%S") if(utils.dateTo8(val.get('billingTime'))) else ""
    sheet.write(row, 4, dt)
    sheet.write(row, 5, val.get('serviceTypeName'))
    sheet.write(row, 6, val.get('deptName'))
    sheet.write(row, 7, val.get('installPackages'))
    pa = 0
    if((isinstance(val.get('packages'),float) or isinstance(val.get('packages'),int))):
        pa = val.get('packages')
    else:
        pa = val.get('packages').to_decimal() if (val.get('packages')) else 0
    sheet.write(row, 8, pa)
    va = 0
    if(isinstance(val.get('volumes'), float) or isinstance(val.get('volumes'), int)):
        va = val.get('volumes')
    else:
        va = val.get('volumes').to_decimal() if (val.get('volumes')) else 0
    sheet.write(row, 9, va)
    sheet.write(row, 10, val.get('endAreaName'))
    sheet.write(row, 11, val.get('receiveAddress'))
    sheet.write(row, 12, val.get('product_name'))
    sheet.write(row, 13, val.get('transportFee'))
    sheet.write(row, 14, val.get('deliveryFee'))
    sheet.write(row, 15, val.get('upstairsFee'))
    sheet.write(row, 16, val.get('installFee'))
    sheet.write(row, 17, val.get('agingServiceFee'))
    sheet.write(row, 18, val.get('insuranceFee'))
    sheet.write(row, 19, val.get('takeGoodsFee'))
    sheet.write(row, 20, val.get('entryHomeFee'))
    sheet.write(row, 21, val.get('upHomeServiceFee'))
    sheet.write(row, 22, val.get('marbleFee'))
    sheet.write(row, 23, val.get('otherFee'))
    sheet.write(row, 24, val.get('truckBranchFee'))
    if val.get('opDate'):
        op_date = datetime.datetime.strftime(utils.dateTo8(val.get('opDate')), "%Y-%m-%d %H:%M:%S") if (utils.dateTo8(val.get('opDate'))) else ""
    sheet.write(row, 25, op_date)
    sheet.write(row, 26, val.get('clientName'))
    row = row + 1
# 最后，将以上操作保存到指定的Excel文件中
book.save(r'./a3.xls')
