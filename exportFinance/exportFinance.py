import copy
import threading

import utils
import mongo_client as mc
import datetime
def load(startTime,endTime):
    billOp = mc.get_col_op_prod("order", "waybillMain")
    orderOp = mc.get_col_op_prod("order", "orderMain")
    col_op_waybillLogisticsLog = mc.get_col_op_prod("order", "waybillLogisticsLog")
    billOp_localhost = mc.get_col_op("order", "waybillMains")
    queryParam = {
    "$and":[
        {"service.serviceType":{"$in":["distributionInstallation","install","cityDistributionInstallation"]}} ,
        {"shipper.clientCode":{"$in":["KH2017106444","KH20170824585","KH18122500000002"]}},
        {"lastStatus" : {'$in':["signed"]}},
        {'billingTime':
             {'$gt': utils.dateTo8(datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")),
              '$lt': utils.dateTo8(datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S"))}}
        ]
    }
    list = billOp.find(queryParam,no_cursor_timeout = True);
    count = 0

    for bill in list:
        count = count + 1;
        print(count)
        waybillId = bill.get("waybillId")

        # 重复检验
        findOne = billOp_localhost.find_one({"waybillId": waybillId})
        if findOne is not None:
            continue
        totalInstallFeeOld = 0
        serviceFee = bill.get("serviceFee")
        for serviceF in serviceFee:
            if serviceF.get("feeType") == "installFee":
                totalInstallFeeOld = serviceF.get("amount")

        addOneMain = {}
        # 一智通单号
        addOneMain["waybillId"] = waybillId
        # 服务类型
        addOneMain["serviceType"] = bill.get("service").get("serviceTypeName")
        # 签收时间
        logs = col_op_waybillLogisticsLog.find({'waybillId': waybillId, 'nodeType': {"$in": ['normal', 'abnormal']}},
                                               {'opDate': 1}).sort([('opDate', -1)])
        if not (logs.count() == 0):
            log = logs[0]
            addOneMain['opDate'] = log.get('opDate')
        # 安维单号
        addOneMain["customerId"] = bill.get("service").get("customerId")

        # 发货人
        addOneMain["clientName"] = bill.get("shipper").get("clientName")

        # 发货商家
        addOneMain["contacts"] = bill.get("shipper").get("contacts")

        addMany = [];

        sproduct = []
        orders = orderOp.find({"waybillId":waybillId})

        if orders is not None:
            for order in orders:
                op = order["product"]
                sproduct = sproduct+op

        product = bill["product"]

        forProduct = []

        if len(sproduct) > len(product):
            for i,sp in enumerate(sproduct):
                appendOne = {}
                appendOne["customerProductCode"] = sp.get("busGoodsId")
                appendOne["customerProductName"] = sp.get("busName")

                if i < len(product):
                    p = product[i]
                    appendOne["standGoodsId"] = p.get("standGoodsId")
                    appendOne["installPackages"] = p.get("installPackages")
                    appendOne["standName"] = p.get("standName")
                    appendOne["standGoodsId"] = p.get("standGoodsId")
                else:
                    appendOne["standGoodsId"] = ""
                    appendOne["installPackages"] = ""
                    appendOne["standName"] = ""
                    appendOne["standGoodsId"] = ""
                forProduct.append(appendOne)
        else:
            for i,p in enumerate(product):
                appendOne = {}
                appendOne["standGoodsId"] = p.get("standGoodsId")
                appendOne["installPackages"] = p.get("installPackages")
                appendOne["standName"] = p.get("standName")
                appendOne["standGoodsId"] = p.get("standGoodsId")

                if i < len(sproduct):
                    sp = sproduct[i]
                    appendOne["customerProductCode"] = sp.get("busGoodsId")
                    appendOne["customerProductName"] = sp.get("busName")
                else:
                    appendOne["customerProductCode"] = ""
                    appendOne["customerProductName"] = ""
                forProduct.append(appendOne)


        for p in forProduct:

            addOne = copy.copy(addOneMain);

            standGoodsId = p.get("standGoodsId")
            customerProductName = p.get("customerProductName")
            if standGoodsId != "":

                # 单品安装单价
                sf = getStandFee(standGoodsId)
                installFee = sf.get("installFee") if sf else None
                addOne["installFee"] = installFee

                # 单品安装件数
                installPackages = p.get("installPackages")
                addOne["installPackages"] = installPackages

                # 单品安装总价
                if not installFee and not installPackages:
                    totalInstallFeeNew = installPackages*installFee
                    addOne["totalInstallFeeNew"] = totalInstallFeeNew

                # 开单总安装费
                addOne["totalInstallFeeOld"] = totalInstallFeeOld

                # 品名
                addOne["standName"] = p.get("standName")

            if customerProductName != "":
                # 商家编号
                addOne["customerProductCode"] = p.get("customerProductCode")
                # 商家品名
                addOne["customerProductName"] = p.get("customerProductName")

            addMany.append(addOne);
        if len(addMany) == 0:
            print(bill.get("waybillId"))
            break;


        billOp_localhost.insert_many(addMany)

def getStandFee(code):
    feeOp = mc.get_col_op_prod("priceManage", "fee")
    feeOne = feeOp.find_one({"feeType":"installFee","groupName":"标准","keys.products.productId":code})
    if feeOne is None:
        return None;
    installFee = 0
    minFee = 0;
    vars = feeOne.get("vars")
    for s in vars:
        varName = s.get("varName")
        if varName == "$安装单价":
            installFee = s.get("varValue")
        if varName == "$最低一票":
            minFee = s.get("varValue")
    return {"installFee":installFee,"minFee":minFee}

def load1():
    load("2018-11-01 00:00:00", "2018-11-10 00:00:00");
threading.Thread(target=load1,name=("thread-%s",1)).start();

def load2():
    load("2018-11-10 00:00:00", "2018-11-15 00:00:00");
threading.Thread(target=load2,name=("thread-%s",2)).start();

def load3():
    load("2018-11-15 00:00:00", "2018-11-20 00:00:00");
threading.Thread(target=load3,name=("thread-%s",3)).start();

def load4():
    load("2018-11-20 00:00:00", "2018-11-25 00:00:00");
threading.Thread(target=load4,name=("thread-%s",4)).start();

def load5():
    load("2018-11-25 00:00:00", "2018-12-01 00:00:00");
threading.Thread(target=load5,name=("thread-%s",4)).start();



print("------------------------------------finish-------------------------------------------------")

# aa = [{"a":1},{"b","zhuzhu"}]
# bb = [{"a":1},{"b","zhuzhu"}]
#
# print(aa+bb)
#
# for i,item in enumerate(aa):
#     print(aa[i])
#     print(item)