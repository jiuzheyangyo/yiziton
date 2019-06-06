import math
from bson import objectid, Timestamp
import pandas as pd

import numpy as np

import mongo_client


def importStandInstallFee():

    fee_op = mongo_client.get_col_op_test("priceManage","purchaseFee");

    df = pd.read_excel("C:/Users/zhu/Desktop/导入导出/采购价/安装费-标准-模板.xlsx")
    dataList = np.array(df).tolist()
    addList = []
    for data in dataList:
        print(data)
        one = {}

        one["_id"] = str(objectid.ObjectId())
        one["groupId"] = "5ce3bfcaeeeb414fbc90831e"
        one["groupName"] = "标准"
        one["priority"] = 1
        one["feeType"] = "installFee"
        one["name"] = data[0]
        one["template"]  = "费用=max(安装件数*$安装单价,$最低一票);返回 费用;"


        keys = {"products":[{"productId":data[3],"productName":data[4],"productType":"standardProduct"}]}
        one["keys"] = keys
        one["vars"] = [{"varName":"$安装单价","varValue":data[5]},{"varName":"$最低一票","varValue":data[6]}]

        one["createTime"] = Timestamp(1559098387, 253)
        one["lastUpdateTime"] = Timestamp(1559785785, 272)
        one["version"] = 1
        one["creator"] = "15000000000"
        one["creatorName"] = "测试九江管理员"
        one["modifier"] = "15000000000"
        one["modifierName"] = "测试九江管理员"

        addList.append(one)
    fee_op.insert_many(addList)

def importStandComboInstallFee():

    fee_op = mongo_client.get_col_op_test("priceManage","purchaseFee");

    df = pd.read_excel("C:/Users/zhu/Desktop/导入导出/采购价/安装费-标准-模板.xlsx")
    dataList = np.array(df).tolist()
    addList = []
    for data in dataList:
        print(data)
        one = {}

        one["_id"] = str(objectid.ObjectId())
        one["groupId"] = "5cf77b79dd77c768c8103f90"
        one["groupName"] = "标准套餐"
        one["priority"] = 2
        one["feeType"] = "installFee"
        one["name"] = data[0]
        one["template"]  = "费用 = $安装单价*安装件数;返回 费用;"


        keys = {"combos":[{"comboId":data[3],"comboName":data[4]}]}
        one["keys"] = keys
        one["vars"] = [{"varName":"$安装单价","varValue":data[5]}]

        one["createTime"] = Timestamp(1559098387, 253)
        one["lastUpdateTime"] = Timestamp(1559785785, 272)
        one["version"] = 1
        one["creator"] = "15000000000"
        one["creatorName"] = "测试九江管理员"
        one["modifier"] = "15000000000"
        one["modifierName"] = "测试九江管理员"

        addList.append(one)
    fee_op.insert_many(addList)

def importStandDeliverFee():

    fee_op = mongo_client.get_col_op_test("priceManage","purchaseFee");
    area_op = mongo_client.get_col_op_prod("baseConfig", "area")

    df = pd.read_excel("C:/Users/zhu/Desktop/导入导出/采购价/送货费-标准-模板.xlsx")
    dataList = np.array(df).tolist()
    addList = []
    errorList = []
    for data in dataList:
        print(data)
        one = {}

        one["_id"] = str(objectid.ObjectId())
        one["groupId"] = "5ce3c643eeeb414fbc908323"
        one["groupName"] = "标准"
        one["priority"] = 1
        one["feeType"] = "basicDeliveryFee"
        one["name"] = data[0]
        one["template"]  = "费用 = $基础送货费;返回 费用；"


        area = area_op.find_one({"mergerName":data[3]+data[4]+data[5]})
        if area is None:
            errorList.append(data);
            continue
        keys = {"destinations":[{"destinationId":area.get("code"),"destinationName":area.get("mergerName")}]}
        one["keys"] = keys
        one["vars"] = [{"varName":"$基础送货费","varValue":data[6]}]

        one["createTime"] = Timestamp(1559098387, 253)
        one["lastUpdateTime"] = Timestamp(1559785785, 272)
        one["version"] = 1
        one["creator"] = "15000000000"
        one["creatorName"] = "测试九江管理员"
        one["modifier"] = "15000000000"
        one["modifierName"] = "测试九江管理员"

        addList.append(one)
    if addList:
        fee_op.insert_many(addList)
    if errorList:
        el = pd.DataFrame(errorList);
        el.to_excel("error_sd.xlsx",index=False)
def importStandComboDeliverFee():

    fee_op = mongo_client.get_col_op_test("priceManage","purchaseFee");
    area_op = mongo_client.get_col_op_prod("baseConfig", "area")

    df = pd.read_excel("C:/Users/zhu/Desktop/导入导出/采购价/送货费-标准套餐-模板.xlsx")
    dataList = np.array(df).tolist()
    addList = []
    errorList = []
    for data in dataList:
        print(data)
        one = {}

        one["_id"] = str(objectid.ObjectId())
        one["groupId"] = "5ce3c643eeeb414fbc908323"
        one["groupName"] = "标准套餐"
        one["priority"] = 2
        one["feeType"] = "basicDeliveryFee"
        one["name"] = data[0]
        one["template"]  = "费用 = $基础送货费;返回 费用；"


        area = area_op.find_one({"mergerName":data[3]+data[4]+data[5]})

        if area is None:
            errorList.append(data);
            continue
        keys = {
            "combos": [{"comboId": data[6], "comboName": data[7]}],
            "destinations":[{"destinationId":area.get("code"),"destinationName":area.get("mergerName")}]}
        one["keys"] = keys
        one["vars"] = [{"varName":"$基础送货费","varValue":data[8]}]

        one["createTime"] = Timestamp(1559098387, 253)
        one["lastUpdateTime"] = Timestamp(1559785785, 272)
        one["version"] = 1
        one["creator"] = "15000000000"
        one["creatorName"] = "测试九江管理员"
        one["modifier"] = "15000000000"
        one["modifierName"] = "测试九江管理员"

        addList.append(one)
    fee_op.insert_many(addList)

    el = pd.DataFrame(errorList);
    el.to_excel("error_scd.xlsx", index=False)
importStandDeliverFee();
