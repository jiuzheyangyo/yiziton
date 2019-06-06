import mongo_client as mc
from bson import objectid

def transform():
    feeOp = mc.get_col_op_prod("priceManage","fee")
    list = feeOp.find({"feeType":"transportFee","groupName":"协议","keys.startWebsites.websiteId":"00003085","name":{"$ne":"喜梦宝协议运费"},"keys.customers":{"$elemMatch":{"customerId":{"$ne":"KH20170824585"}}}})
    # one = feeOp.find_one({"_id" : "5bc1988214776700073735d0"})
    i = 0
    for one in list:
        i = i+1
        print(i)
        newOne = one;
        _id = str(objectid.ObjectId());
        print(_id)
        newOne["_id"] = _id
        newOne["keys"]["startWebsites"] = [{"websiteId":"201710002","websiteName" : "智通一号"}];
        feeOp.insert_one(newOne)
transform()




