import datetime
import utils
import mongo_client as mc
def transport_sc_localhost(time):
    billOp = mc.get_col_op_prod("order","waybillMain")
    billOp_localhost = mc.get_col_op("order", "waybillMain")
    list = billOp.find(
        {'billingTime':
             {'$gt':utils.dateTo8(datetime.datetime.strptime(time+" 00:00:00","%Y-%m-%d %H:%M:%S")),'$lt':utils.dateTo8(datetime.datetime.strptime(time+" 23:59:59","%Y-%m-%d %H:%M:%S"))}})
    i = 0;
    for one in list:
        i = i+1
        print(i)
        localOne = billOp_localhost.find_one({"_id":one.get("_id")})
        if not localOne:
            billOp_localhost.insert_one(one)
        print(not localOne)



transport_sc_localhost("2019-04-07")

# time = "2019-04-03"
# billOp = mc.get_col_op("order","waybillMain")
# count = billOp.count_documents(
#         {'billingTime':
#              {'$gt':utils.dateTo8(datetime.datetime.strptime(time+" 00:00:00","%Y-%m-%d %H:%M:%S")),'$lt':utils.dateTo8(datetime.datetime.strptime(time+" 23:59:59","%Y-%m-%d %H:%M:%S"))}})
# print(count)