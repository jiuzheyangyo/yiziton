import pymongo
import config
# client = config.get_mongo_client()

def getClient(env = "prod"):
    return config.get_mongo_client(env)

def get_col_op(dbName,colName,check_db=True,check_col=True,env = "localhost"):
    client = getClient(env)
    dbNames = client.list_database_names()
    db_flag = False

    for x in dbNames:
        if(dbName == x):
            db_flag = True
    if(not db_flag & check_db):
        raise NameError("db of %s is no exists" % dbName)

    db = client[dbName]
    colNames = db.list_collection_names()
    col_flag = False
    for x in colNames:
        if (colName == x):
            col_flag = True
    if(not col_flag & check_col):
        raise NameError("col of %s is no exists" % colName)
    col_op = db[colName]
    return col_op

def get_col_op_prod(dbName,colName,check_db=True,check_col=True):
    return get_col_op(dbName,colName,check_db,check_col,"prod")
def get_col_op_test(dbName,colName,check_db=True,check_col=True):
    return get_col_op(dbName,colName,check_db,check_col,"test")
