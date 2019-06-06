import yaml
import os
import pymongo
import pymysql
path = os.path.abspath(os.path.dirname(__file__))

def get_file(env):
    file = open(path+'/config/config_'+env+'.yml')
    r = yaml.load(file)
    return r

def get_mongo_config(env):
    return get_file(env).get("mongo")

def get_mysql_config(env):
    return get_file(env).get("mysql")

def get_mongo_client(env):
    return pymongo.MongoClient(**get_mongo_config(env))

def get_mysql_client(env):
    return pymysql.connect(**get_mysql_config(env))