import yaml
import os
path = os.path.abspath(os.path.dirname(__file__))
file = open(path+'/config/config.yml')
r = yaml.load(file)

def get_mongo_config():
    return r.get("mongo")

def get_mysql_config():
    return r.get("mysql")