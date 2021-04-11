import configparser
import mysql.connector

config = configparser.ConfigParser()
config.read('config.ini')

def connect():
    return mysql.connector.connect(host = config['mysqlDB']['host'],
                           user = config['mysqlDB']['user'],
                           passwd = config['mysqlDB']['pass'],
                           db = config['mysqlDB']['db'])
