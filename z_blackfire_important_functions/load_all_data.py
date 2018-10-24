# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 17:52:56 2018

@author: Utilisateur
"""
from b_blackfire_data.stocks.stocks_data_from_wrds import set_table_basic_info
from b_blackfire_data.stocks.stocks_data_from_wrds import set_price
from b_blackfire_data.consensus_and_price_target.data_from_wrds import set_consensus
from b_blackfire_data.consensus_and_price_target.data_from_wrds import set_price_target
import b_blackfire_data.currency.data_from_db as curr_db


class set_stocks_data_in_db():
    def __init__(self, data):
        self.data = data

    def set_all_infos(data):
        set_table_basic_info(global_=True)
        set_table_basic_info(global_=False)

    def set_all_price(data):
        set_price(library='comp', table='g_secd', global_=True)
        set_price(library='comp', table='secd', global_=False)

    def set_all_price_target(data):
        print('price target')
        set_price_target()

    def set_all_consensus(data):
        print('consensus')
        set_consensus()

    def set_all_currency(data):
        print('set currency')
        curr_db.set_currency_usd()
        #curr_db.set_currency_gbp()
        #curr_db.set_currency_euro()


a = set_stocks_data_in_db('')
#a.set_all_infos()
#a.set_all_currency()
#a.set_all_price_target()
# a.set_all_consensus()
#a.set_all_price()
import pymongo
import json
from bson.json_util import dumps, CANONICAL_JSON_OPTIONS
from bson import ObjectId

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#a = myclient.list_database_names()
#for x in a:
#    print(x)

mydb = myclient["stocks_2015M9"].value
for x in mydb.find():
    print(x)
print(json.dumps(mydb.find_one(), indent=1))
