# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 21:53:26 2018

@author: GhislainNoubissie
"""
import pymongo
import numpy as np


class data:

    def __init__(self, database, *data, **Query):

        "Data is t the object Stocks_data to add/update in the db"
        "Query is a table of [Date,query,Value_to_return, value to sort]"
        self.database = database
        self.data = data
        self.query = Query

    def description(self):

        print('Cette classe permet de sauvegarder des donnees dans la base de' +
              'donnee. Elle comprend 2 parametres. le nom de la table et une ' +
              'de type StocksPriceData.')

    def add_stock_data(value):

        for stocks_dat in value.data:

            data = stocks_dat['data']
            data_date = value.database['value']

            try:
                if data['csho'] is not None:
                    if np.isnan(data['csho']) == True:
                        data['csho'] = 0
                else:
                    data['csho'] = 0

                if data['vol'] is not None:
                    if np.isnan(data['vol']) == True:
                        data['vol'] = 0
                else:
                    data['vol'] = 0

                if data['price_close'] is not None:
                    if np.isnan(data['price_close']) == True:
                        data['price_close'] = 0
                else:
                    data['price_close'] = 0

                if data['price_high'] is not None:
                    if np.isnan(data['price_high']) == True:
                        data['price_high'] = 0
                else:
                    data['price_high'] = 0

                if data['price_low'] is not None:
                    if np.isnan(data['price_low']) == True:
                        data['price_low'] = 100000000
                else:
                    data['price_low'] = 100000000

                data_date.insert_one(data)

            except pymongo.errors.DuplicateKeyError:

                myquery = {"_id": data["_id"]}
                mydoc = data_date.find_one(myquery)

                if data['csho'] is not None:
                    if np.isnan(data['csho']) == False:
                        mydoc['csho'] = data['csho']

                if data['vol'] is not None:
                    if np.isnan(data['vol']) == False:
                        mydoc['vol'] += data['vol']

                if data['price_close'] is not None:
                    if np.isnan(data['price_close']) == False:
                        mydoc['price_close'] = data['price_close']

                if data['price_high'] is not None:
                    if np.isnan(data['price_high']) == False:
                        mydoc['price_high'] = max(data['price_high'], mydoc['price_high'])

                if data['price_low'] is not None:
                    if np.isnan(data['price_low']) == False:
                        mydoc['price_low'] = min(data['price_low'], mydoc['price_low'])

                newvalue = {"$set": mydoc}
                data_date.update_one(myquery, newvalue)

    def get_stock_data(value):

        if len(value.query) != 4:
            return "Query must have 4 values"
        Date = value.query[0]

        stocks_db = value.database["StocksPriceData"]
        data_date = stocks_db[Date]

        return data_date.find(value.query[1], value.query[2]).sort(value.query[3], 1)


class infos:

    def __init__(self, database, *data, **query):

        self.database = database
        self.data = data
        self.query = query

    def add_stock_infos(value):

        infos_db = value.database['value']

        for infos in value.data:

            try:
                infos_db.insert_one(infos)
            except pymongo.errors.DuplicateKeyError:

                myquery = {"_id": infos["_id"]}
                mydoc = infos_db.find_one(myquery)

                if 'company name' in mydoc:
                    mydoc['company name'] = infos['company name'] \
                        if infos['company name'] != None \
                        else mydoc['company name']
                else:
                    mydoc['company name'] = infos['company name']

                if 'incorporation location' in mydoc:
                    mydoc['incorporation location'] = infos['incorporation location'] \
                        if infos['incorporation location'] != None \
                        else mydoc['incorporation location']
                else:
                    mydoc['incorporation location'] = infos['incorporation location']

                if mydoc.get('naics', False):
                    mydoc['naics'] = infos['naics'] \
                        if infos['naics'] != None \
                        else mydoc['naics']
                else:
                    mydoc['naics'] = infos['naics']

                if mydoc.get('sic', False):
                    mydoc['sic'] = infos['sic'] if infos['sic'] != None else mydoc['sic']
                else:
                    mydoc['sic'] = infos['sic']

                if mydoc.get('gic sector', False):
                    mydoc['gic sector'] = infos['gic sector'] \
                        if infos['gic sector'] != None \
                        else mydoc['gic sector']
                else:
                    mydoc['gic sector'] = infos['gic sector']

                if mydoc.get('gic ind', False):
                    mydoc['gic ind'] = infos['gic ind'] \
                        if infos['gic ind'] != None \
                        else mydoc['gic ind']
                else:
                    mydoc['gic ind'] = infos['gic ind']

                if mydoc.get('eco zone', False):
                    mydoc['eco zone'] = infos['eco zone'] \
                        if infos['eco zone'] != None \
                        else mydoc['eco zone']
                else:
                    mydoc['eco zone'] = infos['eco zone']

                if mydoc.get('stock identification', False):
                    isIn = False
                    for d in mydoc['stock identification']:
                        if d == infos['stock identification'][0]:
                            isIn = True
                    if isIn == False and len(infos['stock identification'][0]) != 0:
                        mydoc['stock identification'].append(infos['stock identification'][0])
                else:
                    mydoc['stock identification'] = infos['stock identification']

                myquery = {"_id": infos["_id"]}
                newvalue = {"$set": mydoc}

                infos_db.update_one(myquery, newvalue)

    def get_stocks_infos(value):

        stocks_db = value.database["StocksPriceData"]
        infos_db = stocks_db['infos']

        if len(value.query) != 3:
            return "Query must have 3 values"

        return infos_db.find(value.query[0], value.query[1]).sort(value.query[1], 1)

# stock_id = [{'isin':'US150','cusip':'15','tic':'000''iid':'01','exg':'15','sedol':'BC00', 'stock_curr':'USD'}]
# a = sf('gv','Apple','USA','10','15','20','201','USD',stock_id)
# print(a.get_info())
# new_info = infos(a.get_info())
# new_info.add_stock_infos()

# stocks_db = mydb["StocksPriceData"]
# infos_db = stocks_db['1997M5']
# print(infos_db.find().count())
# print(json.dumps(infos_db.find_one(),indent=1))


# for x in infos_db.find():
#  print(json.dumps(x, indent = 1))


# myclient.close()
