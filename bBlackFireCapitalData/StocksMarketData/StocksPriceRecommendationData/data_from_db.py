# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 20:52:58 2018

@author: Utilisateur
"""
import pymongo


class price_target():

    def __init__(self, database, *data, **query):

        self.database = database
        self.data = data
        self.query = query

    def add_price_target(self):

        for stocks_dat in self.data:

            infos = stocks_dat[0]
            data = stocks_dat[1]

            infos_db = self.database['price_target_infos'].value

            try:
                infos_db.insert_one(infos)
            except pymongo.errors.DuplicateKeyError:
                a = 0
                # print(infos["_id"], "exists")
                # myquery = { "_id": infos["_id"] }
                # newvalue = {"$set": infos}
                # infos_db.update_one(myquery, newvalue)

            date = str(data['date_activate'].year) + 'M' + \
                   str(data['date_activate'].month)

            curr_db = self.database['currency'].USD
            curr_db = curr_db[date]
            curr_rate = curr_db.find_one(data['curr'])
            if curr_rate is not None:
                curr_rate = curr_rate['rate']
                data['price_usd'] = data['price']/curr_rate
            else:
                data['price_usd'] = None
            
            pt = self.database["price_target_" + date].value
            data['date_activate'] = date + 'J' + str(data['date_activate'].day)
            try:
                myquery = {"cusip": data['cusip'], "mask_code": data['mask_code']}
                x_ = pt.find(myquery)
                _ = x_.count()
                if _ > 0:
                    for val in x_:

                        d_ = data['date_activate'][-2:] if data['date_activate'][-2:][0] != 'J' \
                            else data['date_activate'][-1]
                        di_ = val['date_activate'][-2:] if val['date_activate'][-2:][0] != 'J' \
                            else val['date_activate'][-1]

                        if int(d_) > int(di_):
                            myquery = {"_id": val["_id"]}
                            newvalue = {"$set": data}
                            pt.update_one(myquery, newvalue)
                else:
                    pt.insert_one(data)
            except pymongo.errors.DuplicateKeyError:

                myquery = {"_id": data["_id"]}
                newvalue = {"$set": data}
                pt.update_one(myquery, newvalue)

    def patch_target_price(self):

        pt = self.database
        data = self.data
        try:
            pt.insert_one(data)
        except pymongo.errors.DuplicateKeyError:

            myquery = {"_id": data["_id"]}
            newvalue = {"$set": data}
            pt.update_one(myquery, newvalue)

    def get_price_target(self):

        query = self.query
        date = query[0]
        query_ = query[1]
        to_display = query[2]
        by = query[3]

        pt = self.database["price_target_" + date].value
        return pt.find(query_,to_display).sort(by, 1)
            

class consensus():

    def __init__(self, database, *data, **query):

        self.database = database
        self.data = data
        self.query = query

    def add_consensus(self):

        for stocks_dat in self.data:

            infos = stocks_dat[0]
            data = stocks_dat[1]

            infos_db = self.database['consensus_infos'].value



            try:
                infos_db.insert_one(infos)
            except pymongo.errors.DuplicateKeyError:
                a = 0
                # print(infos["_id"], "exists")
                # myquery = { "_id": infos["_id"] }
                # newvalue = {"$set": infos}
                # infos_db.update_one(myquery, newvalue)

            date = str(data['date_activate'].year) + 'M' + \
                   str(data['date_activate'].month)
            pt = self.database["consensus_" + date].value
            data['date_activate'] = date + 'J' + str(data['date_activate'].day)
            try:
                myquery = {"cusip": data['cusip'], "mask_code": data['mask_code']}
                x_ = pt.find(myquery)
                _ = x_.count()
                if _ > 0:
                    for val in x_:

                        d_ = data['date_activate'][-2:] if data['date_activate'][-2:][0] != 'J' \
                            else data['date_activate'][-1]
                        di_ = val['date_activate'][-2:] if val['date_activate'][-2:][0] != 'J' \
                            else val['date_activate'][-1]

                        if int(d_) >= int(di_):#Take the last value of the month
                            myquery = {"_id": val["_id"]}
                            newvalue = {"$set": data}
                            pt.update_one(myquery, newvalue)
                else:
                    pt.insert_one(data)
            except pymongo.errors.DuplicateKeyError:

                myquery = {"_id": data["_id"]}
                newvalue = {"$set": data}
                pt.update_one(myquery, newvalue)