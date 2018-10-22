# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 23:57:38 2018

@author: Utilisateur
"""
import wrds
import pymongo


def set_currency_gbp():
    
    db = wrds.Connection()
    count = db.get_row_count(library ="comp",
                             table = "g_exrt_mth")
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Blackfire_Capital"]
    mycur = mydb['currency']
    
    entete = ['datadate', 'fromcurm', 'tocurm', 'exratm', 'exrat1m']
    
    res = db.get_table(library ="comp",
                       columns = entete,
                       table = "g_exrt_mth")
    
    for pos in range(count):
        
        date = res[entete[0]][pos]
        date = str(date.year) + 'M' + str(date.month)
        fr = res[entete[1]][pos]
        to = res[entete[2]][pos]
        rate = res[entete[3]][pos]
        ratem = res[entete[4]][pos]
        
        cur = mycur[fr]
        data_date = cur[date]
        data = {"_id":to, "rate":rate, "rate1m":ratem}
        
        try:
            data_date.insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            myquery = { "_id": data["_id"] }
            newvalue = {"$set": data}
            data_date.update_one(myquery, newvalue)
              
    myclient.close()
    db.close()

def set_currency_usd():
    
    db = wrds.Connection()
    count = db.get_row_count(library ="ibes",
                             table = "hsxrat")
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Blackfire_Capital"]
    mycur = mydb['currency']
    
    entete = ['pubdats', 'exrat', 'curr']
    
    res = db.get_table(library ="ibes",
                       columns = entete,
                       table = "hsxrat")
    
    for pos in range(count):
        
        date = res[entete[0]][pos]
        date = str(date.year) + 'M' + str(date.month)
        rate = res[entete[1]][pos]
        to = res[entete[2]][pos]
        
        cur = mycur['USD']
        data_date = cur[date]
        data = {"_id":to, "rate":rate}
        
        try:
            data_date.insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            myquery = { "_id": data["_id"] }
            newvalue = {"$set": data}
            data_date.update_one(myquery, newvalue)
              
    myclient.close()
    db.close()

def set_currency_euro():
    
    db = wrds.Connection()
    count = db.get_row_count(library ="ibes",
                             table = "eurx")
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Blackfire_Capital"]
    mycur = mydb['currency']
    
    entete = ['sdates', 'curcodef', 'exchrate']
    
    res = db.get_table(library ="ibes",
                       columns = entete,
                       table = "eurx")
    
    for pos in range(count):
        
        date = res[entete[0]][pos]
        date = str(date.year) + 'M' + str(date.month)
        rate = res[entete[2]][pos]
        to = res[entete[1]][pos]
        
        cur = mycur['EUR']
        data_date = cur[date]
        data = {"_id":to, "rate":rate}
        
        try:
            data_date.insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            myquery = { "_id": data["_id"] }
            newvalue = {"$set": data}
            data_date.update_one(myquery, newvalue)
              
    myclient.close()
    db.close()

