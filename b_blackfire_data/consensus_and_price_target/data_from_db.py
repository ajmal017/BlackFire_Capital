# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 20:52:58 2018

@author: Utilisateur
"""
import pymongo

class price_target():
    
    def __init__(self,database,*data):
        
        self.database = database
        self.data = data
        
    def add_price_target(value):
            
        for stocks_dat in value.data:
                
            infos = stocks_dat[0]
            data = stocks_dat[1]
            ibes_db = value.database["ibes"]
            infos_db = ibes_db['infos']
            
            try:
                infos_db.insert_one(infos)
            except pymongo.errors.DuplicateKeyError:
                
                myquery = { "_id": infos["_id"] }
                newvalue = {"$set": infos}
                infos_db.update_one(myquery, newvalue)
            
            date = str(data['date_activate'].year) + 'M'+\
                    str(data['date_activate'].month)
            data['date_activate'] = date + 'J'+ str(data['date_activate'].day)
            add_db = ibes_db[infos["_id"]]
            data_db = add_db[date]
            pt = data_db['price target']
            try:
                pt.insert_one(data)
            except pymongo.errors.DuplicateKeyError:
                
                myquery = { "_id": data["_id"] }
                newvalue = {"$set": data}
                pt.update_one(myquery, newvalue)
            

class consensus():
    
    def __init__(self,database,*data):
        
        self.database = database
        self.data = data
        
    def add_consensus(value):
            
        for stocks_dat in value.data:
                
            infos = stocks_dat[0]
            data = stocks_dat[1]
            ibes_db = value.database["ibes"]
            infos_db = ibes_db['infos']
            
            try:
                infos_db.insert_one(infos)
            except pymongo.errors.DuplicateKeyError:
                
                myquery = { "_id": infos["_id"] }
                newvalue = {"$set": infos}
                infos_db.update_one(myquery, newvalue)
            
            date = str(data['date_activate'].year) + 'M'+\
                    str(data['date_activate'].month)
            data['date_activate'] = date + 'J'+ str(data['date_activate'].day)
            
            add_db = ibes_db[infos["_id"]]
            data_db = add_db[date]
            pt = data_db['consensus']
            try:
                pt.insert_one(data)
            except pymongo.errors.DuplicateKeyError:
                
                myquery = { "_id": data["_id"] }
                newvalue = {"$set": data}
                pt.update_one(myquery, newvalue)