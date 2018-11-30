# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 17:52:56 2018

@author: Utilisateur
"""
import collections
import multiprocessing
import multiprocessing.pool

import wrds
import pymongo

from bBlackFireCapitalData.StocksMarketData.StocksPriceData import set_table_basic_info
from bBlackFireCapitalData.StocksMarketData.StocksPriceData import set_data_parrallel_mode
from bBlackFireCapitalData.StocksMarketData.StocksPriceRecommendationData.data_from_wrds import set_consensus
from bBlackFireCapitalData.StocksMarketData.StocksPriceRecommendationData.data_from_wrds import set_price_target
from bBlackFireCapitalData.StocksPriceRecommendationData.patch_data import patch_price_target
from bBlackFireCapitalData.StocksPriceRecommendationData.patch_data import patch_consensus
import bBlackFireCapitalData.CountriesEconomicsData.CountriesExchangeRatesData.data_from_db as curr_db

price_info_tup = collections.namedtuple('price_info_tup', [
    'global_',
])

price_tup = collections.namedtuple('price_tup',[
    'table',
    'library',
    'observation',
    'offset',
    'global_',
    'gvkey',
])

price_target_tup = collections.namedtuple('price_target_tup',[
    'table',
    'library',
    'observation',
    'offset',
    'ticker',
])

patch_ibes_tup = collections.namedtuple('patch_ibes_tup', [
    'query',
])

cusip_data = collections.namedtuple('cusip_data', [
    'query',
])

class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

class set_stocks_data_in_db():
    
    def __init__(self, data):
        self.data = data

    def set_all_infos(data):

        p = ()
        p += price_info_tup(global_=False),
        p += price_info_tup(global_=True),
        pool = multiprocessing.Pool()
        result = pool.map(set_table_basic_info, p)
        print(result)

    def set_all_price(data):

        #db = wrds.Connection()
        #count = db.get_row_count(library="comp",
        #                         table="g_secd")
        #db.close()
        #count = 1000000
        #observ = 500000
        #iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
        #pt = ()
        #for v in range(iter):
        #    pt += price_tup(library='comp',
        #                    table='g_secd',
        #                    observation=observ,
        #                    offset=v * observ,
        #                    global_=True),
        #pool = multiprocessing.Pool()
        #result = pool.map(set_price, pt)
        #print(result)

        #db = wrds.Connection()
        #count = db.get_row_count(library="comp",
        #                         table="secd")
        #print(count)
        #db.close()

        observ = 1000000
        #count = 50000000
        #iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1

        pt = ()
        for v in range(100,121):
            pt += price_tup(library='comp',
                            table='secd',
                            observation=observ,
                            offset=v * 1000000,
                            global_=False,
                            gvkey=v),
        print(pt)
        pool = MyPool(2)
        result = pool.map(set_data_parrallel_mode, pt)
        pool.close()
        pool.join()
        print(result)

    def set_all_price_target(data):

        db = wrds.Connection()
        count = db.get_row_count(library="ibes",
                                 table="ptgdet")
        db.close()

        observ = 1000000
        iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
        pt = ()
        tab = ['@MOE', '@SQJ', '@BNP']
        for v in range(iter):
            pt += price_target_tup(library='ibes',
                                   table='ptgdet',
                                   observation=observ,
                                   offset=observ*v,
                                   ticker=v),
        pool = multiprocessing.Pool(processes=2)
        result = pool.map(set_price_target, pt)
        print(result)

    def set_all_consensus(data):

        print('consensus')
        db = wrds.Connection()
        count = db.get_row_count(library="ibes",
                                 table="recddet")
        db.close()
        print(count)
        observ = 1000000
        iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
        pt = ()
        #tab = ['@MOE', '@SQJ', '@BNP']
        for v in range(iter):
            pt += price_target_tup(library='ibes',
                                   table='recddet',
                                   observation=observ,
                                   offset=observ*v,
                                   ticker=v),
        pool = multiprocessing.Pool(processes=2)
        result = pool.map(set_consensus, pt)
        print(result)

    def set_all_currency(data):
        print('set currency')
        curr_db.set_currency_usd()
        curr_db.set_currency_gbp()
        curr_db.set_currency_euro()

    def patch_price_target_data(self):

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        pt_infos = myclient["price_target_infos"].value
        t = ()
        for value in pt_infos.find():
            cusip = value["_id"]
            ticker = value["ticker"]
            t += cusip_data(query=[{"cusip": cusip}, {"ticker": ticker}]),

        pool = multiprocessing.Pool(processes=16)
        pool.map(patch_price_target, t)

    def patch_consensus_data(self):

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        pt_infos = myclient["consensus_infos"].value
        t = ()
        for value in pt_infos.find():
            cusip = value["_id"]
            ticker = value["ticker"]
            t += cusip_data(query=[{"cusip": cusip}, {"ticker": ticker}]),

        pool = multiprocessing.Pool(processes=16)
        pool.map(patch_consensus, t)

a = set_stocks_data_in_db('')


if __name__ == '__main__':

    #myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    #list_db = myclient.list_database_names()
    #print(list_db)
    #for name in list_db:
    #    if name[:9] == "consensus":
    #        if name != "consensus_infos":
    #            myclient.drop_database(name)

    #set_stocks_data_in_db('').set_all_infos()
    set_stocks_data_in_db('').set_all_price()
    #set_stocks_data_in_db('').set_all_currency()
    #set_stocks_data_in_db('').set_all_price_target()
    #set_stocks_data_in_db("").set_all_consensus()
    #set_stocks_data_in_db('').patch_consensus_data()
    #set_stocks_data_in_db('').patch_price_target_data()





