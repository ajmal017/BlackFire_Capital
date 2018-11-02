# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 17:52:56 2018

@author: Utilisateur
"""
from b_blackfire_data.stocks.stocks_data_from_wrds import set_table_basic_info
from b_blackfire_data.stocks.stocks_data_from_wrds import set_price
from b_blackfire_data.consensus_and_price_target.data_from_wrds import set_consensus
from b_blackfire_data.consensus_and_price_target.data_from_wrds import set_price_target
from b_blackfire_data.consensus_and_price_target.patch_data import patch_price_target
from b_blackfire_data.consensus_and_price_target.patch_data import patch_consensus
import b_blackfire_data.currency.data_from_db as curr_db
import collections
import multiprocessing
import wrds

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

        db = wrds.Connection()
        count = db.get_row_count(library="comp",
                                 table="secd")
        db.close()

        observ = 500000
        count = 1000000
        iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
        pt = ()
        gvkey_t = ['014447', '101204', '015532']
        for v in gvkey_t:
            pt += price_tup(library='comp',
                            table='secd',
                            observation=observ,
                            offset=1 * observ,
                            global_=False,
                            gvkey=v),
        pool = multiprocessing.Pool()
        result = pool.map(set_price, pt)
        print(result)

    def set_all_price_target(data):

        db = wrds.Connection()
        count = db.get_row_count(library="ibes",
                                 table="ptgdet")
        db.close()

        observ = 500000
        iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
        pt = ()
        tab = ['@MOE', '@SQJ', '@BNP']
        for v in tab:
            pt += price_target_tup(library='ibes',
                                   table='ptgdet',
                                   observation=observ,
                                   offset=observ,
                                   ticker=v),
        pool = multiprocessing.Pool()
        result = pool.map(set_price_target, pt)
        print(result)


    def set_all_consensus(data):

        print('consensus')
        db = wrds.Connection()
        count = db.get_row_count(library="ibes",
                                 table="recddet")
        db.close()

        observ = 500000
        iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
        pt = ()
        iter = 1
        tab = ['@MOE', '@SQJ', '@BNP']
        for v in tab:
            pt += price_target_tup(library='ibes',
                                   table='ptgdet',
                                   observation=observ,
                                   offset=observ,
                                   ticker=v),
        pool = multiprocessing.Pool()
        result = pool.map(set_consensus, pt)
        print(result)

    def set_all_currency(data):
        print('set currency')
        curr_db.set_currency_usd()
        #curr_db.set_currency_gbp()
        #curr_db.set_currency_euro()

    def patch_price_target_data(self):

        tab = ['@MOE', '@SQJ', '@BNP']
        pt = ()
        for v in tab:
            pt += patch_ibes_tup(query={'ticker': v}),
        print(pt)
        pool = multiprocessing.Pool()
        pool.map(patch_price_target, pt)

    def patch_consensus_data(self):

        tab = ['@MOE', '@SQJ', '@BNP']
        pt = ()
        for v in tab:
            pt += patch_ibes_tup(query={'ticker': v}),
        print(pt)
        pool = multiprocessing.Pool()
        pool.map(patch_consensus, pt)

a = set_stocks_data_in_db('')

if __name__ == '__main__':
    #set_stocks_data_in_db('').set_all_infos()
    #set_stocks_data_in_db('').set_all_price()
    #set_stocks_data_in_db('').set_all_currency()
    set_stocks_data_in_db('').set_all_price_target()
    set_stocks_data_in_db('').patch_price_target_data()
    #set_stocks_data_in_db('').patch_consensus_data()




