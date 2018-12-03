# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 21:14:06 2018

@author: GhislainNoubissie
"""
import pandas as pd

df2 = pd.DataFrame([[1,2,3,4,5]],
                   columns=['a', 'b', 'c', 'd', 'e'])
print(df2)

class StocksMarketInfos:
    'This class create an object with the informations of all the StocksPriceData in the db'

    #def __init__(self, database, *data):

    #    self.database = database
    #    self.data = data

    def __init__(self, gvkey, company_name, incorporation_location,
                 naics, sic, gic_sector, gic_ind, eco_zone,
                 stock_indentification):
        self.gvkey = gvkey
        self.company_name = company_name
        self.incorporation_location = incorporation_location
        self.naics = naics
        self.sic = sic
        self.gic_sector = gic_sector
        self.gic_ind = gic_ind
        self.eco_zone = eco_zone
        self.stock_indentification = stock_indentification

    def setDataInDB(self):

        infos_db = self.database['value']
    #def getDataFromDB(self):

    def get_info(x):
        d = dict()

        d['_id'] = x.gvkey
        d['company name'] = x.company_name
        d['incorporation location'] = x.incorporation_location
        d['naics'] = x.naics
        d['sic'] = x.sic
        d['gic sector'] = x.gic_sector
        d['gic ind'] = x.gic_ind
        d['eco zone'] = x.eco_zone
        d['stock identification'] = x.stock_indentification

        return d


class stocks_data:
    'This class create an objet containing StocksPriceData information of a security in a given month'

    def __init__(self, gvkey, date, cusip, curr, csho, vol,
                 adj_factor, price_close,
                 price_high, price_low, ret, ret_usd,
                 curr_to_usd, consensus_info, price_target_info):
        self.gvkey = gvkey
        self.cusip = cusip
        self.date = date
        self.curr = curr
        self.csho = csho
        self.vol = vol
        self.adj_factor = adj_factor
        self.price_close = price_close
        self.price_high = price_high
        self.price_low = price_low
        self.ret = ret
        self.ret_usd = ret_usd
        self.curr_to_usd = curr_to_usd
        self.consensus_info = consensus_info
        self.price_target_info = price_target_info

    def get_info(x):
        d = dict()



        return {'date': x.date, "data": d}


class stocks_consensus:
    def __init__(self, cusip, tic, conm, analyst, recom, date, mask_code):
        self.cusip = cusip
        self.tic = tic
        self.conm = conm
        self.analyst = analyst
        self.recom = recom
        self.date = date
        self.mask_code = str(int(mask_code))

    def get_info(x):
        d = dict()
        d_ = dict()
        d['cusip'] = x.cusip
        d['ticker'] = x.tic
        d['analyst'] = x.analyst
        d['recom'] = float(x.recom)
        d['date_activate'] = x.date
        d['mask_code'] = x.mask_code
        d['variation'] = 0.0

        d_['_id'] = x.cusip
        d_['ticker'] = x.tic
        d_['conm'] = x.conm
        return [d_, d]


class price_target:
    def __init__(self, cusip, tic, conm, analyst, price_target, horizon, currency, date, mask_code):
        self.cusip = cusip
        self.tic = tic
        self.conm = conm
        self.analyst = analyst
        self.price_target = price_target
        self.horizon = horizon
        self.currency = currency
        self.date = date
        self.mask_code = str(int(mask_code))

    def get_info(x):
        d = dict()
        d_ = dict()
        d['cusip'] = x.cusip
        d['ticker'] = x.tic
        d['analyst'] = x.analyst
        d['price'] = x.price_target
        d['horizon'] = x.horizon
        d['curr'] = x.currency
        d['date_activate'] = x.date
        d['mask_code'] = x.mask_code
        d['variation'] = 0
        d['price_usd'] = 0

        d_['_id'] = x.cusip
        d_['ticker'] = x.tic
        d_['conm'] = x.conm
        return [d_, d]


class stocks:
    def __init__(self, infos, data):
        self.infos = infos
        self.data = data

    def get_stocks(value):
        return value
