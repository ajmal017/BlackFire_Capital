# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:07:09 2018
@author: Utilisateur
"""
import datetime

import mongobackup
import multiprocessing
import collections
import pymongo
import wrds

from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import secondary_processor, TestNoneValue

table = collections.namedtuple('table', [
    'value', "position", "globalWRDS",
])

def GetStocksPriceData(params):

    db = wrds.Connection()

    global SetStockPriceDataInDB

    if params.globalWRDS:
        entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'fic', 'isin']
    else:
        entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'fic', 'cusip']

    res = db.get_table(library=params.library,
                       table=params.table,
                       columns=entete,
                       obs=params.observation,
                       offset=params.offset)
    db.close()

    def SetStockPriceDataInDB(params):

        ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

        tab = params.value
        d = dict()
        d_vol = dict()
        d_prcld = dict()
        d_prchd = dict()

        for res in tab:

            gvkey = res[0]
            date = res[1]
            date = str(date.year) + 'M' + str(date.month)
            conm = res[2]
            ajex = TestNoneValue(res[3], 1)
            csho = TestNoneValue(res[4], 0)
            vol = TestNoneValue(res[5], 0)
            prccd = TestNoneValue(res[6], 0)
            prchd = TestNoneValue(res[7], 0)
            prcld = TestNoneValue(res[8], 100000000)
            curcdd = res[9]
            fic = res[10]
            isin = res[11]
            #ret = res[entete[12]]

            if d_vol.get((date, isin), False):
                d_vol[(date, isin)] += vol
            else:
                d_vol[(date, isin)] = vol

            if d_prchd.get((date, isin), False):
                d_prchd[(date, isin)] = max(d_prchd[(date, isin)], prchd)
            else:
                d_prchd[(date, isin)] = prchd

            if d_prcld.get((date, isin), False):
                d_prcld[(date, isin)] = min(d_prcld[(date, isin)], prcld)
            else:
                d_prcld[(date, isin)] = prcld

            d[(date, isin)] = [gvkey, curcdd, csho, vol, ajex, prccd, prchd,
                                   prcld, conm, fic, res[1]]

        for key in d:

            date = key[0]
            isin = key[1]

            gvkey = d[key][0]
            curcdd = d[key][1]
            csho = d[key][2]
            vol = d[key][3]
            ajex = d[key][4]
            prccd = d[key][5]
            prchd = d[key][6]
            prcld = d[key][7]

            if params.globalWRDS == False:

                data = {'_id': gvkey, 'company name': d[key][8], 'incorporation location': d[key][9], 'naics': None,
                       'sic': None, 'gic sector': None, 'gic ind': None, 'eco zone': None,
                       'stock identification': None}
                StocksMarketDataInfos(ClientDB, data).SetDataInDB()

            "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
            "'price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"

            date_str = str(d[key][10].year) + '-' + str(d[key][10].month) + '-' + str(d[key][10].day)
            date_ = datetime.datetime.strptime(date_str, "%Y-%m-%d")

            data = {'_id': isin,'date': date_, 'curr': curcdd, 'csho': csho, 'vol': vol, 'adj_factor':ajex,
                    'price_close': prccd, 'price_high': prchd, 'price_low': prcld, 'return': 0, 'return_usd': 0,
                    'curr_to_USD': None, 'consensus': {}, 'price_target': {}}

            StocksMarketDataPrice(ClientDB,date,data).SetStocksPriceInDB()
        ClientDB.close()
        return 'lot : [', params.position, "] Completed"

    res = res.values
    count = res.shape[0]
    observ = 200000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1

    pt = ()
    for v in range(iter):

        start = v * observ
        end = (v + 1) * observ
        if end > count:
            end = count
        pt += table(value=res[start:end, :], position=params.offset, globalWRDS=params.globalWRDS),

    pool = multiprocessing.Pool(processes=secondary_processor)
    result = pool.map(SetStockPriceDataInDB, pt)
    print(result)
    pool.close()
    pool.join()

    return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Completed"


def ConvertStocksPriceToUSD(params):

    date = params.date
    ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

    list_sp = StocksMarketDataPrice(ClientDB,date, {}, None).GetStocksPriceFromDB()
    print(date)
    for stocks in list_sp:
        id = stocks['_id']
        curr = stocks['curr']
        try:
            tab_rate = CurrenciesExchangeRatesData(ClientDB,{'from':'USD', '_id': curr + "_" + date}, None)\
                .GetExchangeRatesFromDB()
            for value in tab_rate:
                StocksMarketDataPrice(ClientDB, date, id, {'curr_to_USD': 1/value['rate']}).UpdateStocksPriceInDB()
        except TypeError:
            a = 0
            # print("Curreny is None for ", stocks["_id"], date)

    ClientDB.close()


def SetStockPriceDataInDB(params):

        ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

        tab = params.value
        d = dict()
        d_vol = dict()
        d_prcld = dict()
        d_prchd = dict()

        for res in tab:

            gvkey = res[0]
            date = res[1]
            date = str(date.year) + 'M' + str(date.month)
            conm = res[2]
            ajex = TestNoneValue(res[3], 1)
            csho = TestNoneValue(res[4], 0)
            vol = TestNoneValue(res[5], 0)
            prccd = TestNoneValue(res[6], 0)
            prchd = TestNoneValue(res[7], 0)
            prcld = TestNoneValue(res[8], 100000000)
            curcdd = res[9]
            fic = res[10]
            isin = res[11]
            #ret = res[entete[12]]

            if d_vol.get((date, isin), False):
                d_vol[(date, isin)] += vol
            else:
                d_vol[(date, isin)] = vol

            if d_prchd.get((date, isin), False):
                d_prchd[(date, isin)] = max(d_prchd[(date, isin)], prchd)
            else:
                d_prchd[(date, isin)] = prchd

            if d_prcld.get((date, isin), False):
                d_prcld[(date, isin)] = min(d_prcld[(date, isin)], prcld)
            else:
                d_prcld[(date, isin)] = prcld

            d[(date, isin)] = [gvkey, curcdd, csho, vol, ajex, prccd, prchd,
                                   prcld, conm, fic, res[1]]

        for key in d:

            date = key[0]
            isin = key[1]

            gvkey = d[key][0]
            curcdd = d[key][1]
            csho = d[key][2]
            vol = d[key][3]
            ajex = d[key][4]
            prccd = d[key][5]
            prchd = d[key][6]
            prcld = d[key][7]

            if params.globalWRDS == False:

                data = {'_id': gvkey, 'company name': d[key][8], 'incorporation location': d[key][9], 'naics': None,
                       'sic': None, 'gic sector': None, 'gic ind': None, 'eco zone': None,
                       'stock identification': None}
                StocksMarketDataInfos(ClientDB, data).SetDataInDB()

            "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
            "'price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"
            date_str = str(d[key][10].year) + '-' + str(d[key][10].month) + '-' + str(d[key][10].day)
            date_ = datetime.datetime.strptime(date_str, "%Y-%m-%d")

            data = {'_id': isin,'date': date_, 'curr': curcdd, 'csho': csho, 'vol': vol, 'adj_factor':ajex,
                    'price_close': prccd, 'price_high': prchd, 'price_low': prcld, 'return': 0, 'return_usd': 0,
                    'curr_to_USD': None, 'consensus': {}, 'price_target': {}}

            StocksMarketDataPrice(ClientDB,date,data).SetStocksPriceInDB()
        ClientDB.close()
        return 'lot : [', params.position, "] Completed"

