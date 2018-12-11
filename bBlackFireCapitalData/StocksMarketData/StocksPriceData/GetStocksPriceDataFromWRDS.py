# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:07:09 2018
@author: Utilisateur
"""
import datetime

import mongobackup
import multiprocessing
import collections

import motor
import pymongo
import tornado
import wrds

from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import secondary_processor, TestNoneValue

table = collections.namedtuple('table', [
    'value', "position", "globalWRDS", "connectionstring"
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

        ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)

        tab = params.value
        d = dict()

        for res in tab:

            gvkey = res[0]
            date = res[1]
            yr = str(date.year)
            if date.month < 10:
                month = "0" + str(date.month)
            else:
                month = str(date.month)
            if date.day < 10:
                day = "0" + str(date.day)
            else:
                day = str(date.day)

            date_str = yr + '-' + month + '-' + day
            date = datetime.datetime(date.year, date.month, date.day, 16, 0, 0, 0)

            ajex = TestNoneValue(res[3], 1)
            csho = TestNoneValue(res[4], 0)
            vol = TestNoneValue(res[5], 0)
            prccd = TestNoneValue(res[6], 0)
            prchd = TestNoneValue(res[7], 0)
            prcld = TestNoneValue(res[8], 0)
            curcdd = res[9]
            isin = res[11]

            "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
            "'price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"

            data = {'_id': isin, 'gvkey': gvkey,'date': date, 'curr': curcdd, 'csho': csho, 'vol': vol, 'adj_factor': ajex,
                    'price_close': prccd, 'price_high': prchd, 'price_low': prcld, 'return': 0, 'return_usd': 0,
                    'curr_to_USD': None, 'consensus': {}, 'price_target': {}}

            if date_str in d:
                d[date_str].append(data)
            else:
                d[date_str] = [data]

        for key in d:
            tornado.ioloop.IOLoop.current().run_sync(
                StocksMarketDataPrice(ClientDB, key, d[key]).SetStocksPriceInDB)

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
        pt += table(value=res[start:end, :], position=params.offset, globalWRDS=params.globalWRDS,
                    connectionstring=params.connectionstring),

    pool = multiprocessing.Pool(processes=secondary_processor)
    result = pool.map(SetStockPriceDataInDB, pt)
    print(result)
    pool.close()
    pool.join()

    return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Completed"


def ConvertStocksPriceToUSD(params):

    date = params.date
    ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)

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

    ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)

    tab = params.value
    d = dict()

    for res in tab:

        gvkey = res[0]
        date = res[1]
        yr = str(date.year)
        if date.month < 10:
            month = "0" + str(date.month)
        else:
            month = str(date.month)
        if date.day < 10:
            day = "0" + str(date.day)
        else:
            day = str(date.day)

        date_str = yr + '-' + month + '-' + day
        date = datetime.datetime(date.year, date.month, date.day, 16, 0, 0, 0)

        ajex = TestNoneValue(res[3], 1)
        csho = TestNoneValue(res[4], 0)
        vol = TestNoneValue(res[5], 0)
        prccd = TestNoneValue(res[6], 0)
        prchd = TestNoneValue(res[7], 0)
        prcld = TestNoneValue(res[8], 0)
        curcdd = res[9]
        isin = res[11]

        "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
        "'price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"

        data = {'_id': isin, 'gvkey': gvkey,'date': date, 'curr': curcdd, 'csho': csho, 'vol': vol, 'adj_factor': ajex,
                'price_close': prccd, 'price_high': prchd, 'price_low': prcld, 'return': 0, 'return_usd': 0,
                'curr_to_USD': None, 'consensus': {}, 'price_target': {}}

        if date_str in d:
            d[date_str].append(data)
        else:
            d[date_str] = [data]

    for key in d:
        tornado.ioloop.IOLoop.current().run_sync(
                StocksMarketDataPrice(ClientDB, key, d[key]).SetStocksPriceInDB)

    ClientDB.close()
    return 'lot : [', params.position, "] Completed"
