# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:07:09 2018
@author: Utilisateur
"""
import datetime
import multiprocessing
import collections

import motor
import tornado
import wrds
from sqlalchemy import exc
from pymongo import InsertOne, UpdateOne
from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import secondary_processor, TestNoneValue

table = collections.namedtuple('table', [
    'value', "position", "connectionstring","Global",
])

OBSERVATION = 200000

def GetStocksPriceData(params):

    db = wrds.Connection()
    global SetStockPriceDataInDB

    if params.globalWRDS:
        entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'fic', 'isin', 'iid']
    else:
        entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'fic', 'cusip', 'iid']
    try:
        res = db.get_table(library=params.library,
                           table=params.table,
                           columns=entete,
                           obs=params.observation,
                           offset=params.offset)
    except exc.SQLAlchemyError:
        return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Not downloaded"
    db.close()

    def SetStockPriceDataInDB(params):

        ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)

        table = params.value
        tab_price = []
        for res in table:

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
            iid = res[12]

            try:
                ID = isin+"_"+date_str + "_" + curcdd

                "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
                "'price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"

                data = {'isin_or_cusip': isin, 'gvkey': gvkey,'date': date, 'curr': curcdd, 'csho': csho, 'vol': vol, 'adj_factor': ajex,
                        'price_close': prccd, 'price_high': prchd, 'price_low': prcld, 'return': 0, 'return_usd': 0,
                        'curr_to_USD': None,'iid': iid,'global':params.Global, 'consensus': {},
                        'price_target': {}}

                tab_price.append(InsertOne(data))

            except TypeError:
                "Currency is None"

        tornado.ioloop.IOLoop.current().run_sync(
                    StocksMarketDataPrice(ClientDB, "ALL", tab_price).SetStocksPriceInDB)

        ClientDB.close()
        return 'lot : ', params.position, " Completed"

    count = res.shape[0]
    iter = int(count/OBSERVATION) if count % OBSERVATION == 0 else int(count/OBSERVATION) + 1
    res =res.values

    pt = ()
    for v in range(iter):
        start = v * OBSERVATION
        end = (v + 1) * OBSERVATION
        if end > count:
            end = count
        pt += table(value= res[start: end], position=[params.offset + start,params.offset + end],
                    connectionstring=params.connectionstring, Global=params.globalWRDS),

    pool = multiprocessing.Pool(5)
    result = pool.map(SetStockPriceDataInDB, pt)
    pool.close()
    pool.join()
    print('lot : [', params.offset, ", ", params.observation + params.offset, "]")

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

    print(params)
    ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)

    table = params.value
    tab_price = []
    for res in range(len(table)):

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

        try:
            ID = isin+"_"+date_str + "_" + curcdd

            "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
            "'price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"

            data = {'_id': ID, 'isin': isin, 'gvkey': gvkey,'date': date, 'curr': curcdd, 'csho': csho, 'vol': vol, 'adj_factor': ajex,
                    'price_close': prccd, 'price_high': prchd, 'price_low': prcld, 'return': 0, 'return_usd': 0,
                    'curr_to_USD': None, 'consensus': {}, 'price_target': {}}


            tab_price.append(UpdateOne({"_id": ID},data,upsert=True))

        except TypeError:
            "Currency is None"

    tornado.ioloop.IOLoop.current().run_sync(
                StocksMarketDataPrice(ClientDB, "ALL", tab_price).SetStocksPriceInDB)

    ClientDB.close()
    return 'lot : ', params.position, " Completed"
