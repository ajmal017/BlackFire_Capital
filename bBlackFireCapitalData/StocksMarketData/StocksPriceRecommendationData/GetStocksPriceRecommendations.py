# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 20:05:00 2018

@author: Utilisateur
"""
import motor
import tornado
import wrds
import datetime
from sqlalchemy import exc
import numpy as np
import pandas as pd
from pymongo import InsertOne
import multiprocessing
import collections
from zBlackFireCapitalImportantFunctions.ConnectionString import TestConnectionString, ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import profile
from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target, \
    secondary_processor, GenerateMonthlyTab

table = collections.namedtuple('table', [
    'type',
])
_ACTUAL_ = '_act'
_PREVIOUS_ = '_prev'
params = table(type=type_price_target)

entete = ['anndats', 'curr', 'exrat']

exrate = np.load('exrates.npy')
exrate = pd.DataFrame(exrate,columns=entete)

exr = exrate[(exrate['anndats'].isin([datetime.date(2018,4,11) - datetime.timedelta(days=i) for i in range(1,6)]))
          & (exrate['curr'] == 'ZWK')].sort_values('anndats', ascending=False).reset_index()
print(exr['exrat'])
print(exr['exrat'][0])
def convertDateToString(date):
    return date.strftime('%Y-%m')


def CalculateConsensusVar(gvkey_act, gvkey_prev, mask_code_act, mask_code_prev,date_act,
                            date_prev, recom_act, recom_prev):

    if gvkey_act != gvkey_prev:
        return None
    if mask_code_act != mask_code_prev:
        return None
    if (date_act - date_prev).days > 6*30:
        return None
    return int(recom_act) - int(recom_prev)


def CalculatePriceTargetVar(gvkey_act, gvkey_prev, mask_code_act, mask_code_prev,date_act,
                            date_prev, recom_act, recom_prev, curr_act, curr_prev, exchg_act, exchg_prev, horizon):

    if gvkey_act != gvkey_prev:
        return None
    if mask_code_act != mask_code_prev:
        return None
    if (date_act - date_prev).days > horizon*30:
        return None
    if curr_act == curr_prev:
        try:
            return -1 + recom_act/recom_prev
        except ZeroDivisionError:
            return None
        except TypeError:
            return None
    else:
        try:
            return -1 + (recom_act* exchg_act)/(recom_prev * exchg_prev)
        except ZeroDivisionError:
            return None
        except TypeError:
            return None


def PatchMaskcd(amask, emask):

    if amask == 0 or amask is None or np.isnan(amask):
        return emask
    else:
        return str(amask)


def PatchExrates(exrates, anndats, curr):

    if np.isnan(exrates) or exrates is None or exrates == 0:
        exr = exrate[(exrate['anndats'].isin([anndats - datetime.timedelta(days=i) for i in range(1,6)]))
          & (exrate['curr'] == curr)].sort_values('anndats', ascending=False).reset_index()
        if exr.shape[0] > 0:
            return exr['exrat'][0]
        return None
    else:
        return exrates


def BulkSetConsensusData(ticker, cusip, emaskcd, ireccd, anndats, amaskcd, gvkey, variation):

    return InsertOne({"ticker": ticker, "cusip": cusip, "emasckd": emaskcd,
                      "recom": ireccd, "anndats": datetime.datetime(anndats.year,anndats.month,anndats.day,16,0,0), "amaskcd": amaskcd,
                      "variation": variation, "gvkey": gvkey})


@profile
def GetStocksPriceRecommendations(params):

    if params.type == type_price_target:

        entete = ['ticker', 'cusip', 'estimid', 'horizon', 'value',
                  'estcur', 'anndats', 'amaskcd']
        sqlstmt = 'select pt.*, B.exrat FROM(select ' + ','.join(entete) + ' FROM {schema}.{table}  ' \
                    .format(schema='ibes', table='ptgdet',) +' ) As pt ' \
                    'LEFT JOIN ibes.hdxrati B ON (pt.anndats = B.anndats AND pt.estcur = B.curr) '

    if params.type == type_consensus:
        entete = ['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd']

        sqlstmt = 'select ' + ','.join(entete) + ' From {schema}.{table} '.format(
            schema='ibes',
            table='recddet',
        )

    try:
        db = wrds.Connection()
        res = db.raw_sql(sqlstmt)
        db.close()
        np.save(params.type + '_data', res)

    except exc.SQLAlchemyError as e:
        print(e)
        return "Error Loading File"
    finally:
        db.close()

@profile
def AddGvkeyToTable(params):

    if params.type == type_price_target:
        entete = ['ticker', 'cusip', 'emaskcd', 'horizon', 'value',
                  'estcur', 'anndats', 'amaskcd', 'exrat']
        res = np.load(params.type + '_data.npy')
        res = pd.DataFrame(res, columns=entete)
        v = np.vectorize(PatchMaskcd)
        res['Pamaskcd'] = v(res['amaskcd'], res['emaskcd'])
        print(res.dropna(subset=['exrat']).shape)
        v = np.vectorize(PatchExrates)
        res['Pexrat'] = v(res['exrat'], res['anndats'], res['estcur'])
        print(res.dropna(subset=['exrat']).shape)
        entete[7] = 'Pamaskcd'
        entete[8] = 'Pexrat'

        res = res[entete]

    if params.type == type_consensus:

        entete = ['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd']
        res = np.load(params.type + '_data.npy')
        res = pd.DataFrame(res, columns=entete)
        v = np.vectorize(PatchMaskcd)
        res['Pamaskcd'] = v(res['amaskcd'], res['emaskcd'])
        entete[5] = 'Pamaskcd'
        res = res[entete]


    tabStocksInfosGvkey = np.load('tabStocksInFosGvkey.npy')


    tabStocksInfosGvkey = pd.DataFrame(tabStocksInfosGvkey, columns=['gvkey', 'cusip', 'ticker'])

    CusipFilterTab = res[res['cusip'] != None]
    CusipFilterTab = CusipFilterTab.dropna(subset=['cusip'])

    TickerFilterTab = res[res['ticker'] != None]
    TickerFilterTab = TickerFilterTab.dropna(subset=['ticker'])

    t = tabStocksInfosGvkey[['gvkey', 'cusip',]].drop_duplicates('cusip')
    t = t.dropna(subset=['cusip'])
    CusipFilterTab = pd.merge(CusipFilterTab, t, on='cusip').reset_index()

    t = tabStocksInfosGvkey[['gvkey', 'ticker',]].drop_duplicates('ticker')
    t = t.dropna(subset=['ticker'])
    TickerFilterTab = pd.merge(TickerFilterTab, t, on='ticker').reset_index()

    CusipFilterTab = CusipFilterTab.append(TickerFilterTab)
    entete.append('gvkey')

    CusipFilterTab = CusipFilterTab.drop_duplicates(entete)
    CusipFilterTab = CusipFilterTab[CusipFilterTab['gvkey'] != None]
    CusipFilterTab = CusipFilterTab[entete]

    print(CusipFilterTab.columns)
    np.save(params.type + '_dataWithGVKEY.npy', CusipFilterTab)

AddGvkeyToTable(params)

@profile
def CalculateRecommendationVar(params):

    if params.type == type_price_target:
        entete = ['ticker', 'cusip', 'emaskcd', 'horizon', 'value',
                  'estcur', 'anndats', 'amaskcd', 'exrat', 'gvkey']


    if params.type == type_consensus:
        entete = ['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd', 'gvkey']
        indice_for_var = [6, 5, 4, 3, 2]

    res = np.load(params.type + '_dataWithGVKEY.npy')
    res = pd.DataFrame(res, columns=entete)

    v = np.vectorize(convertDateToString)
    res['date'] = v(res['anndats'])

    res = res.sort_values(["gvkey","amaskcd","date"], ascending=[True, False,False])
    res = res.iloc[:].reset_index(drop=True)
    res_p = res.iloc[1:, indice_for_var].reset_index(drop=True)
    res = res.iloc[:-1]
    res = res.join(res_p, lsuffix=_ACTUAL_, rsuffix=_PREVIOUS_)

    if params.type == type_consensus:
        v = np.vectorize(CalculateConsensusVar)
        res['variation'] = v(res[entete[indice_for_var[0]] + _ACTUAL_], res[entete[indice_for_var[0]] + _PREVIOUS_],
                             res[entete[indice_for_var[1]] + _ACTUAL_], res[entete[indice_for_var[1]] + _PREVIOUS_],
                             res[entete[indice_for_var[2]] + _ACTUAL_], res[entete[indice_for_var[2]] + _PREVIOUS_],
                             res[entete[indice_for_var[3]] + _ACTUAL_], res[entete[indice_for_var[3]] + _PREVIOUS_])

        v = np.vectorize(BulkSetConsensusData)
        res['data'] = v(res['ticker'], res['cusip'], res['emaskcd'+ _ACTUAL_], res['ireccd'+ _ACTUAL_], res['anndats' + _ACTUAL_],
                        res['amaskcd' + _ACTUAL_], res['gvkey' + _ACTUAL_], res['variation'])
        res = res[['date', 'data', 'gvkey' + _ACTUAL_, 'amaskcd' + _ACTUAL_, 'anndats' + _ACTUAL_, 'emaskcd']]

    if params.type == type_price_target:
        v = np.vectorize(CalculatePriceTargetVar)
        res['variation'] = v()

    res = res.sort_values("date", ascending=True).reset_index(drop=True)

    np.save(params.type + "_toSaveInDB", res)

# CalculateRecommendationVar(params)
def SetDataToDB(params):

    if params.type == type_price_target:
        entete = ['ticker', 'cusip', 'estimid', 'horizon', 'value',
                  'estcur', 'anndats', 'amaskcd', 'exrat', 'gvkey']


    if params.type == type_consensus:
        entete = ['date', 'data', 'gvkey', 'amaskcd', 'anndats','emaskcd']

    res = np.load(params.type + "_toSaveInDB.npy")
    res = pd.DataFrame(res, columns= entete)

    tabDate = GenerateMonthlyTab('1993-10', '2018-04')
    tabInFile = []

    for pos in range(len(tabDate)):

        date_end = tabDate[pos]
        pos_begin = pos - 5
        if pos_begin < 0:
            pos_begin = 0
        pos_last = pos - 1
        if pos_last < 0:
            pos_last = 0
        tabInFile.append([date_end, tabDate[pos_last], tabDate[pos_begin]])

    res = res.set_index('date')
    ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    tabDate = GenerateMonthlyTab('1994-03', '19988-02')

    for v in tabDate:
        ClientDB['stocks']['consensus'][v].drop()

    for value in tabInFile[1:]:

        print(value)

        tab = res.loc[value[2]: value[1]]
        tab = tab.sort_values(["gvkey","amaskcd","anndats"], ascending=[True, False,False])
        tab = tab.drop_duplicates(subset=["gvkey","amaskcd", "emaskcd"], keep="first")
        toWrite = list(tab['data'])
        tab = res.loc[value[0]]
        toWrite += list(tab['data'])

        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(PriceTargetAndconsensusValuesData(ClientDB, value[0],params.type, toWrite).SetValuesInDB)
    ClientDB.close()


def ConvertPriceTagetToUSD(params):

    """ This function set the field curr_to_USD for all the price target in the DB.
        curr_to_USD is the exchange rate from the base currency to USD at the date of the
        stocks price target.
         params.connectionstring:  the Connection url to the MongoDB
         params.currency:  the currency to convert

        :return: Status Done
    """

    ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)
    currency = params.currency

    tab_currency = tornado.\
        ioloop.\
        IOLoop.\
        current()\
        .run_sync(CurrenciesExchangeRatesData(ClientDB,{'from':'USD', 'to': currency}, None)
                  .GetExchangeRatesFromDB)

    print(currency, tab_currency[0]['date'], tab_currency[-1]['date'])
    BulkOp = []
    for value in tab_currency:
        if value['date'] > datetime.datetime(1980,1,1):
            try:

                date = value['date']
                rate = "{0:.4f}".format(1/value['rate'])
                query = {"date_activate": date, "curr":currency}
                newValue = {"curr_to_USD": rate}
                BulkOp.append(UpdateMany(query,{"$set": newValue}))

            except TypeError:

                print('problem: '+ currency + ' at date '+ date)

    tornado.\
        ioloop.\
        IOLoop.\
        current()\
        .run_sync(PriceTargetAndconsensusValuesData(ClientDB,'','price_target', BulkOp).UpdateValuesInDB)

    ClientDB.close()
    return currency + ' Completed'

def PatchStocksPriceRecommendations(params):

    """This function patch all the data for the Price Target and the Recommendations giver the horizon

    """
    ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

    cusip_query = params.query[0]
    ticker_query = params.query[1]

    tab_date = GenerateMonthlyTab("1984M1", "2018M12")

    for per in range(1, len(tab_date)):

        recommendationsPreviousYearData = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per - 1], params.type,
                                                         cusip_query, None).GetValuesFromDB()
        if len(recommendationsPreviousYearData) == 0:
            recommendationsPreviousYearData = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per - 1], params.type, ticker_query,
                                                             None).GetValuesFromDB()
        for value in recommendationsPreviousYearData:

            cusip = value['cusip']
            tic = value['ticker']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = int(value['horizon'])

            if params.type == type_consensus:
                previous = value['recom']
            if params.type == type_price_target:
                previous = value['price_usd']

            newquery = {'cusip': cusip, 'mask_code': mask_code}
            recommendationsActualYeardata = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per], params.type,
                                                         newquery, None).GetValuesFromDB()

            if len(recommendationsActualYeardata) == 0:
                newquery = {'ticker': tic, 'mask_code': mask_code}
                recommendationsActualYeardata = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per], params.type,
                                                                                  newquery, None).GetValuesFromDB()

            if len(recommendationsActualYeardata) == 0:
                act = str(act_date.year) + 'M' + str(act_date.month)
                act_date_pos = tab_date.index(act)

                if per < act_date_pos + hor: #patch if current date < activate date + horizon
                    PriceTargetAndconsensusValuesData(ClientDB,tab_date[per], params.type, value).SetValuesInDB()
            else:

                for actual_value in recommendationsActualYeardata: #if there is a new value don't patch and calculate variation of consensu

                    if act_date != actual_value['date_activate']:
                        if params.type == type_consensus:
                            try:
                                var = actual_value['recom'] - previous
                            except TypeError:
                                var = None

                        if params.type == type_price_target:
                            try:
                                var = (actual_value['price_usd'] - previous) / previous
                            except TypeError:
                                var = None
                            except ZeroDivisionError:
                                var = None

                        PriceTargetAndconsensusValuesData(ClientDB, tab_date[per], params.type,
                                                              actual_value['_id'], {'$set': {"variation": var}})

def SetPriceRecommendationsInDB(params):

    tab = params.value
    dict_infos = dict()
    ClientDB = motor.motor_tornado.MotorClient(params.connectionString)

    if params.type == type_price_target:

        for res in tab:

            tic = res[0]
            cusip = res[1]
            cname = res[2]
            estim = res[3]
            hor = res[4]
            value = res[5]
            cur = res[6]
            date = res[7]
            mask_code = res[8]
            if cusip == None:
                cusip = tic

            yr = str(date.year)
            if date.month < 10:
                month = "0" + str(date.month)
            else:
                month = str(date.month)
            if date.day < 10:
                day = "0" + str(date.day)
            else:
                day = str(date.day)

            date_str = yr + '-' + month
            date = datetime.datetime(date.year, date.month, date.day, 16, 0, 0, 0)

            data = {'cusip':cusip,'ticker': tic,'analyst':estim,'price':value,'horizon':hor,
                    'curr':cur,'date_activate':date,'mask_code':mask_code,'variation':None,'price_usd':None}

            if dict_infos.get(date_str,False):
                dict_infos[date_str].append(data)
            else:
                dict_infos[date_str] = [data]


    if params.type == type_consensus:

        for res in range(tab):

            tic = res[0]
            cusip = res[1]
            cname = res[2]
            estim = res[3]
            value = res[4]
            date = res[5]
            mask_code = res[6]
            if cusip == None:
                cusip = tic

            yr = str(date.year)
            if date.month < 10:
                month = "0" + str(date.month)
            else:
                month = str(date.month)
            if date.day < 10:
                day = "0" + str(date.day)
            else:
                day = str(date.day)

            date_str = yr + '-' + month
            date = datetime.datetime(date.year, date.month, date.day, 16, 0, 0, 0)

            "'consensus': {'cusip', 'ticker', 'analyst', 'recom', "" \
                    ""'horizon','date_activate','mask_code','variation'}"
            data = {'cusip': cusip, 'ticker': tic, 'analyst': estim, 'recom': value, 'horizon': 6
                    ,'date_activate': date, 'mask_code': mask_code, 'variation': None}

            if dict_infos.get(date_str,False):
                dict_infos[date_str].append(data)
            else:
                dict_infos[date_str] = [data]

    tornado.ioloop.IOLoop.current().run_sync(
                PriceTargetAndconsensusValuesData(ClientDB, key, params.type, dict_infos[key]).SetValuesInDB())
    ClientDB.close()
    return 'lot : [', params.position, "] Completed"

