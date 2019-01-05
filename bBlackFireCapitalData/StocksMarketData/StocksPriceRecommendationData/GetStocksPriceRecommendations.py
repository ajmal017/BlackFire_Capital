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
import collections
from zBlackFireCapitalImportantFunctions.ConnectionString import TestConnectionString, ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import profile
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target, \
     GenerateMonthlyTab

table = collections.namedtuple('table', [
    'type',
])
_ACTUAL_ = '_act'
_PREVIOUS_ = '_prev'

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
                            date_prev, recom_act, recom_prev, cusip_act, cusip_prev):

    if gvkey_act != gvkey_prev:
        return None
    if cusip_act != cusip_prev:
        return None
    if mask_code_act != mask_code_prev:
        return None
    if (date_act - date_prev).days > 6*30:
        return None
    return int(recom_act) - int(recom_prev)


def CalculatePriceTargetVar(gvkey_act, gvkey_prev, mask_code_act, mask_code_prev,date_act,
                            date_prev, recom_act, recom_prev, curr_act, curr_prev, exchg_act,
                            exchg_prev, cusip_act, cusip_prev, horizon):

    if gvkey_act != gvkey_prev:
        return None
    if cusip_act != cusip_prev:
        return None
    if mask_code_act != mask_code_prev:
        return None
    if (date_act - date_prev).days > int(horizon*30):
        return None
    if curr_act == curr_prev:
        try:
            return -1 + recom_act/recom_prev
        except ZeroDivisionError:
            return None
        except TypeError:
            return None
    # else:
    #     # print(gvkey_act, gvkey_prev, mask_code_act, mask_code_prev,date_act,
    #     #                     date_prev, recom_act, recom_prev, curr_act, curr_prev, exchg_act, exchg_prev, horizon)
    #     try:
    #         return -1 + (recom_act* exchg_act)/(recom_prev * exchg_prev)
    #     except ZeroDivisionError:
    #         return None
    #     except TypeError:
    #         return None


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


def BulkSetPriceTargetData(gvkey, ticker, cusip, amaskcd, emaskcd, anndats, curr, horizon, exrat, value, variation):

    return InsertOne({"ticker": ticker, "cusip": cusip, "emasckd": str(emaskcd), "horizon": str(horizon), "curr": curr,
                      "value": value, "anndats": datetime.datetime(anndats.year,anndats.month,anndats.day,16,0,0),
                      "amaskcd": amaskcd, "variation": variation, "USD_to_curr": exrat, "gvkey": gvkey})


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
        v = np.vectorize(PatchExrates)
        res['Pexrat'] = v(res['exrat'], res['anndats'], res['estcur'])
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

@profile
def CalculateRecommendationVar(params):

    if params.type == type_price_target:
        entete = ['ticker', 'cusip', 'emaskcd', 'horizon', 'value',
                  'estcur', 'anndats', 'amaskcd', 'exrat', 'gvkey']
        indice_for_var = [9, 7, 6, 4, 5, 8, 1]

    if params.type == type_consensus:
        entete = ['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd', 'gvkey']
        indice_for_var = [6, 5, 4, 3, 1]

    res = np.load(params.type + '_dataWithGVKEY.npy')
    res = pd.DataFrame(res, columns=entete)

    v = np.vectorize(convertDateToString)
    res['date'] = v(res['anndats'])

    res = res.sort_values(["gvkey","cusip", "amaskcd","date"], ascending=[True, True, False,False])
    res = res.iloc[:].reset_index(drop=True)
    res_p = res.iloc[1:, indice_for_var].reset_index(drop=True)
    res = res.iloc[:-1]
    res = res.join(res_p, lsuffix=_ACTUAL_, rsuffix=_PREVIOUS_)

    if params.type == type_consensus:
        v = np.vectorize(CalculateConsensusVar)
        res['variation'] = v(res[entete[indice_for_var[0]] + _ACTUAL_], res[entete[indice_for_var[0]] + _PREVIOUS_],
                             res[entete[indice_for_var[1]] + _ACTUAL_], res[entete[indice_for_var[1]] + _PREVIOUS_],
                             res[entete[indice_for_var[2]] + _ACTUAL_], res[entete[indice_for_var[2]] + _PREVIOUS_],
                             res[entete[indice_for_var[3]] + _ACTUAL_], res[entete[indice_for_var[3]] + _PREVIOUS_],
                             res[entete[indice_for_var[4]] + _ACTUAL_], res[entete[indice_for_var[4]] + _PREVIOUS_],)

        v = np.vectorize(BulkSetConsensusData)

        res['data'] = v(res['ticker'], res['cusip' + _ACTUAL_], res['emaskcd'], res['ireccd'+ _ACTUAL_],
                        res['anndats' + _ACTUAL_], res['amaskcd' + _ACTUAL_], res['gvkey' + _ACTUAL_],
                        res['variation'])

        res = res[['ticker', 'cusip'+ _ACTUAL_,'emaskcd', 'ireccd'+ _ACTUAL_, 'anndats'+ _ACTUAL_, 'amaskcd'+ _ACTUAL_,
                   'gvkey'+ _ACTUAL_, 'variation', 'data', 'date']]


    if params.type == type_price_target:
        v = np.vectorize(CalculatePriceTargetVar)
        res['variation'] = v(res[entete[indice_for_var[0]] + _ACTUAL_], res[entete[indice_for_var[0]] + _PREVIOUS_],
                             res[entete[indice_for_var[1]] + _ACTUAL_], res[entete[indice_for_var[1]] + _PREVIOUS_],
                             res[entete[indice_for_var[2]] + _ACTUAL_], res[entete[indice_for_var[2]] + _PREVIOUS_],
                             res[entete[indice_for_var[3]] + _ACTUAL_], res[entete[indice_for_var[3]] + _PREVIOUS_],
                             res[entete[indice_for_var[4]] + _ACTUAL_], res[entete[indice_for_var[4]] + _PREVIOUS_],
                             res[entete[indice_for_var[5]] + _ACTUAL_], res[entete[indice_for_var[5]] + _PREVIOUS_],
                             res[entete[indice_for_var[6]] + _ACTUAL_], res[entete[indice_for_var[6]] + _PREVIOUS_],
                             res['horizon'])

        v = np.vectorize(BulkSetPriceTargetData)

        res['data'] = v(res['gvkey'+ _ACTUAL_], res['ticker'], res['cusip'+ _ACTUAL_], res['amaskcd'+ _ACTUAL_],
                        res['emaskcd'], res['anndats'+ _ACTUAL_], res['estcur'+ _ACTUAL_], res['horizon'],
                        res['exrat'+ _ACTUAL_], res['value'+ _ACTUAL_], res['variation'])


        res = res[['ticker', 'cusip'+ _ACTUAL_, 'emaskcd', 'horizon', 'value'+ _ACTUAL_, 'estcur'+ _ACTUAL_,
                   'anndats'+ _ACTUAL_, 'amaskcd'+ _ACTUAL_, 'exrat'+ _ACTUAL_, 'gvkey'+ _ACTUAL_, 'variation',
                   'data', 'date']]

    res = res.sort_values("date", ascending=True).reset_index(drop=True)
    np.save(params.type + "_toSaveInDB", res)


def SetDataToDB(params):


    entete = ['date', 'data', 'gvkey', 'amaskcd', 'anndats', 'emaskcd', 'cusip']

    res = np.load(params.type + "_toSaveInDB.npy")
    res = pd.DataFrame(res, columns= entete)

    tabDate = GenerateMonthlyTab('1999-02', '2018-04')
    tabInFile = []

    for pos in range(len(tabDate)):

        date_end = tabDate[pos]
        pos_begin = pos - 11
        if pos_begin < 0:
            pos_begin = 0
        pos_last = pos - 1
        if pos_last < 0:
            pos_last = 0
        tabInFile.append([date_end, tabDate[pos_last], tabDate[pos_begin]])

    res = res.set_index('date')

    ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)


    for value in tabInFile[1:]:

        print(value)

        tab = res.loc[value[2]: value[1]]
        tab = tab.sort_values(["gvkey", "cusip", "amaskcd", "anndats"], ascending=[True,True, False,False])
        tab = tab.drop_duplicates(subset=["gvkey", "cusip", "amaskcd", "emaskcd"], keep="first")
        toWrite = list(tab['data'])
        tab = res.loc[value[0]]
        toWrite += list(tab['data'])

        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(PriceTargetAndconsensusValuesData(ClientDB, value[0],params.type, toWrite).SetValuesInDB)
    ClientDB.close()

if __name__ == '__main__':
    params = table(type=type_price_target)
    # AddGvkeyToTable(params)
    # CalculateRecommendationVar(params)
    SetDataToDB(params)
