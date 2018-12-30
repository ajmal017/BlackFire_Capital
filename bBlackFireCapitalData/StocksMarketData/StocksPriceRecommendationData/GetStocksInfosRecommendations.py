import pickle
import motor
import tornado
import numpy as np
import pandas as pd
from pymongo import UpdateOne
from bson.objectid import ObjectId

from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.ConnectionString import TestConnectionString, ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target

__author__ = 'pougomg'
import wrds

def SetStocksInfosRecommendationsInDB(type, connectionstring):

    """
        This function set all the Stocks Recommendations Infos in the DB.
        :param:
            type: price_target/consensus DB
            connectionstring. The DB location where the data will be store.

    """

    if type == type_consensus:
        db = wrds.Connection()
        res = db.raw_sql("select a.cusip, a.ticker from ibes.recddet a group by a.cusip, a.ticker")
        db.close()
    elif type == type_price_target:
        db = wrds.Connection()
        res = db.raw_sql("select a.cusip, a.ticker from ibes.ptgdet a group by a.cusip, a.ticker")
        db.close()
    else:
        error = "Incorrection Argument Type It must be {} or {}."
        raise TypeError(error.format(type_price_target, type_consensus))

    dict_infos = dict()
    for pos in range(res.shape[0]):
        cusip = res['cusip'][pos]
        ticker = res['ticker'][pos]

        if cusip is None:
            cusip = ticker

        dict_infos[(cusip, ticker)] = {'ticker': ticker, 'cusip': cusip}

        if (cusip != ticker):
            if dict_infos.get((ticker, ticker), False):
                del dict_infos[(ticker, ticker)]
    data = []
    for key in dict_infos:
        data.append(dict_infos[key])
    ClientDB = motor.motor_tornado.MotorClient(connectionstring)
    tornado.ioloop.IOLoop.current().run_sync(PriceTargetAndconsensusInfosData(ClientDB,type,data).SetInfosInDB)
    ClientDB.close()


def BulkSetData(_id, gvkey):

    return UpdateOne({"_id":ObjectId(_id)},{"$set":{"gvkey":gvkey}})

def SetGvkeyToInfosRecommendations(type_, connectionstring):


    # tabStocksInFosGvkey = []
    # for value in StocksInFosGvkeyList:
    #     tabStocksInFosGvkey.append([value["_id"], value['cusip'], value['ticker']])


    tabStocksInfosGvkey = np.load('tabStocksInFosGvkey.npy')

    tabStocksRecommendationInfos = np.load('tabStocksConsensusInfos.npy')

    tabStocksInfosGvkey = pd.DataFrame(tabStocksInfosGvkey, columns=['gvkey', 'cusip', 'ticker'])

    tabStocksRecommendationInfos = pd.DataFrame(tabStocksRecommendationInfos, columns=['_id', 'cusip', 'ticker'])

    CusipFilterTab = tabStocksRecommendationInfos[tabStocksRecommendationInfos['cusip'] != None]


    CusipFilterTab = pd.merge(CusipFilterTab, tabStocksInfosGvkey, on='cusip')[['_id', 'gvkey']].set_index('_id')

    TickerFilterTab = tabStocksRecommendationInfos[tabStocksRecommendationInfos['ticker'] != None]
    TickerFilterTab = pd.merge(TickerFilterTab, tabStocksInfosGvkey, on='ticker')[['_id', 'gvkey']].set_index('_id')



    tabResult = pd.concat([TickerFilterTab, CusipFilterTab]).reset_index().drop_duplicates('_id')
    v = np.vectorize(BulkSetData)
    tabResult['data'] = v(tabResult['_id'], tabResult['gvkey'])
    print(tabResult[tabResult.gvkey == '062634'])

    # data = list(tabResult['data'].values)
    # ClientDB = motor.motor_tornado.MotorClient(connectionstring)
    # tornado.\
    #     ioloop.IOLoop.current().\
    #     run_sync(PriceTargetAndconsensusInfosData(ClientDB,type_, data).SetInfosInDB)
    #
    # ClientDB.close()


SetGvkeyToInfosRecommendations('consensus', ProdConnectionString)
# SetGvkeyToInfosRecommendations('price_target', ProdConnectionString)