from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import  GetMeanValueOfPriceRecommendationAgregation
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target, \
     GenerateMonthlyTab
import numpy as np
import pandas as pd

def makeKey(gvkey, cusip, ticker, maskcd, date):

    return gvkey + "_" + cusip + "_" + ticker+ "_" + maskcd + "_" + date


def SetdataToDB():

    tabPT = np.load(type_price_target + "_toSaveInDB.npy")
    entete = ['ticker', 'cusip', 'emaskcd', 'horizon', 'value', 'estcur', 'anndats', 'amaskcd', 'exrat',
              'gvkey', 'variation', 'data', 'date']
    tabPT = pd.DataFrame(tabPT, columns= entete)
    tabPT = tabPT[['ticker', 'cusip', 'emaskcd', 'horizon', 'value', 'estcur', 'anndats', 'amaskcd', 'exrat',
              'gvkey', 'variation', 'date']]

    tabCS = np.load(type_consensus + "_toSaveInDB.npy")
    entete = ['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd',
                   'gvkey', 'variation', 'data', 'date']
    tabCS = pd.DataFrame(tabCS, columns= entete)
    tabCS = tabCS[['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd',
                   'gvkey', 'variation', 'date']]


    tabDate = GenerateMonthlyTab('1999-02', '1999-02')
    tabInFile = []

    for pos in range(len(tabDate)):

        date_end = tabDate[pos]
        pos_begin_pt = pos - 11
        pos_begin_cs = pos - 5
        if pos_begin_pt < 0:
            pos_begin_pt = 0
        if pos_begin_cs < 0:
            pos_begin_cs = 0

        pos_last = pos - 1
        if pos_last < 0:
            pos_last = 0
        tabInFile.append([date_end, tabDate[pos_begin_pt], tabDate[pos_begin_cs]])

    for value in tabInFile[1:]:

        tabPTtoWork = tabPT.loc[value[1]: value[0]]
        tabCStoWork = tabCS.loc[value[2]: value[0]]

        v = np.vectorize(makeKey)

        tabPTtoWork = tabPTtoWork.sort_values(["gvkey", "cusip", "amaskcd", "anndats"], ascending=[True,True, False,False])
        tabPTtoWork = tabPTtoWork.drop_duplicates(subset=["gvkey", "cusip", "amaskcd"], keep="first")

        tabCStoWork = tabCStoWork.sort_values(["gvkey", "cusip", "amaskcd", "anndats"], ascending=[True,True, False,False])
        tabCStoWork = tabCStoWork.drop_duplicates(subset=["gvkey", "cusip", "amaskcd"], keep="first")



def SetGvkeyInStocksPriceRecoomendationsInfos(params):

    """params: type"""
    for infos in PriceTargetAndconsensusInfosData(ClientDB, params.type, {}, {"_id": 1, "ticker": 1}).GetInfosFromDB():

        stocks_infos_query_ibtic = {'ibtic': infos["ticker"]}
        stocks_infos_query_cusip_8 = {'cusip_8': infos["_id"]}

        for stocks_infos in StocksMarketDataInfos(ClientDB,
                                                  {'stock identification': {'$elemMatch': stocks_infos_query_ibtic}},
                                                  None).GetDataFromDB():
            PriceTargetAndconsensusInfosData(ClientDB, params.type, infos["_id"],
                                             {"$set": {"gvkey": stocks_infos["_id"]}}).UpdateInfosInDB()

        for stocks_infos in StocksMarketDataInfos(ClientDB,
                                                  {'stock identification': {'$elemMatch': stocks_infos_query_cusip_8}},
                                                  None).GetDataFromDB():
            PriceTargetAndconsensusInfosData(ClientDB, params.type, infos["_id"],
                                             {"$set": {"gvkey": stocks_infos["_id"]}}).UpdateInfosInDB()


def MergeStocksWithPriceRecommendations(params):
    """params = collection( type, date)"""
    date = params.date

    for stocks in StocksMarketDataPrice(ClientDB, date, {}, {"_id":1, "gvkey":1}).GetStocksPriceFromDB():

        gvkey = stocks["gvkey"]
        cusip = stocks["_id"]

        tab_value = []

        for infos in PriceTargetAndconsensusInfosData(ClientDB, params.type,{"gvkey": gvkey}, {"_id": 1}).GetInfosFromDB():
            cusip_ibes = infos["_id"]

            for value in PriceTargetAndconsensusValuesData(ClientDB, date, params.type,{"cusip": cusip_ibes}, None).GetValuesFromDB():
                tab_value.append(value)

        return_value = GetMeanValueOfPriceRecommendationAgregation(date, tab_value, params.type)
        newvalues = {"$set": {params.type: return_value}}
        StocksMarketDataPrice(ClientDB, date, cusip, newvalues).UpdateStocksPriceInDB()

    return "done with " + params.type + "for period" + date