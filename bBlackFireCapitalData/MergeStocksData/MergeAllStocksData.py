import motor
import tornado
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import  GetMeanValueOfPriceRecommendationAgregation
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target, \
     GenerateMonthlyTab
import numpy as np
import pandas as pd
from pymongo import UpdateMany
from scipy import  stats

def makeKey(gvkey, cusip, ticker, maskcd, date):

    return gvkey + "_" + cusip + "_" + ticker+ "_" + maskcd + "_" + date

def getPriceTargetVar(group):

    group = group[((group['price_to_USD']/ group['price_to_USD'].median()).abs() < 2) &
                ((group['price_to_USD']/ group['price_to_USD'].median()).abs() > 0.5)]

    group = group[group['variation'].abs() < 2]

    result = [str('{0:.4f}'.format(group['price_to_USD'].mean())),
              str(group['price_to_USD'].count()), str('{0:.4f}'.format(group['variation'].mean())),
              str(group['variation'].count())]
    return ' '.join(result)

def meanVar(group):
    # print(group)
    try:
        value = sum(group['PtVarcount'] * group['PtVarmean'])/sum(group['PtVarcount'])
    except ZeroDivisionError:
        value = None
    except TypeError:
        value = None

    return value

def BulkPriceTarget(isin, gvkey, pt_mean, pt_count, ptvarmean, ptvarcount, var_mean, var_count):

    return UpdateMany({"isin_or_cusip": isin, "gvkey": gvkey},
                       {"$set":
                           {type_price_target:
                                {'price':str('{0:.4f}'.format(pt_mean)),
                                 "num_price": int(pt_count),
                                 "pmean_var": str(ptvarmean),
                                 "pnum_var": int(ptvarcount),
                                 "mean_var": str(var_mean),
                                 "num_var": int(var_count),
                                 }
                            }
                        }
    )

def getConsensusVar(group):

    # print(group)
    if group['variation'].count() == 0:
        var = None
    else:
        var = '{0:.4f}'.format(group['variation'].mean())

    return UpdateMany({"gvkey":group.name},
                      {"$set":
                           {type_consensus:
                                {'mean_recom':str('{0:.4f}'.format(group['ireccd'].mean())), "num_recom": int(group['ireccd'].count()),
                                  "mean_var": str(var), "num_var": int(group['variation'].count())}}}
    )

def SetdataToDB():

    tabPT = np.load(type_price_target + "_toSaveInDB.npy")
    entete = ['ticker', 'cusip', 'emaskcd', 'horizon', 'value', 'estcur', 'anndats', 'amaskcd', 'exrat',
              'gvkey', 'variation', 'data', 'date']
    tabPT = pd.DataFrame(tabPT, columns= entete)
    tabPT = tabPT[['ticker', 'cusip', 'emaskcd', 'horizon', 'value', 'estcur', 'anndats', 'amaskcd', 'exrat',
              'gvkey', 'variation', 'date']]
    tabPT = tabPT.set_index('date')

    tabSI = np.load("/home/pougomg/Bureau/BlackFire Capital/zBlackFireCapitalImportantFunctions/StocksPricesInfos.npy")
    tabSI = pd.DataFrame(tabSI, columns= ['gvkey', 'eco', 'naics', 'isin', 'ticker', 'cusip', 'exhg'])
    tabSI = tabSI[['gvkey', 'isin', 'ticker', 'cusip']]
    tabSI = tabSI[tabSI['isin'] != None]
    tabSI = tabSI.dropna(subset=['isin'])


    # tabCS = np.load(type_consensus + "_toSaveInDB.npy")
    # entete = ['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd',
    #                'gvkey', 'variation', 'data', 'date']
    # tabCS = pd.DataFrame(tabCS, columns= entete)
    # tabCS = tabCS[['ticker', 'cusip','emaskcd', 'ireccd', 'anndats', 'amaskcd',
    #                'gvkey', 'variation', 'date']]
    # tabCS = tabCS.set_index('date')


    tabDate = GenerateMonthlyTab('1999-02', '2018-04')
    tabInFile = []

    for pos in range(len(tabDate)):

        date_end = tabDate[pos]
        pos_begin_pt = pos - 11
        pos_begin_cs = pos - 5
        if pos_begin_pt < 0:
            pos_begin_pt = 0
        if pos_begin_cs < 0:
            pos_begin_cs = 0

        tabInFile.append([date_end, tabDate[pos_begin_pt], tabDate[pos_begin_cs]])

    ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    # for value in [tabInFile[-1]]:
    for value in tabInFile[2:]:
        print(value)
        tabPTtoWork = tabPT.loc[value[1]: value[0]]
        tabPTtoWork = tabPTtoWork.sort_values(["gvkey", "cusip", "amaskcd", "anndats"], ascending=[True,True, False,False])
        tabPTtoWork = tabPTtoWork.drop_duplicates(subset=["gvkey", "cusip", 'ticker', "amaskcd"], keep="first")

        # tabPTtoWork = tabPTtoWork[tabPTtoWork['gvkey'] == '001186']
        tabPTtoWork[['value', 'exrat', 'variation']] = tabPTtoWork[['value', 'exrat', 'variation']].astype(float)
        tabPTtoWork["price_to_USD"] = tabPTtoWork.value/tabPTtoWork.exrat

        tabPTtoGroup = tabPTtoWork.groupby(['gvkey', 'cusip', 'ticker'])[['value', 'exrat', 'variation', 'estcur', 'price_to_USD']]\
            .apply(getPriceTargetVar)

        del tabPTtoWork

        tabPTtoGroup = pd.DataFrame({'result' : tabPTtoGroup}).reset_index()
        tabPTtoGroup['result'] = tabPTtoGroup['result'].astype(str)

        tabPTtoGroup['pt_mean'], tabPTtoGroup['pt_count'], tabPTtoGroup['PtVarmean'], tabPTtoGroup['PtVarcount'] = tabPTtoGroup['result'].str.split(' ').str

        tabPTtoGroup[['pt_mean', 'pt_count', 'PtVarmean', 'PtVarcount']] = tabPTtoGroup[['pt_mean', 'pt_count', 'PtVarmean', 'PtVarcount']].astype(float)

        t= tabPTtoGroup.groupby('gvkey')['PtVarcount'].sum()
        tabPTtoGroup['var_count'] = tabPTtoGroup['gvkey'].map(t)

        t = tabPTtoGroup.groupby('gvkey')[['PtVarcount', 'PtVarmean']].apply(meanVar)
        tabPTtoGroup['var_mean'] = tabPTtoGroup['gvkey'].map(t)
        print(tabPTtoGroup.shape)


        CusipFilterTab = tabSI[tabSI['cusip'] != None][['gvkey', 'isin', 'cusip']]
        CusipFilterTab = CusipFilterTab.dropna(subset=['cusip'])
        CusipFilterTab= pd.merge(CusipFilterTab, tabPTtoGroup,on=['gvkey', 'cusip'], sort=False)


        TickerFilterTab = tabSI[tabSI['ticker'] != None][['gvkey', 'isin', 'ticker']]
        TickerFilterTab = TickerFilterTab.dropna(subset=['ticker'])

        TickerFilterTab = pd.merge(TickerFilterTab, tabPTtoGroup,on=['gvkey', 'ticker'], sort=False)

        CusipFilterTab = CusipFilterTab.append(TickerFilterTab)
        CusipFilterTab = CusipFilterTab.drop_duplicates(CusipFilterTab.columns)

        del TickerFilterTab
        v = np.vectorize(BulkPriceTarget)

        CusipFilterTab['data'] = v(CusipFilterTab['isin'],CusipFilterTab['gvkey'],CusipFilterTab['pt_mean'],
                                   CusipFilterTab['pt_count'], CusipFilterTab['PtVarmean'], CusipFilterTab['PtVarcount'],
                                   CusipFilterTab['var_mean'], CusipFilterTab['var_count'])

        # print(CusipFilterTab[['gvkey', 'isin', 'data']])


        # tabCStoWork = tabCS.loc[value[2]: value[0]]
        # tabCStoWork = tabCStoWork.sort_values(["gvkey", "cusip", "amaskcd", "anndats"], ascending=[True,True, False,False])
        # tabCStoWork = tabCStoWork.drop_duplicates(subset=["gvkey", "cusip", 'ticker', "amaskcd"], keep="first")
        # tabCStoWork[['variation', 'ireccd']] = tabCStoWork[['variation', 'ireccd']].astype(float)
        # tabCStoGroup = tabCStoWork[['variation', 'ireccd']].groupby(tabCStoWork['gvkey']).apply(getConsensusVar)

        # for v in list(tabCStoGroup):
        #     print(v)
        # break

        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(StocksMarketDataPrice(ClientDB, value[0], list(CusipFilterTab['data'])).SetStocksPriceInDB)

    ClientDB.close()


if __name__ =='__main__':

    SetdataToDB()

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