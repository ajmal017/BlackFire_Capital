from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import ClientDB, \
    GetMeanValueOfPriceRecommendationAgregation


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