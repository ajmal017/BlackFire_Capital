import pymongo
from aBlackFireCapitalClass.ClassEconomcisZonesData.ClassEconomicsZonesDataInfos import EconomicsZonesDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import country_zone_and_exchg

__author__ = 'pougomg'


def SetCountriesEconomicsZonesInDB():

    ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

    for value in country_zone_and_exchg:
        data = {"_id":value[1], "eco zone": value[2], "name": value[0]}
        EconomicsZonesDataInfos(ClientDB,data).SetEconomicsZonesInDB()
    ClientDB.close()

def SetCountriesEconomicsZonesForStocksInDB():

    ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

    for stocksInfos in StocksMarketDataInfos(ClientDB, {}, None).GetDataFromDB():
        fic = stocksInfos['incorporation location']
        tab_zone_eco = EconomicsZonesDataInfos(ClientDB, {'_id': fic}, None).GetEconomicsZonesFromDB()
        for zone_eco in tab_zone_eco:
            StocksMarketDataInfos(ClientDB,stocksInfos['_id'], {'eco zone': zone_eco['eco zone']}).UpdateDataInDB()

    ClientDB.close()