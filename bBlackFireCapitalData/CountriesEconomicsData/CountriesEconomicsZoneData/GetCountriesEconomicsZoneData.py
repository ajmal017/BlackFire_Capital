from aBlackFireCapitalClass.ClassEconomcisZonesData.ClassEconomicsZonesDataInfos import EconomicsZonesDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import country_zone_and_exchg, ClientDB

__author__ = 'pougomg'


def SetCountriesEconomicsZonesInDB():

    for value in country_zone_and_exchg:
        data = {"_id":value[1], "eco zone": value[2], "name": value[0]}
        EconomicsZonesDataInfos(ClientDB,data).SetEconomicsZonesInDB()


def SetCountriesEconomicsZonesForStocksInDB():

    for stocksInfos in StocksMarketDataInfos(ClientDB, {}, None).GetDataFromDB():
        fic = stocksInfos['incorporation location']
        tab_zone_eco = EconomicsZonesDataInfos(ClientDB, {'_id': fic}, None).GetEconomicsZonesFromDB()
        for zone_eco in tab_zone_eco:
            StocksMarketDataInfos(ClientDB,stocksInfos['_id'], {'eco zone': zone_eco['eco zone']}).UpdateDataInDB()


#SetCountriesEconomicsZonesInDB()
#SetCountriesEconomicsZonesForStocksInDB()

#for value in country:
#    for st in value[3:]:
#        d = {'exhg': st, 'excntry': value[1]}
#        zone_eco.insert(d)

#stocks_infos_db = myclient["stocks_infos"].value
#zone_eco = zone_eco_db["zone_eco"]

#for stocks in stocks_infos_db.find():

#    id = stocks["_id"]
#    inc = stocks["incorporation location"]
#    v = zone_eco.find_one({"_id": inc})

#    if v is not None:
#        stocks_infos_db.update_one({'_id': id}, {"$set": {"eco zone": v["eco zone"]}})