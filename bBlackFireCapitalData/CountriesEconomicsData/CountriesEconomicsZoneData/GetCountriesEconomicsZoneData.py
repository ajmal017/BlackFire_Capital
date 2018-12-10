import motor
import pymongo
import tornado

from aBlackFireCapitalClass.ClassEconomcisZonesData.ClassEconomicsZonesDataInfos import EconomicsZonesDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import country_zone_and_exchg

__author__ = 'pougomg'


def SetCountriesEconomicsZonesInDB(connectionstring):
    ClientDB = motor.motor_tornado.MotorClient(connectionstring)
    tab_country_zone = []
    for value in country_zone_and_exchg:
        tab_country_zone.append({"_id": value[1], "eco zone": value[2], "name": value[0]})

    tornado.ioloop.IOLoop.current().run_sync(EconomicsZonesDataInfos(ClientDB, tab_country_zone).SetEconomicsZonesInDB)
    ClientDB.close()


def SetCountriesEconomicsZonesForStocksInDB(connectionstring):

    ClientDB = motor.motor_tornado.MotorClient(connectionstring)
    tab_zone_eco = tornado.ioloop.IOLoop.current().run_sync(EconomicsZonesDataInfos(ClientDB, {}, None).GetEconomicsZonesFromDB)

    print(tab_zone_eco)

    for zone_eco in tab_zone_eco:
        tornado.ioloop.IOLoop.current().run_sync(
            StocksMarketDataInfos(
                ClientDB,
                {'incorporation location': zone_eco['_id']},
                {'eco zone': zone_eco['eco zone']}).UpdateDataInDB)

    ClientDB.close()
