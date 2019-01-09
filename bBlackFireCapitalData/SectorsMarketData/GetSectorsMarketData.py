import motor
import tornado

import pymongo
from csv import reader
import numpy as np
import collections
import pandas as pd
import re
from pathlib import Path



from aBlackFireCapitalClass.ClassEconomcisZonesData.ClassEconomicsZonesDataInfos import EconomicsZonesDataInfos
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateMonthlyTab

set_sector_tuple = collections.namedtuple('set_sector_tuple', [
    'naics',
    'zone_eco',
])


def GetListofEcoZoneAndNaics(connectionstring):
    ClientDB = motor.motor_tornado.MotorClient(connectionstring)
    loop = tornado.ioloop.IOLoop
    ZoneEcoTab = loop.current().run_sync(EconomicsZonesDataInfos(ClientDB, {}, {"eco zone": 1}).GetEconomicsZonesFromDB)

    NaicsTab = tornado.ioloop.IOLoop.current(). \
        run_sync(SectorsMarketDataInfos(
        ClientDB, {"$or": [{"level": "1"}, {"level": "2"}]}, {"_id": 1, "level": 1}).GetDataFromDB)

    t = pd.DataFrame([[zone['eco zone'], naics['_id'], naics['level']] for zone in ZoneEcoTab for naics in NaicsTab],
                     columns=['eco zone', 'naics', 'level'])
    t = t.drop_duplicates(subset=['eco zone', 'naics', 'level'])
    np.save('ZoneAndNaics', t)


def AddStocksPerNaicsAndEcoZone():

    my_path = Path(__file__).parent.parent.parent.resolve()
    print(my_path)
    res = np.load('ZoneAndNaics.npy')
    StocksPriceInfos = np.load(str(my_path) + '/zBlackFireCapitalImportantFunctions/StocksPricesInfos.npy')

    res = pd.DataFrame(res, columns=['eco zone', 'naics'])
    StocksPriceInfos = pd.DataFrame(StocksPriceInfos, columns=['gvkey', 'eco zone', 'naics',
                                                               'isin', 'ibtic', 'cusip8', 'exchg'])

    StocksPriceInfos = StocksPriceInfos[['gvkey', 'eco zone', 'naics', 'isin', 'exchg']]
    StocksPriceInfos = StocksPriceInfos.dropna(subset=['isin'])
    StocksPriceInfos = StocksPriceInfos.dropna(subset=['eco zone'])

    def SetStocksPrice(group):

        naics = group.iloc[0,1]
        zone = group.iloc[0,0]
        dt = StocksPriceInfos[(StocksPriceInfos['eco zone'] == zone) &
                              (StocksPriceInfos['naics'].str.startswith(naics, na=False))]
        dt['zone'] = zone
        dt['gnaics'] = naics

        return dt[['zone', 'gnaics', 'gvkey', 'isin', 'exchg']]

    tabGroupNaicsAndSector = res.groupby(['eco zone', 'naics']).apply(SetStocksPrice)
    tabMatching = pd.DataFrame(np.array(tabGroupNaicsAndSector),
                      # index=tabGroupNaicsAndSector.index,
                      columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg'])

    print(tabMatching)


def SetSectorPriceToDB(params):

    print(0)

if __name__ == "__main__":
    print('aaa')
    # GetListofEcoZoneAndNaics(ProdConnectionString)
    AddStocksPerNaicsAndEcoZone()
