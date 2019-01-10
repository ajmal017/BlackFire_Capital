import motor
import tornado

import pymongo
from csv import reader
import numpy as np
import collections
import pandas as pd
import re
from pathlib import Path
from pymongo import InsertOne
from datetime import  datetime



from aBlackFireCapitalClass.ClassEconomcisZonesData.ClassEconomicsZonesDataInfos import EconomicsZonesDataInfos
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateMonthlyTab, profile

set_sector_tuple = collections.namedtuple('set_sector_tuple', [
    'naics',
    'zone_eco',
])

def SectorGrouping(group):

    #Stocks Infos
    csho = group['csho'].sum()
    vol = group['vol'].sum()
    pc = group['mc'].sum()/csho
    ph = (group['csho'] * group['ph']/group['USDtocurr']).sum()/csho
    pl = (group['csho'] * group['pl']/group['USDtocurr']).sum()/csho

    #PT infos
    nptvar = group['nptvar'].sum()
    ptvar = (group['ptvar'] * group['nptvar']).sum()/nptvar
    npptvar = group['npptvar'].sum()
    pptvar = (group['pptvar'] * group['nptvar']).sum()/npptvar

    #CS infos
    nrc = group['nrc'].sum()
    rc = (group['rc'] * group['nrc']).sum()/nrc
    nrcvar = group['nrcvar'].sum()
    rcvar = (group['rcvar'] * group['nrcvar']).sum()/nrcvar
    a = pd.NaT/0
    b =pd.NaT/pd.NaT


    return InsertOne({'csho': csho, "vol": vol, 'price_close': pc, 'price_high': ph, "price_low": pl,
                      'price_target':{'num_var': nptvar, 'mean_var': ptvar, 'pnum_var': npptvar, 'pmean_var': pptvar},
                      'consensus': {'mean_recom': rc, 'num_recom': nrc, 'mean_var': rcvar, "num_var": nrcvar},
                      'eco zone': group.name[0], 'naics': group.name[1], 'date': group.iloc[0,-1]})


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

@profile
def AddStocksPerNaicsAndEcoZone():

    my_path = Path(__file__).parent.parent.parent.resolve()
    print(my_path)
    res = np.load('ZoneAndNaics.npy')
    StocksPriceInfos = np.load(str(my_path) + '/zBlackFireCapitalImportantFunctions/StocksPricesInfos.npy')

    res = pd.DataFrame(res, columns=['eco zone', 'naics', 'level'])
    # res = res[res['level'] == '2']
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

    np.save('StocksBySector',tabGroupNaicsAndSector)

@profile
def SetSectorPriceToDB(params):

    StocksBySector = np.load('StocksBySector.npy')
    StocksBySector = pd.DataFrame(StocksBySector, columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg'])
    # print(StocksBySector.groupby(['eco zone', 'naics']).count())

    pipeline = [{'$sort': {"isin_or_cusip": 1, "date": 1}},
                {
                    "$group":{
                        "_id":"$isin_or_cusip",
                        "date": {"$last": "$date"},
                        "gvkey": {"$last": "$gvkey"},
                        "curr": {"$last": "$curr"},
                        "csho":{"$last": "$csho"},
                        "vol": {"$sum": "$vol"},
                        "adj_factor": {"$last": "$adj_factor"},
                        "price_close":{"$last": "$price_close"},
                        "price_high":{"$max": "$price_high"},
                        "price_low":{"$min": "$price_low"},
                        "USD_to_curr":{"$last": "$USD_to_curr"},
                        "consensus":{"$last": "$consensus"},
                        "price_target":{"$last": "$price_target"},

                    }
                }]

    # ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    # # loop = tornado.ioloop.IOLoop
    # # loop.current().run_sync(StocksMarketDataPrice(ClientDB, '2017-12', {}, None).SetIndexCreation)
    # loop = tornado.ioloop.IOLoop
    # tabStocksPrice = loop.current().run_sync(StocksMarketDataPrice(ClientDB, '2017-12', pipeline).GetMontlyPrice)
    # ClientDB.close()
    # # # print(tabStocksPrice)
    # tab_result = []
    #
    # for value in tabStocksPrice:
    #
    #     tPrice = [value['_id'], value['gvkey'], value['curr'], value['csho'], value['vol'], value['adj_factor'],
    #          value['price_close'], value['price_high'], value['price_low'], value['USD_to_curr']]
    #
    #     if value['price_target'] is not None:
    #         _  = value['price_target']
    #         _['price'] = None if _['price'] == "None" else float (_['price'])
    #         _['mean_var'] = None if _['mean_var'] == "None" else float (_['mean_var'])
    #         _['pmean_var'] = None if _['pmean_var'] == "None" else float (_['pmean_var'])
    #
    #         tPriceTarget = [_['price'], _['num_price'], _['mean_var'], _['num_var'], _['pmean_var'], _['pnum_var']]
    #     else:
    #         tPriceTarget = [None, None, None, None, None, None]
    #
    #     if value['consensus'] is not None:
    #         _  = value['consensus']
    #         _['mean_recom'] = None if _['mean_recom'] == "None" else float (_['mean_recom'])
    #         _['mean_var'] = None if _['mean_var'] == "None" else float (_['mean_var'])
    #         tConsensus = [_['mean_recom'], _['num_recom'], _['mean_var'], _['num_var']]
    #     else:
    #         tConsensus = [None, None, None, None]
    #
    #     tab_result.append(tPrice + tPriceTarget + tConsensus)
    #
    # np.save('tabtest', tab_result)
    tab_result = np.load('tabtest.npy')
    tab_result = pd.DataFrame(tab_result, columns=['isin', 'gvkey', 'curr', 'csho', 'vol', 'adj_factor', 'pc', 'ph',
                                                   'pl', 'USDtocurr','pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar',
                                                   'rc', 'nrc', 'rcvar', 'nrcvar'])



    tab_result = pd.merge(StocksBySector, tab_result,on=['gvkey', 'isin'])

    tab_result[['csho', 'vol', 'adj_factor', 'pc', 'ph','pl', 'USDtocurr','pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar',
                                                   'rc', 'nrc', 'rcvar', 'nrcvar']] \
        = tab_result[['csho', 'vol', 'adj_factor', 'pc', 'ph','pl', 'USDtocurr','pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar',
                                                   'rc', 'nrc', 'rcvar', 'nrcvar']].astype(float)


    tab_result['mc'] = tab_result['csho'] * tab_result['pc']/tab_result['USDtocurr']
    tab_result['date'] = datetime(2017,12,15)
    tab_result = tab_result.sort_values(["gvkey", "mc"], ascending=[True, False])

    tab_result = tab_result.drop_duplicates(['eco zone','naics','gvkey'])


    result = tab_result.groupby(['eco zone', 'naics']).apply(SectorGrouping)

    tabTopush = pd.DataFrame(np.array(result))
                      # index=tabGroupNaicsAndSector.index,
                      # columns=['eco zone', 'naics', 'data'])

    print(tabTopush)

    # print(tab_result[tab_result['eco zone'] == "USD"][['naics', 'gvkey','isin', 'pc', 'csho', 'USDtocurr','mc']])











    print(0)

if __name__ == "__main__":
    # GetListofEcoZoneAndNaics(ProdConnectionString)
    # AddStocksPerNaicsAndEcoZone()
    SetSectorPriceToDB(ProdConnectionString)