import collections

import motor
import tornado
import tornado.web
import pymongo
import wrds
import multiprocessing
import multiprocessing.pool
import numpy as np
import time

from bBlackFireCapitalData.CountriesEconomicsData.CountriesExchangeRatesData.GetExchangeRatesData import \
    SetExchangeRatesCurrencyInDB
from bBlackFireCapitalData.StocksMarketData.StocksPriceData.GetStocksInfosDataFromWRDS import SetStocksInfosDataInDB, \
    GetStocksInfosDataDict
from bBlackFireCapitalData.CountriesEconomicsData.CountriesEconomicsZoneData.GetCountriesEconomicsZoneData import \
    SetCountriesEconomicsZonesInDB, SetCountriesEconomicsZonesForStocksInDB
from bBlackFireCapitalData.MergeStocksData.MergeAllStocksData import SetGvkeyInStocksPriceRecoomendationsInfos, \
    MergeStocksWithPriceRecommendations
from bBlackFireCapitalData.StocksMarketData.StocksPriceData.GetStocksPriceDataFromWRDS import GetStocksPriceData, \
    ConvertStocksPriceToUSD
from bBlackFireCapitalData.StocksMarketData.StocksPriceRecommendationData.GetStocksInfosRecommendations import \
    SetStocksInfosRecommendationsInDB
from bBlackFireCapitalData.StocksMarketData.StocksPriceRecommendationData.GetStocksPriceRecommendations import \
    GetStocksPriceRecommendations, ConvertPriceTagetToUSD, PatchStocksPriceRecommendations
from zBlackFireCapitalImportantFunctions.ConnectionString import TestConnectionString, ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateMonthlyTab, principal_processor, \
    type_consensus, type_price_target, SetBackupOfDataBase, RestoreBackupOfdataBase, \
    CurrenciesExchangeRatesDBName, StocksMarketDataInfosDBName
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData


class NoDaemonProcess(multiprocessing.Process):
   # make 'daemon' attribute always return False
   def _get_daemon(self):
      return False

   def _set_daemon(self, value):
      pass

   daemon = property(_get_daemon, _set_daemon)


class MyPool(multiprocessing.pool.Pool):
   Process = NoDaemonProcess


StocksInfosParams = collections.namedtuple('StocksInfosParams',[
    'table',
    'library',
    'globalWRDS',])

StocksPriceParams = collections.namedtuple('StocksPriceParams',[
      'table',
      'library',
      'globalWRDS',
      'observation',
      'offset',
        'connectionstring'])

StocksRecommentdationsParams = collections.namedtuple('StocksRecommentdationsParams',[
      'table',
      'library',
      'type',
      'observation',
      'offset',
        "connectionstring",])

YearParams = collections.namedtuple('YearParams',[
      'date',
      ])

PatchStockPriceRecommendationsDataParams = collections.namedtuple('PatchStockPriceRecommendationsDataParams',[
        'type',
        'query',
      ])

MergeStocksWithPriceRecommendationsParams = collections.namedtuple('MergeStocksWithPriceRecommendationsParams',[
        'type',
        'date',
      ])


if __name__ == '__main__':

    connectionstring = TestConnectionString

    """This function set all the data inside the platforms"""

    print("""1. Download of all the currency pair, DONE: Verified""") #DONE: Verified
    #SetExchangeRatesCurrencyInDB(currency_from='USD', connectionString=ProdConnectionString)
    # SetExchangeRatesCurrencyInDB(currency_from='EUR', connectionString=TestConnectionString)
    # SetExchangeRatesCurrencyInDB(currency_from='GBP', connectionString=TestConnectionString)

    print("""2. Download All the Economics Zones, DONE: Verified""") #DONE: Verified
    #SetCountriesEconomicsZonesInDB(connectionstring=ProdConnectionString)

    print("""3. Download all the Stocks Infos, DONE: Verified """)

    # """parameter: library = comp, table= [security, names], observation = int, offset = int, globalWrds =true/false."""
    # params = StocksInfosParams(library='comp', table=['g_security', 'g_names'], globalWRDS=True)
    # GetStocksInfosDataDict(params)
    # print("GLobal Completed")
    # params = StocksInfosParams(library='comp', table=['security', 'names'], globalWRDS=False)
    # GetStocksInfosDataDict(params)
    # print("North America Completed, DONE: Verfified")
    #
    # SetStocksInfosDataInDB(ProdConnectionString)

    print("4. Donwnload All Economics zones and add Zones to Stocks Infos: Done Verified")

    # SetCountriesEconomicsZonesForStocksInDB(ProdConnectionString)

    print("5. Download Recommendation Infos. Done Verified")
    # SetStocksInfosRecommendationsInDB(type_price_target, ProdConnectionString)
    # SetStocksInfosRecommendationsInDB(type_consensus, ProdConnectionString)

    print("")

    print("5. Download Stock Price Data,")

    db = wrds.Connection()
    count = db.get_row_count(library="comp",
                            table="secd")
    db.close()
    observ = 1000000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
    pt = ()
    for v in range(iter):
        pt += StocksPriceParams(library='comp',
                                table='secd',
                                observation=observ,
                                offset=v * 1000000,
                                globalWRDS= False,
                                connectionstring=ProdConnectionString),
    pool = MyPool(2)
    result = pool.map(GetStocksPriceData, pt)
    pool.close()
    pool.join()
    print(result)

    db = wrds.Connection()
    count = db.get_row_count(library="comp",
                             table="g_secd")
    db.close()
    observ = 1000000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
    pt = ()
    for v in range(iter):
       pt += StocksPriceParams(library='comp',
                                table='g_secd',
                                observation=observ,
                                offset=v * 1000000,
                                globalWRDS=True,
                               connectionstring=ProdConnectionString),
    pool = MyPool(2)
    result = pool.map(GetStocksPriceData, pt)
    pool.close()
    pool.join()
    print(result)

    print("6. Set currency in the Stocks Price: TO DO")

    # params = ()
    # for month in GenerateMonthlyTab('1984-01', '2018-011'):
    #     params += YearParams(date=month),
    #
    # pool = MyPool(16)
    # result = pool.map(ConvertStocksPriceToUSD, params)
    # pool.close()
    # pool.join()


    print("7. Set Price Target and Consensus Data")


    db = wrds.Connection()
    count = db.get_row_count(library="ibes",
                             table="ptgdet")
    db.close()
    print(count)
    observ = 1000000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
    pt = ()
    for v in range(iter):
       pt += StocksRecommentdationsParams(library='ibes',
                                          table='ptgdet',
                                          observation=observ,
                                          offset=v * 1000000,
                                          type= type_price_target,
                                          connectionstring=ProdConnectionString),
    pool = MyPool(2)
    result = pool.map(GetStocksPriceRecommendations, pt)
    pool.close()
    pool.join()
    print(result)

    db = wrds.Connection()
    count = db.get_row_count(library="ibes",
                             table="recddet")
    db.close()
    observ = 1000000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
    pt = ()
    for v in range(iter):
       pt += StocksRecommentdationsParams(library='ibes',
                                          table='recddet',
                                          observation=observ,
                                          offset=v * 1000000,
                                          type= type_consensus,
                                          connectionstring=ProdConnectionString),
    pool = MyPool(2)
    result = pool.map(GetStocksPriceRecommendations, pt)
    pool.close()
    pool.join()
    print(result)


"7. Set all Price target to USD"
# params = ()
# for month in GenerateMonthlyTab('1984M1', '2018M11'):
#     params += YearParams(date=month),
#
# pool = MyPool(principal_processor)
# result = pool.map(ConvertPriceTagetToUSD, params)
# pool.close()
# pool.join()
# description = 'Backup Add Currency to the Price Target'
# SetBackupOfDataBase(description)

# "8. Patch all PriceRecommendations Data"
# params = ()
#
# for value in PriceTargetAndconsensusInfosData(ClientDB,type_consensus, {}, {'_id':1, 'ticker':1}).GetInfosFromDB():
#     params += PatchStockPriceRecommendationsDataParams(type=type_consensus,
#                                                        query=[{"cusip": value['_id']}, {"ticker": value["ticker"]}]),
#
# pool = MyPool(principal_processor)
# result = pool.map(PatchStocksPriceRecommendations, params)
# pool.close()
# pool.join()
#
# params = ()
#
# for value in PriceTargetAndconsensusInfosData(ClientDB, type_price_target, {}, {'_id': 1, 'ticker': 1}).GetInfosFromDB():
#     params += PatchStockPriceRecommendationsDataParams(type=type_price_target,
#                                                        query=[{"cusip": value['_id']}, {"ticker": value["ticker"]}]),
#
# pool = MyPool(principal_processor)
# result = pool.map(PatchStocksPriceRecommendations, params)
# pool.close()
# pool.join()
#
# "9. Add Gvkey to StockPriceRecommendationsInfos"
# params = PatchStockPriceRecommendationsDataParams(type=type_consensus, query=0)
# SetGvkeyInStocksPriceRecoomendationsInfos(params)
# params = PatchStockPriceRecommendationsDataParams(type=type_price_target, query=0)
# SetGvkeyInStocksPriceRecoomendationsInfos(params)
#
# "10. Merge all Stocks Data"
# params = ()
# for month in GenerateMonthlyTab('1984M1', '2018M11'):
#     params += MergeStocksWithPriceRecommendationsParams(date=month, type=type_consensus),
#
# pool = MyPool(principal_processor)
# result = pool.map(MergeStocksWithPriceRecommendations, params)
# pool.close()
# pool.join()
#
# params = ()
# for month in GenerateMonthlyTab('1984M1', '2018M11'):
#     params += MergeStocksWithPriceRecommendationsParams(date=month, type=type_price_target),
#
# pool = MyPool(principal_processor)
# result = pool.map(MergeStocksWithPriceRecommendations, params)
# pool.close()
# pool.join()


