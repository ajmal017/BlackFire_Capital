import collections
import wrds
import multiprocessing
import multiprocessing.pool

from bBlackFireCapitalData.CountriesEconomicsData.CountriesExchangeRatesData.GetExchangeRatesData import \
    SetExchangeRatesCurrencyInDB
from bBlackFireCapitalData.StocksMarketData.StocksPriceData.GetStocksInfosDataFromWRDS import SetStocksInfosDataInDB
from bBlackFireCapitalData.CountriesEconomicsData.CountriesEconomicsZoneData.GetCountriesEconomicsZoneData import \
    SetCountriesEconomicsZonesInDB, SetCountriesEconomicsZonesForStocksInDB
from bBlackFireCapitalData.MergeStocksData.MergeAllStocksData import SetGvkeyInStocksPriceRecoomendationsInfos, \
    MergeStocksWithPriceRecommendations
from bBlackFireCapitalData.StocksMarketData.StocksPriceData.GetStocksPriceDataFromWRDS import GetStocksPriceData, \
    ConvertStocksPriceToUSD
from bBlackFireCapitalData.StocksMarketData.StocksPriceRecommendationData.GetStocksPriceRecommendations import \
    GetStocksPriceRecommendations, ConvertPriceTagetToUSD, PatchStocksPriceRecommendations
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateMonthlyTab, principal_processor, \
    type_consensus, type_price_target, ClientDB
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
      'offset'])

StocksRecommentdationsParams = collections.namedtuple('StocksRecommentdationsParams',[
      'table',
      'library',
      'type',
      'observation',
      'offset'])

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

"""This function set all the data inside the platforms"""


"""1. Download of all the currency pair"""
SetExchangeRatesCurrencyInDB(currency_from ='USD')
SetExchangeRatesCurrencyInDB(currency_from='EUR')
SetExchangeRatesCurrencyInDB(currency_from='GBP')

"""2. Download all the Stocks Infos """
"""parameter: library = comp, table= [security, names], observation = int, offset = int, globalWrds =true/false."""
params = StocksInfosParams(table='comp', library=['g_security', 'g_names'], globalWRDS=True)
SetStocksInfosDataInDB(params)

params = StocksInfosParams(table='comp', library=['security', 'names'], globalWRDS=False)
SetStocksInfosDataInDB(params)

"3. Donwnload All Economics zones and add Zones to Stocks Infos"
SetCountriesEconomicsZonesInDB()
SetCountriesEconomicsZonesForStocksInDB()

"4. Download Stock Price Data"

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
                            globalWRDS= False),
pool = MyPool(2)
result = pool.map(GetStocksPriceData, pt)
pool.close()
pool.join()
print(result)

db = wrds.Connection()
count = db.get_row_count(library="g_comp",
                         table="g_secd")
db.close()
observ = 1000000
iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
pt = ()
for v in range(iter):
   pt += StocksPriceParams(library='g_comp',
                            table='g_secd',
                            observation=observ,
                            offset=v * 1000000,
                            globalWRDS= True),
pool = MyPool(2)
result = pool.map(GetStocksPriceData, pt)
pool.close()
pool.join()
print(result)

"5. Set currency in the Stocks Price"

params = ()
for month in GenerateMonthlyTab('1984M1', '2018M11'):
    params += YearParams(date=month),

pool = MyPool(principal_processor)
result = pool.map(ConvertStocksPriceToUSD, params)
pool.close()
pool.join()

"6. Set Price Target and Consensus Data"

db = wrds.Connection()
count = db.get_row_count(library="ibes",
                         table="ptgdet")
db.close()
observ = 1000000
iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1
pt = ()
for v in range(iter):
   pt += StocksRecommentdationsParams(library='ibes',
                                      table='ptgdet',
                                      observation=observ,
                                      offset=v * 1000000,
                                      type= type_price_target),
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
                                      type= type_consensus),
pool = MyPool(2)
result = pool.map(GetStocksPriceRecommendations, pt)
pool.close()
pool.join()
print(result)

"7. Set all Price target to USD"
params = ()
for month in GenerateMonthlyTab('1984M1', '2018M11'):
    params += YearParams(date=month),

pool = MyPool(principal_processor)
result = pool.map(ConvertPriceTagetToUSD, params)
pool.close()
pool.join()

"8. Patch all PriceRecommendations Data"
params = ()

for value in PriceTargetAndconsensusInfosData(ClientDB,type_consensus, {}, {'_id':1, 'ticker':1}).GetInfosFromDB():
    params += PatchStockPriceRecommendationsDataParams(type=type_consensus,
                                                       query=[{"cusip": value['_id']}, {"ticker": value["ticker"]}]),

pool = MyPool(principal_processor)
result = pool.map(PatchStocksPriceRecommendations, params)
pool.close()
pool.join()

params = ()

for value in PriceTargetAndconsensusInfosData(ClientDB, type_price_target, {}, {'_id': 1, 'ticker': 1}).GetInfosFromDB():
    params += PatchStockPriceRecommendationsDataParams(type=type_price_target,
                                                       query=[{"cusip": value['_id']}, {"ticker": value["ticker"]}]),

pool = MyPool(principal_processor)
result = pool.map(PatchStocksPriceRecommendations, params)
pool.close()
pool.join()

"9. Add Gvkey to StockPriceRecommendationsInfos"
params = PatchStockPriceRecommendationsDataParams(type=type_consensus, query=0)
SetGvkeyInStocksPriceRecoomendationsInfos(params)
params = PatchStockPriceRecommendationsDataParams(type=type_price_target, query=0)
SetGvkeyInStocksPriceRecoomendationsInfos(params)

"10. Merge all Stocks Data"
params = ()
for month in GenerateMonthlyTab('1984M1', '2018M11'):
    params += MergeStocksWithPriceRecommendationsParams(date=month, type=type_consensus),

pool = MyPool(principal_processor)
result = pool.map(MergeStocksWithPriceRecommendations, params)
pool.close()
pool.join()

params = ()
for month in GenerateMonthlyTab('1984M1', '2018M11'):
    params += MergeStocksWithPriceRecommendationsParams(date=month, type=type_price_target),

pool = MyPool(principal_processor)
result = pool.map(MergeStocksWithPriceRecommendations, params)
pool.close()
pool.join()


