import motor
import tornado
import numba
from multiprocessing import Pool
import numpy as np
import collections
import pandas as pd
from pathlib import Path
from pymongo import InsertOne
from datetime import  datetime
import wrds


from aBlackFireCapitalClass.ClassEconomcisZonesData.ClassEconomicsZonesDataInfos import EconomicsZonesDataInfos
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataPrice import SectorsMarketDataPrice
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString, TestConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateMonthlyTab, profile, country_zone_and_exchg

set_sector_tuple = collections.namedtuple('set_sector_tuple', [
    'naics',
    'zone_eco',
])

monthly_stocks_price = collections.namedtuple('monthly_stocks_price', ['month', 'stocks_by_sector'])

__ENTETE__ = ['eco zone', 'naics', 'csho', 'vol', 'pc', 'ph', 'pl', 'nptvar', 'ptvar', 'npptvar',
              'pptvar', 'nrc', 'rc', 'nrcvar', 'rcvar', 'nstocks']
@numba.jit
def split_price_target(args):

    return pd.Series([args['price_target']['price'], args['price_target']['num_price'],
                      args['price_target']['mean_var'], args['price_target']['num_var'],
                      args['price_target']['pmean_var'], args['price_target']['pnum_var']])
@numba.jit
def split_consensus(args):

    return pd.Series([args['consensus']['mean_recom'], args['consensus']['num_recom'],
                      args['consensus']['mean_var'], args['consensus']['num_var']])

def SectorGrouping(group):

    #Identification
    ecozone = list(group['v1'])[0]
    naics = list(group['v2'])[0]

    #Stocks Infos
    csho = group['csho'].sum()
    vol = group['vol'].sum()
    try:
        pc = (group['csho'] * group['pc']/group['USDtocurr']).sum()/csho
        ph = (group['csho'] * group['ph']/group['USDtocurr']).sum()/csho
        pl = (group['csho'] * group['pl']/group['USDtocurr']).sum()/csho
    except:
        pc = None
        ph = None
        pl = None

    #PT infos
    try:
        nptvar = group['nptvar'].sum()
        ptvar = (group['ptvar'] * group['nptvar']).sum()/nptvar
    except ZeroDivisionError:
        ptvar = None

    try:
        npptvar = group['npptvar'].sum()
        pptvar = (group['pptvar'] * group['nptvar']).sum()/npptvar
    except ZeroDivisionError:
        pptvar = None

    #CS infos
    try:
        nrc = group['nrc'].sum()
        rc = (group['rc'] * group['nrc']).sum()/nrc
    except ZeroDivisionError:
        rc = None
    try:
        nrcvar = group['nrcvar'].sum()
        rcvar = (group['rcvar'] * group['nrcvar']).sum()/nrcvar
    except ZeroDivisionError:
        rcvar = None



    nstocks = group['nstocks'].sum()

    tab = [ecozone, naics, csho, vol, pc, ph, pl, nptvar, ptvar, npptvar, pptvar, nrc, rc, nrcvar, rcvar, nstocks]

    return pd.DataFrame([tab], columns=__ENTETE__)


def correct_stocks(group):

    group.sort_index(inplace=True, ascending=True)
    group[['csho', 'pc']] = group[['csho', 'pc']].fillna(method='bfill')
    # return group[['csho', 'pc']]


def BulkWriteData(csho, vol, pc, ph, pl, nptvar, ptvar, npptvar, pptvar, rc, nrc, rcvar, nrcvar, eco, naics, date):

    return InsertOne({'csho': csho, "vol": vol, 'price_close': pc, 'price_high': ph,
                      "price_low": pl,
                      'price_target':{'num_var': nptvar, 'mean_var': ptvar, 'pnum_var': npptvar,
                                      'pmean_var': pptvar},
                      'consensus': {'mean_recom': rc, 'num_recom': nrc, 'mean_var': rcvar,
                                    "num_var": nrcvar},
                      'eco zone': eco, 'naics': naics, 'date': datetime(int(date[0:4]), int(date[5:]), 15)})


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


def filterStocksPerTradeStocksExchange():



    StocksBySector = np.load('StocksBySector.npy')
    StocksBySector = pd.DataFrame(StocksBySector, columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg'])
    print(StocksBySector.shape)

    tabStocksExchange = (",".join([",".join(value[3:]) for value in country_zone_and_exchg])).split(',')

    StocksBySector = StocksBySector[StocksBySector['exchg'].isin(tabStocksExchange)]
    print(StocksBySector.shape)

    print(tabStocksExchange)
    np.save('StocksBySectorWithLiquidExchg',StocksBySector)

# @profile
def SetSectorPriceToDB(params):

    """"
    Set Sector Price in DB

    Use this function when you need to aggregate the stocks price by Sector and by Economics zones. This function
    is useful to build some specific sectors indexes.
    The economics zones are given by the currencies of developped and emerging countries.
    The sectors are the NAICS sectors for levels 1 and 2.

    Parameters
    ----------
    connection_string: url of the connection to the MongoDB.

    Returns
    -------


    """""
    #Pipeline to query the last price of the month

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
    ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)


    #Get Last Stocks Price of the month from the MongoDB
    loop = tornado.ioloop.IOLoop
    tab_of_stocks_price = loop.current().run_sync(StocksMarketDataPrice(ClientDB, params.month, pipeline).GetMontlyPrice)
    tab_of_stocks_price.rename(columns = {'_id': 'isin', 'price_close': 'pc', 'price_high': 'ph', 'price_low': 'pl',
                                          'USD_to_curr': 'USDtocurr'},
                               inplace=True)
    ClientDB.close()

    #Split Price Target information
    _filter = tab_of_stocks_price[tab_of_stocks_price['price_target'].notna()]
    result = _filter.apply(split_price_target, axis = 1)
    result['isin'] = _filter['isin']
    result.columns = ['pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar', 'isin']
    tab_of_stocks_price = pd.merge(tab_of_stocks_price, result, on=['isin'], how='left')

    #Split Consensus information
    _filter = tab_of_stocks_price[tab_of_stocks_price['consensus'].notna()]
    result = _filter[['consensus']].apply(split_consensus, axis = 1)
    result['isin'] = _filter['isin']
    result.columns = ['rc', 'nrc', 'rcvar', 'nrcvar', 'isin']
    tab_of_stocks_price = pd.merge(tab_of_stocks_price, result, on=['isin'], how='left')

    entete = ['isin', 'gvkey', 'curr', 'csho', 'vol', 'adj_factor', 'pc', 'ph', 'pl', 'USDtocurr','pt', 'npt', 'ptvar',
              'nptvar', 'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar', 'date']

    tab_of_stocks_price = pd.merge(params.stocks_by_sector, tab_of_stocks_price[entete],on=['gvkey', 'isin'], how='inner')
    print('Periode {0} Completed'.format(params.month))
    return tab_of_stocks_price


    for o in [0]:
        tab_result = []

        for value in tab_of_stocks_price:

            tPrice = [value['_id'], value['gvkey'], value['curr'], value['csho'], value['vol'], value['adj_factor'],
                 value['price_close'], value['price_high'], value['price_low'], value['USD_to_curr']]

            if value['price_target'] is not None:

                _  = value['price_target']
                _['price'] = None if _['price'] == "None" else float (_['price'])
                _['mean_var'] = None if _['mean_var'] == "None" else float (_['mean_var'])
                _['pmean_var'] = None if _['pmean_var'] == "None" else float (_['pmean_var'])

                tPriceTarget = [_['price'], _['num_price'], _['mean_var'], _['num_var'], _['pmean_var'], _['pnum_var']]
            else:
                tPriceTarget = [None, None, None, None, None, None]

            if value['consensus'] is not None:
                _  = value['consensus']
                _['mean_recom'] = None if _['mean_recom'] == "None" else float (_['mean_recom'])
                _['mean_var'] = None if _['mean_var'] == "None" else float (_['mean_var'])
                tConsensus = [_['mean_recom'], _['num_recom'], _['mean_var'], _['num_var']]
            else:
                tConsensus = [None, None, None, None]

            tab_result.append(tPrice + tPriceTarget + tConsensus)

        del tab_of_stocks_price


        tab_result = pd.DataFrame(tab_result, columns=['isin', 'gvkey', 'curr', 'csho', 'vol', 'adj_factor', 'pc', 'ph',
                                                       'pl', 'USDtocurr','pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar',
                                                       'rc', 'nrc', 'rcvar', 'nrcvar'])




        tab_result[['csho', 'vol', 'adj_factor', 'pc', 'ph','pl', 'USDtocurr','pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar',
                                                       'rc', 'nrc', 'rcvar', 'nrcvar']] \
            = tab_result[['csho', 'vol', 'adj_factor', 'pc', 'ph','pl', 'USDtocurr','pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar',
                                                       'rc', 'nrc', 'rcvar', 'nrcvar']].astype(float)


        tab_result['mc'] = tab_result['csho'] * tab_result['pc']/tab_result['USDtocurr']
        tab_result['nstocks'] = 1
        tab_result = tab_result.sort_values(["gvkey", "mc"], ascending=[True, False])

        tab_result = tab_result.drop_duplicates(['eco zone','naics','gvkey'])
        tab_result['v1'] = tab_result['eco zone']
        tab_result['v2'] = tab_result['naics']

        "Group by Eco Zone and Naics"
        tabGroup = tab_result.groupby(['eco zone', 'naics']).apply(SectorGrouping)
        tabGroup = pd.DataFrame(np.array(tabGroup), columns=__ENTETE__)

        del tab_result

        "Group by Eco Zone"
        tabGroup['USDtocurr'] = 1
        tabGroup['v1'] = tabGroup['eco zone']
        tabGroup['v2'] = 'ALL'

        tabGroupByCountry = tabGroup.groupby(['eco zone']).apply(SectorGrouping)
        tabGroupByCountry = pd.DataFrame(np.array(tabGroupByCountry), columns=__ENTETE__)


        "Group by NAICS"
        tabGroup = tabGroup[__ENTETE__]
        tabGroup['USDtocurr'] = 1
        tabGroup['v1'] = "WLD"
        tabGroup['v2'] = tabGroup['naics']

        tabGroupByNAICS = tabGroup.groupby(['naics']).apply(SectorGrouping)
        tabGroupByNAICS = pd.DataFrame(np.array(tabGroupByNAICS), columns=__ENTETE__)


        "World Indices"
        tabGroup = tabGroup[__ENTETE__]
        tabGroup['USDtocurr'] = 1
        tabGroup['v1'] = "WLD"
        tabGroup['v2'] = "ALL"
        tabGroup['ALL'] = "ALL"

        tabGroupWLD = tabGroup.groupby(['ALL']).apply(SectorGrouping)
        tabGroupWLD = pd.DataFrame(np.array(tabGroupWLD), columns=__ENTETE__)


        tabGroup = tabGroup[__ENTETE__]

        tabGroup = tabGroup.append(tabGroupByCountry)
        tabGroup = tabGroup.append(tabGroupByNAICS)
        tabGroup = tabGroup.append(tabGroupWLD)

        tabGroup['date'] = month

        v = np.vectorize(BulkWriteData)

        tabGroup['data'] = v(tabGroup['csho'], tabGroup['vol'], tabGroup['pc'],
                                                                  tabGroup['ph'], tabGroup['pl'], tabGroup['nptvar'],
                                                                  tabGroup['ptvar'], tabGroup['npptvar'], tabGroup['pptvar'],
                                                                  tabGroup['rc'], tabGroup['nrc'], tabGroup['rcvar'],
                                                                  tabGroup['nrcvar'], tabGroup['eco zone'], tabGroup['naics'],
                                                                  tabGroup['date'])


        tabtoinsert = list(tabGroup['data'])

        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(SectorsMarketDataPrice(ClientDB, tabtoinsert).SetSectorsPriceInDB)


def patch_stocks_price(monthly_prices) -> pd.DataFrame:

    """"
    This function is used to correct all the mistakes in the price and csho of the data. To correct the mistake,
    we make a rolling of 3 months. if there is a missing value on the month 2, the price is replace by the mean
    between month 1 and 2 and the csho take the value of the month 1.

    :param
    monthly_prices: dataFrame containing monthly price of the stocks

    :return
    Dataframe of Price with correction
    """""
    entete = ['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr', 'csho', 'vol', 'adj_factor', 'pc', 'ph', 'pl',
              'USDtocurr', 'pt', 'npt', 'ptvar', 'nptvar', 'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar', 'date']
    monthly_prices = pd.DataFrame(monthly_prices, columns=entete)
    monthly_prices.iloc[monthly_prices.iloc[:, :] == 'None'] = None
    monthly_prices['date'] = pd.to_datetime(monthly_prices['date'])
    monthly_prices[['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr']] = monthly_prices[['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr']].astype(str)
    monthly_prices[[ 'csho', 'vol', 'adj_factor', 'pc', 'ph', 'pl', 'USDtocurr', 'pt', 'npt', 'ptvar', 'nptvar',
                     'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']] = monthly_prices[[ 'csho', 'vol', 'adj_factor',
                    'pc', 'ph', 'pl', 'USDtocurr', 'pt', 'npt', 'ptvar', 'nptvar',
                     'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']].astype(float)
    print(monthly_prices.info())
    monthly_prices = monthly_prices.set_index('date')

    #Group by cusip and apply the corrections
    monthly_prices = monthly_prices[(monthly_prices['naics']=='52') & (monthly_prices['eco zone']=='GBP')]
    print(monthly_prices[monthly_prices['csho'] == 0])

    result = monthly_prices[['naics', 'isin', 'pc', 'csho', 'curr', 'USDtocurr']].groupby(['naics', 'isin']).apply(correct_stocks)
    print(result)
    # print(monthly_prices[['isin', 'csho', 'pc']].head())
    # monthly_prices[['csho', 'pc']].rolling(3, min_periods=3).apply(correct_stocks, raw=True)
    # print(monthly_prices[['isin', 'naics', 'csho']].groupby(['isin', 'naics']).apply(correct_stocks))


def getSectorsPrice(params):


    # ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    # # loop = tornado.ioloop.IOLoop
    # # loop.current().run_sync(SectorsMarketDataPrice(ClientDB, None).create_index)
    #
    # loop = tornado.ioloop.IOLoop
    # tabSectorInfos = loop.current().run_sync(SectorsMarketDataPrice(ClientDB, {}, None).GetStocksPriceFromDB)
    #
    # tab_to_save = []
    #
    # for value in tabSectorInfos:
    #
    #
    #     eco = value['eco zone']
    #     naics = value['naics']
    #     date = value['date']
    #
    #     csho = value['csho']
    #     vol = value['vol']
    #     pc = value['price_close']
    #     ph = value['price_high']
    #     pl = value['price_low']
    #
    #     pt = value['price_target']
    #     cs = value['consensus']
    #
    #     t = [date, eco, naics, csho, vol, pc, ph, pl, pt['num_var'], pt['mean_var'], pt['pnum_var'], pt['pmean_var'],
    #          cs['num_recom'], cs['mean_recom'], cs['num_var'], cs['mean_var']]
    #
    #     tab_to_save.append(t)
    #
    # np.save('tabSectorPrice.npy', tab_to_save)

    # ClientDB.close()
    tab = np.load('tabSectorPrice.npy')
    print(tab.shape)



if __name__ == "__main__":
    # GetListofEcoZoneAndNaics(ProdConnectionString)
    # AddStocksPerNaicsAndEcoZone()
    # filterStocksPerTradeStocksExchange()

    stocks_by_sector = np.load('StocksBySectorWithLiquidExchg.npy')
    stocks_by_sector = pd.DataFrame(stocks_by_sector, columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg'])
    tabofMonth = GenerateMonthlyTab('2017-09', '2017-12')
    tup = ()

    for month in tabofMonth:
        tup += monthly_stocks_price(month=month, stocks_by_sector=stocks_by_sector),
    pool = Pool(4)
    result = pool.map(SetSectorPriceToDB, tup)
    pool.close()
    pool.join()

    df = pd.concat(result)
    # print(df.columns)
    monthly_prices = np.save('monthly_prices.npy', df)
    # patch_stocks_price(monthly_prices)
    # print(df)


    # getSectorsPrice('')
    # db = wrds.Connection()
    # count = db.get_row_count(library='comp', table='g_funda')
    # print(count)
    #
    # count = db.get_row_count(library='comp', table='g_fundq')
    # print(count)
    #
    # count = db.get_row_count(library='comp', table='funda')
    # print(count)
    #
    # count = db.get_row_count(library='comp', table='fundq')
    # print(count)
    #
    # db.close()