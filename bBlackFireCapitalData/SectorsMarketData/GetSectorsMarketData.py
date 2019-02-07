import motor
import tornado
import numba
from multiprocessing import Pool
import multiprocessing.pool
import numpy as np
import collections
import pandas as pd
from pathlib import Path
from pymongo import InsertOne
from datetime import datetime
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

monthly_stocks_price = collections.namedtuple('monthly_stocks_price', ['value', 'data_table'])

__ENTETE__ = ['eco zone', 'naics', 'csho', 'vol', 'pc', 'ph', 'pl', 'nptvar', 'ptvar', 'npptvar',
              'pptvar', 'nrc', 'rc', 'nrcvar', 'rcvar', 'nstocks']


class NoDaemonProcess(multiprocessing.Process):

    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


@numba.jit
def split_value(args):
    return args.data_table[args.value].apply(pd.Series)


def SectorGrouping(group):
    # Identification
    ecozone = list(group['v1'])[0]
    naics = list(group['v2'])[0]

    # Stocks Infos
    csho = group['csho'].sum()
    vol = group['vol'].sum()
    try:
        pc = (group['csho'] * group['pc'] / group['USDtocurr']).sum() / csho
        ph = (group['csho'] * group['ph'] / group['USDtocurr']).sum() / csho
        pl = (group['csho'] * group['pl'] / group['USDtocurr']).sum() / csho
    except:
        pc = None
        ph = None
        pl = None

    # PT infos
    try:
        nptvar = group['nptvar'].sum()
        ptvar = (group['ptvar'] * group['nptvar']).sum() / nptvar
    except ZeroDivisionError:
        ptvar = None

    try:
        npptvar = group['npptvar'].sum()
        pptvar = (group['pptvar'] * group['nptvar']).sum() / npptvar
    except ZeroDivisionError:
        pptvar = None

    # CS infos
    try:
        nrc = group['nrc'].sum()
        rc = (group['rc'] * group['nrc']).sum() / nrc
    except ZeroDivisionError:
        rc = None
    try:
        nrcvar = group['nrcvar'].sum()
        rcvar = (group['rcvar'] * group['nrcvar']).sum() / nrcvar
    except ZeroDivisionError:
        rcvar = None

    nstocks = group['nstocks'].sum()

    tab = [ecozone, naics, csho, vol, pc, ph, pl, nptvar, ptvar, npptvar, pptvar, nrc, rc, nrcvar, rcvar, nstocks]

    return pd.DataFrame([tab], columns=__ENTETE__)


def correct_group_stocks(group):
    # Find duplicates ISIN by gvkey
    result = group.groupby(['isin', 'curr']).apply(correct_stocks).reset_index(drop=True)
    r = result.groupby(['isin', 'curr'])['return'].nunique().reset_index()
    index_max = r['return'].idxmax()
    isin = r.loc[index_max, 'isin']
    curr = r.loc[index_max, 'curr']

    return result[(result['isin'] == isin) & (result['curr'] == curr)]


def correct_stocks(group):
    group.set_index('date', inplace=True)
    group[['csho', 'adj_pc']] = group[['csho', 'adj_pc']].fillna(method='ffill')
    group['return'] = group['adj_pc'].pct_change(fill_method='ffill', freq='1M')
    group['pt_return'] = group[['adj_pc', 'pt']].pct_change(axis='columns')['pt']
    group.reset_index(inplace=True)

    return group


def shift_mc(group, periode):
    group['index'] = group.index
    group = group.set_index('date')
    t = group[['mc']].shift(periods=periode, freq='M')
    group.loc[:, 'mc'] = t['mc']

    return group.set_index('index')[['return']]


def calculate_sectors_summary(group) -> pd.DataFrame:
    """
        This function is used to compute all the summary informations for the sectors. With this function
        we compute:
        - return: sector return for month t
        - mc:  market capitalisation of the sector at month t
        - vol: volume traded for the sector at month t
        - pt_return: price target return of the sector at month t
        - mpt_return: mean of price target return of the sector at month t
        
        :param group: Data frame containing the stocks summary information's for month t
         
        :return:  Data frame of sectors summary for month t.
    """

    sum = ['mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrc', 'nrcvar']
    try:
        ret = (group['mc'] * group['return']).sum() / group['mc'].sum()
    except:
        ret = None
    pt_return = (group['mc'] * group['pt_return']).sum() / group['mc'].sum()
    mpt_return = group['pt_return'].mean()

    try:
        pptvar = (group['pptvar'] * group['npptvar']).sum() / group['npptvar'].sum()
    except ZeroDivisionError:
        pptvar = None

    try:
        ptvar = (group['ptvar'] * group['nptvar']).sum() / group['nptvar'].sum()
    except ZeroDivisionError:
        ptvar = None

    try:
        rc = (group['rc'] * group['nrc']).sum() / group['nrc'].sum()
    except ZeroDivisionError:
        rc = None

    try:
        rcvar = (group['rcvar'] * group['nrcvar']).sum() / group['nrcvar'].sum()
    except ZeroDivisionError:
        rcvar = None

    group[sum].sum()

    tab = pd.DataFrame([ret, pt_return, mpt_return, pptvar, ptvar, rc, rcvar],
                       columns=['return', 'pt_return', 'mpt_return', 'pptvar', 'ptvar', 'rc', 'rcvar'])

    return pd.concat([group[sum].sum().reset_index(drop=True), tab], axis=1, ignore_index=True)


def bulk_write_data(csho, vol, pc, ph, pl, nptvar, ptvar, npptvar, pptvar, 
                    rc, nrc, rcvar, nrcvar, eco, naics, date):
    """"
        This function is used to create the InsertOne to bulkwwrite sector prices and informations inside the
        DB.
        The Insertone will be used inside a DataFrame to bulkWrite directly in Batches.

        :parameter
        csho: common shares for the index.
        vol: total volume traded.
        pc: price close of the sector

        :return
        pymongo.InsertOne. Data to insert into the DB in a dictionnary form.

    """

    return InsertOne({'csho': csho, "vol": vol, 'price_close': pc, 'price_high': ph,
                      "price_low": pl,
                      'price_target': {'num_var': nptvar, 'mean_var': ptvar, 'pnum_var': npptvar,
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
        naics = group.iloc[0, 1]
        zone = group.iloc[0, 0]
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

    np.save('StocksBySector', tabGroupNaicsAndSector)


def filterStocksPerTradeStocksExchange():
    StocksBySector = np.load('StocksBySector.npy')
    StocksBySector = pd.DataFrame(StocksBySector, columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg'])
    print(StocksBySector.shape)

    tabStocksExchange = (",".join([",".join(value[3:]) for value in country_zone_and_exchg])).split(',')

    StocksBySector = StocksBySector[StocksBySector['exchg'].isin(tabStocksExchange)]
    print(StocksBySector.shape)

    print(tabStocksExchange)
    np.save('StocksBySectorWithLiquidExchg', StocksBySector)


# @profile
def get_monthly_stocks_summary_from_the_db(parameter):
    # TODO: Update description of this function
    """"
    Set Sector Price in DB

    Use this function when you need to aggregate the stocks price by Sector and by Economics zones. This function
    is useful to build some specific sectors indexes.
    The economics zones are given by the currencies of developped and emerging countries.
    The sectors are the NAICS sectors for levels 1 and 2.

    :Parameters
    ----------
    connection_string: url of the connection to the MongoDB.

    Returns
    -------


    """""
    # Pipeline to query the last price of the month
    print('start: ', parameter.value)
    pipeline = [{'$sort': {"isin_or_cusip": 1, "date": 1}},
                {
                    "$group": {
                        "_id": {'isin_or_cusip': "$isin_or_cusip", "curr": "$curr"},
                        "date": {"$last": "$date"},
                        "gvkey": {"$last": "$gvkey"},
                        "csho": {"$last": "$csho"},
                        "vol": {"$sum": "$vol"},
                        "adj_factor": {"$last": "$adj_factor"},
                        "price_close": {"$last": "$price_close"},
                        "price_high": {"$max": "$price_high"},
                        "price_low": {"$min": "$price_low"},
                        "USD_to_curr": {"$last": "$USD_to_curr"},
                        "consensus": {"$last": "$consensus"},
                        "price_target": {"$last": "$price_target"},

                    }
                }]

    ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    # SetIndexCreation
    loop = tornado.ioloop.IOLoop
    tab_of_stocks_price = loop.current().run_sync(
        StocksMarketDataPrice(ClientDB, parameter.value, pipeline).SetIndexCreation)

    # Get Last Stocks Price of the month from the MongoDB
    loop = tornado.ioloop.IOLoop
    tab_of_stocks_price = loop.current().run_sync(
        StocksMarketDataPrice(ClientDB, parameter.value, pipeline).GetMontlyPrice)

    ClientDB.close()

    tup = ()
    for value in ['_id', 'price_target', 'consensus']:
        tup += monthly_stocks_price(value=value, data_table=tab_of_stocks_price),
    pool = Pool(3)
    result = pool.map(split_value, tup)
    pool.close()
    pool.join()
    tab_of_stocks_price = pd.concat([tab_of_stocks_price.drop(['_id', 'price_target', 'consensus'], axis=1)] + result,
                                    axis=1)
    tab_of_stocks_price.rename(
        columns={'isin_or_cusip': 'isin', 'price': 'pt', 'num_price': 'npt', 'pmean_var': 'pptvar',
                 'pnum_var': 'npptvar', 'mean_var': 'ptvar', 'num_var': 'nptvar', 'mean_recom': 'rec',
                 'num_recom': 'nrec', 'price_close': 'pc',
                 'price_high': 'ph', "price_low": 'pl'},
        inplace=True)
    tab_of_stocks_price.rename(columns={tab_of_stocks_price.columns[19]: "rcvar",
                                        tab_of_stocks_price.columns[20]: "nrcvar"},
                               inplace=True)

    tab_of_stocks_price = pd.merge(parameter.data_table, tab_of_stocks_price,
                                   on=['gvkey', 'isin'], how='inner')
    tab_of_stocks_price.drop_duplicates(['gvkey', 'isin', 'curr', 'date', 'naics'], inplace=True)
    return tab_of_stocks_price


def patch_stocks_price(parameter) -> pd.DataFrame:
    """"
    This function is used to correct all the mistakes in the price and csho of the data. To correct the mistake,
    we make a rolling of 3 months. if there is a missing value on the month 2, the price is replace by the mean
    between month 1 and 2 and the csho take the value of the month 1.

    :param
    monthly_prices: dataFrame containing monthly price of the stocks

    :return
    Dataframe of Price with correction
    """""
    print('start')
    entete = ['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'USDtocurr', 'adj_factor', 'csho', 'date', 'pc', 'ph',
              'pl', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar', 'nptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']

    # set all null csho to None
    monthly_prices = parameter.data_table

    monthly_prices.loc[monthly_prices['csho'] == 0, 'csho'] = None
    monthly_prices.loc[monthly_prices['pc'] == 0, 'pc'] = None

    # Order columns by naics gvkey and isin
    monthly_prices = monthly_prices.sort_values(by=['naics', 'gvkey', 'isin', 'curr', 'date'],
                                                ascending=[False, True, True, True, True])

    # adjusted price
    monthly_prices.loc[:, 'adj_pc'] = monthly_prices['pc'] / monthly_prices['adj_factor'] / monthly_prices['USDtocurr']

    # fill csho, adjusted price by the previous value in cases of NaN and calculate returns
    result = monthly_prices[['naics', 'gvkey', 'isin', 'csho', 'adj_pc', 'pt', 'date', 'curr']].groupby(
        ['naics', 'gvkey']).apply(correct_group_stocks).reset_index(drop=True)

    # print(result.reset_index(drop=True)[['csho', 'adj_pc', 'pt', 'date', 'curr', 'return']])

    entete.remove('csho')

    # Merge Result to the DataFrame
    monthly_prices = monthly_prices[entete]
    monthly_prices = pd.merge(monthly_prices,
                              result[
                                  ['date', 'isin', 'gvkey', 'naics', 'csho', 'adj_pc', 'return', 'pt_return', 'curr']],
                              on=['date', 'isin', 'gvkey', 'naics', 'curr'])
    print(monthly_prices.head())

    return monthly_prices


def get_monthly_sectors_summary(parameter) -> pd.DataFrame:
    """
        This function takes the summary informations from the stocks and compute the summary for the sectors for each
        NAICS and eco zone.

        :parameter

        monthly_prices (pd.Dataframe): Dataframe containing the information from the stocks from a given eco zone.

        :return

        sector_prices (pd.Dataframe): pd.DataFrame containing the summary information for all the NAICS in a given eco
        zone

    """

    data = parameter.data_table
    value = parameter.value

    data['date'] = pd.DatetimeIndex(data['date'])
    data[['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr']] = data[
        ['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr']].astype(str)
    data[['USDtocurr', 'adj_factor', 'pc', 'ph', 'pl', 'vol', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar', 'nptvar', 'rc',
          'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return', 'pt_return']] = data[
        ['USDtocurr', 'adj_factor', 'pc', 'ph', 'pl', 'vol', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar', 'nptvar', 'rc',
         'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return', 'pt_return']].astype(float)

    data.sort_values(by=['eco zone', 'naics', 'date'], ascending=[True, True, True], inplace=True)

    # Shift market capitalization to compute sector return
    data.loc[:, 'mc'] = data.loc[: 'pc'] * data.loc[: 'csho'] / data.loc[: 'USDtocurr']
    data['mc'] = data[['date', 'naics', 'isin', 'mc']].groupby(['naics', 'isin']).apply(shift_mc, 1)

    # Compute summary information
    result = data[['date', 'naics', 'eco zone', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar', 'nptvar',
                   'rc', 'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return', 'pt_return']
    ].groupby(['eco zone', 'naics', 'date']).apply(calculate_sectors_summary)

    # Compute country summary information
    _ = data[['date', 'eco zone', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar', 'nptvar',
              'rc', 'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return', 'pt_return']
    ].groupby(['eco zone', 'date']).apply(calculate_sectors_summary)

    result = pd.concat([result, _], ignore_index=True)

    'vol', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar',
    'nptvar', 'rc', 'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return',
    'pt_return'
    print(data.info())
    print(data.head(20))


if __name__ == "__main__":
    # GetListofEcoZoneAndNaics(ProdConnectionString)
    # AddStocksPerNaicsAndEcoZone()
    # filterStocksPerTradeStocksExchange()

    # stocks_by_sector = np.load('StocksBySectorWithLiquidExchg.npy')
    # stocks_by_sector = pd.DataFrame(stocks_by_sector, columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg'])
    # tabofMonth = GenerateMonthlyTab('1999-11', '2017-12')
    # tup = ()
    #
    # for month in tabofMonth:
    #     tup += monthly_stocks_price(value=month, data_table=stocks_by_sector),
    # pool = MyPool(4)
    # result = pool.map(SetSectorPriceToDB, tup)
    # pool.close()
    # pool.join()
    #
    # monthly_prices = pd.concat(result)
    # np.save('monthly_prices.npy', monthly_prices)

    # entete = ['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'USDtocurr','adj_factor', 'csho', 'date', 'pc', 'ph',
    #           'pl', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar', 'nptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']
    #
    # # monthly_prices.columns = entete
    # monthly_prices = np.load('test.npy')
    # monthly_prices = pd.DataFrame(monthly_prices, columns=entete)
    # # assign type to the columns
    # monthly_prices[[  'pt', 'npt', 'ptvar', 'nptvar',
    #                  'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']] = monthly_prices[[  'pt', 'npt', 'ptvar', 'nptvar',
    #                  'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']].astype(str)
    # monthly_prices.loc[monthly_prices['pt'] == 'None', 'pt'] = None
    # monthly_prices.loc[monthly_prices['pptvar'] == 'None', 'pptvar'] = None
    # monthly_prices.loc[monthly_prices['npptvar'] == 'None', 'npptvar'] = None
    # monthly_prices.loc[monthly_prices['rc'] == 'None', 'rc'] = None
    # monthly_prices.loc[monthly_prices['nrc'] == 'None', 'nrc'] = None
    # monthly_prices.loc[monthly_prices['rcvar'] == 'None', 'rcvar'] = None
    # monthly_prices.loc[monthly_prices['nrcvar'] == 'None', 'nrcvar'] = None
    #
    # monthly_prices['date'] = pd.DatetimeIndex(monthly_prices['date'])
    # monthly_prices['date'] = monthly_prices.date + pd.offsets.MonthEnd(0)
    # monthly_prices[['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr']] = monthly_prices[['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'curr']].astype(str)
    # monthly_prices[[ 'csho', 'vol', 'adj_factor', 'pc', 'ph', 'pl', 'USDtocurr', 'pt', 'npt', 'ptvar', 'nptvar',
    #                  'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']] = monthly_prices[[ 'csho', 'vol', 'adj_factor',
    #                 'pc', 'ph', 'pl', 'USDtocurr', 'pt', 'npt', 'ptvar', 'nptvar',
    #                  'pptvar', 'npptvar', 'rc', 'nrc', 'rcvar', 'nrcvar']].astype(float)
    #
    # print(monthly_prices.info())
    #
    # print('Download of Stocks Prices OK')
    #
    # # np.save('test', monthly_prices[monthly_prices['eco zone'] == 'USD'])
    #
    # chunk_size = 2500
    # records = []
    # frames = []
    # i = 0
    # df = pd.DataFrame(None, columns = ['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'USDtocurr', 'adj_factor', 'date',
    #                                    'pc', 'ph', 'pl', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar',
    #                                    'nptvar', 'rc', 'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return',
    #                                    'pt_return'])
    #
    # tab_of_gvkey = monthly_prices['gvkey'].unique()
    # print(len(tab_of_gvkey))
    # for i in range(0, len(tab_of_gvkey), 16*chunk_size):
    #
    #     chunk_gvkey = tab_of_gvkey[i:i + 16*chunk_size]
    #     tup = ()
    #
    #     for j in range(0, len(chunk_gvkey), chunk_size):
    #         tup += monthly_stocks_price(value='', data_table=monthly_prices[monthly_prices['gvkey'].isin(chunk_gvkey[j:j + chunk_size])]),
    #
    #     pool = MyPool(16)
    #     result = pool.map(patch_stocks_price, tup)
    #     pool.close()
    #     pool.join()
    #
    #     df = pd.concat([df] + result)
    #     print("Done for the gvkey at index [{0}, {1}]".format(i, i + 16*chunk_size))
    #     np.save('monthly_prices_adj_us.npy', df)
    #     print("Save for the gvkey at index [{0}, {1}]".format(i, i + 16*chunk_size))

    data = np.load('monthly_prices_adj_us.npy')
    data = pd.DataFrame(data, columns=['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'USDtocurr', 'adj_factor', 'date',
                                       'pc', 'ph', 'pl', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar',
                                       'nptvar', 'rc', 'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return',
                                       'pt_return'])

    params = monthly_stocks_price(value='USD', data_table=data)
    get_monthly_sectors_summary(params)

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
