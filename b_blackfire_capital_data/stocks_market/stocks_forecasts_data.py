
import wrds
import motor
import tornado
import collections
import pandas as pd
import numpy as np
from datetime import date
from sqlalchemy import exc
from pymongo import InsertOne
from pandas.tseries.offsets import BDay
import itertools
import time
import multiprocessing
from typing import Callable, Tuple, Union

from a_blackfire_capital_class.data_from_mongodb import DataFromMongoDB
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import TYPE_CONSENSUS, TYPE_PRICE_TARGET, \
    STOCKS_MARKET_DATA_DB_NAME, STOCKS_MARKET_DATA_INFOS_DB_COL_NAME, STOCKS_MARKET_FORECASTS_INFOS_DB_COL_NAME, \
    CURRENCIES_EXCHANGE_RATES_DB_NAME, CURRENCIES_EXCHANGE_RATES_DB_COL_NAME, profile

###################################################################################################################
#
# This script is used to download the stocks forecasts data from IBES and save it to the mongo db.
#
###################################################################################################################
CONNECTION_STRING = TEST_CONNECTION_STRING
stocks_info_params = collections.namedtuple('stocks_info_params', ['type'])
stocks_forecasts_parameter = collections.namedtuple('stocks_forecasts_parameter', ['type', 'start_date', 'end_date'])
###################################################################################################################
#
# Stocks Forecasts info zone.
#
###################################################################################################################


def download_stocks_forecasts_info_from_ibes(parameter: collections) -> pd.DataFrame:

    """
    This function is used to download the prices forecasts information from IBES.

    Parameter:
    ---------

    :type parameter: collections.
    :param parameter: collections containing the useful information to download the stocks price information.
    1. type: [price_target, consensus]

    Return:
    ------

    :return: DataFrame of all the stocks info with the columns ['cusip', 'ticker']

    :rtype pandas.DataFrame

    Raise error:
    -----------

    :raise ValueError if input parameter are wrong

    Source:
    ------

    1. IBES recommendation: https://wrds-web.wharton.upenn.edu/wrds/ds/ibes/recddet/index.cfm?navId=232
    2. IBES price target: https://wrds-web.wharton.upenn.edu/wrds/ds/ibes/ptgdet/index.cfm?navId=223

    Usage:
    -----

    parameter = stocks_info_params(type=TYPE_CONSENSUS)
    stocks_info = download_stocks_forecasts_info_from_ibes(parameter)
    stocks_info.head()
           cusip ticker
    0  FJ664480   @Y66
    1  EF476400   @GI0
    2  AAB1Z2Y5   @5L1
    3  AAB16P77   @PJ1
    4  FCBQV6G6  @00GY
    5  80546810   SAWS
    6      None     AQ
    7  FMB06KJ2   @J3K
    8      None   @6LX
    9  FM606906   @YAE
    ...
    """

    info_df = pd.DataFrame(None, columns=['cusip', 'ticker'])

    if parameter.type == TYPE_CONSENSUS:
        query = "select a.cusip, a.ticker from ibes.recddet a group by a.cusip, a.ticker"
    elif parameter.type == TYPE_PRICE_TARGET:
        query = "select a.cusip, a.ticker from ibes.ptgdet a group by a.cusip, a.ticker"
    else:
        error = "Incorrect input argument. parameter.type must be {} or {}. Please refer to the documentation" \
                "of the function to have a better understanding."
        raise ValueError(error.format(TYPE_CONSENSUS, TYPE_PRICE_TARGET))

    db = wrds.Connection()
    try:
        info_df = db.raw_sql(query)
    except exc.SQLAlchemyError as e:
        print(e.args[0])
    finally:
        db.close()

    def remove_none_cusip(group):
        ticker = group.name
        if group.shape[0] == 1:
            return group
        return group[group['cusip'] != ticker]

    info_df.loc[info_df['cusip'].isna() == True, 'cusip'] = info_df['ticker']
    info_df = info_df.groupby('ticker').apply(remove_none_cusip).reset_index(drop=True)

    return info_df


def concat_stocks_forecast_info() -> pd.DataFrame:

    """
    This function is used to concat the forecast stocks info for all the IBES DB.
    :return: Data Frame of all the stocks forecasts info.
    """
    params = stocks_info_params(type=TYPE_CONSENSUS)
    cons = download_stocks_forecasts_info_from_ibes(params)

    params = stocks_info_params(type=TYPE_PRICE_TARGET)
    pt = download_stocks_forecasts_info_from_ibes(params)

    result = pd.concat([cons, pt[cons.columns]], ignore_index=True)
    result = result.drop_duplicates(result.columns)

    return result


def merge_stocks_forecast_info_with_gvkey(stocks_forecast_info_df: pd.DataFrame) -> pd.DataFrame:

    """
    This function is used to assign a gvkey to all the stocks forecasts info
    :param stocks_forecast_info_df: DataFrame with all the stocks forecasts info
    :type stocks_forecast_info_df: pd.DataFrame
    :return: pd.DataFrame with columns [cusip, ticker, gvkey]
    """

    def split_identification(group):

        value = group.loc[:, 'stock identification'].values.tolist()
        df = pd.DataFrame({'stock identification': value[0]})
        df = df['stock identification'].apply(pd.Series)
        df['gvkey'] = group.name

        return df[['gvkey', 'ibtic', 'cusip_8']]


    # Download stocks infos fron the mongo db
    client_db = motor.motor_tornado.MotorClient(CONNECTION_STRING)
    db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_DATA_INFOS_DB_COL_NAME]
    query = {}
    display = {'_id': 1, 'stock identification': 1}
    stocks_price_info = tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, query, display).get_data_from_db)

    # Unstack the stock identification from the result.
    stocks_price_info = stocks_price_info.groupby('_id').apply(split_identification).reset_index(drop=True)

    # Add Gvkey to the stocks forecast info.
    ib_tic_df = pd.merge(stocks_forecast_info_df, stocks_price_info[['gvkey', 'ibtic']],
                         left_on='ticker', right_on='ibtic')
    cusip_df = pd.merge(stocks_forecast_info_df, stocks_price_info[['gvkey', 'cusip_8']],
                        left_on='cusip', right_on='cusip_8')

    stocks_forecast_info_df = pd.concat([ib_tic_df[['cusip', 'ticker', 'gvkey']],
                                         cusip_df[['cusip', 'ticker', 'gvkey']]],
                                        ignore_index=True)

    stocks_forecast_info_df['gvkey'] = stocks_forecast_info_df.groupby('ticker')[['gvkey']].apply(lambda x: x.bfill().ffill())
    stocks_forecast_info_df = stocks_forecast_info_df.drop_duplicates(['ticker', 'cusip'])

    return stocks_forecast_info_df


def save_stocks_forecast_info_in_mongodb(stocks_forecast_info_df: pd.DataFrame):
    """
    This function is used to save stocks forecasts information in the mongo DB.
    :param stocks_forecast_info_df: DataFrame of stocks forecasts information containing the columns
    [gvkey, cusip, ticker]

    :type stocks_forecast_info_df: pd.DataFrame
    :raise ValueError when the column doesn't contain gvkey, cusip, ticker.
    :return: none
    """

    def group_stocks_forecast_info(gvkey, cusip, ticker):

        return InsertOne({'gvkey': gvkey, 'cusip_8': cusip, 'ibtic': ticker, '_id': ticker + '_' + cusip})

    stocks_forecast_info_df.loc[:, 'to_save'] = stocks_forecast_info_df.\
        apply(lambda x: group_stocks_forecast_info(*x[['gvkey', 'cusip', 'ticker']]), axis=1)

    # Add stocks forecast info to mongo DB.
    client_db = motor.motor_tornado.MotorClient(CONNECTION_STRING)
    db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_FORECASTS_INFOS_DB_COL_NAME]
    tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db,
                                                             stocks_forecast_info_df.loc[:, 'to_save'].values.tolist(),
                                                             None).set_data_in_db)


###################################################################################################################
#
# Stocks Forecasts Data zone.
#
###################################################################################################################

def download_stocks_forecasts_from_ibes(stocks_forecasts_parameter: collections) -> pd.DataFrame:

    """
    Description:
    ------------

    This function is used to download the stocks forecasts information from the market analysts. The information
    could be price target, recommendations or fundamentals forecasts for a certain horizon of time. We use the IBES
    DB to download the information from the table ptgdet (price target), recddet (consensus).

    Parameter:
    ----------

    :param stocks_forecasts_parameter: collections containing the type of data we want to download within a period.
     The collections parameters are:
    1. type: PRICE_TARGET or CONSENSUS;
    2. start_date
    3. end_date

    :type stocks_forecasts_parameter: collections

    Return:
    -------

    :return: pd.DataFrame containing the forecasts information
    :rtype: pd.DataFrame

    Raise error:
    ------------

    :raise ValueError if input parameter are wrong

    Source:
    -------

    1. IBES recommendation: https://wrds-web.wharton.upenn.edu/wrds/ds/ibes/recddet/index.cfm?navId=232
    2. IBES price target: https://wrds-web.wharton.upenn.edu/wrds/ds/ibes/ptgdet/index.cfm?navId=223

    Usage:
    ------
    stocks_forecasts_parameter = stocks_forecasts_parameter(type=TYPE_PRICE_TARGET, start_date=date(2019, 1, 1),
                                                            end_date=date(2019, 4, 1))
    data = download_stocks_forecasts_from_ibes(stocks_forecasts_parameter)
    data.head(10)

            ibtic   cusip_8   company      ...          anndats  analyst       measure
        0  0001  26878510  PRMDN066      ...       2019-01-04   545708  price_target
        1  0001  26878510  PRMDN016      ...       2019-01-07   545356  price_target
        2  0001  26878510  PRMDN082      ...       2019-01-07   534751  price_target
        3  0001  26878510  PRMDN058      ...       2019-01-30   539762  price_target
        4  0001  26878510  PRMDN010      ...       2019-02-12   538610  price_target
        5  000R  14163310      BTIG      ...       2019-01-18   191423  price_target
        6  000R  14163310   CRUTTEN      ...       2019-02-14   114029  price_target
        7  000V  28249U10  PRMDN010      ...       2019-01-28   560487  price_target
        8  000Y  90400D10     PIPER      ...       2019-01-18    79991  price_target
        9  000Y  90400D10  PRMDN070      ...       2019-01-18   554900  price_target

    """

    # Check if input is correct or raise a ValueError
    if stocks_forecasts_parameter.type == TYPE_CONSENSUS:

        header = ['ticker', 'cusip', 'emaskcd', 'ireccd', 'anndats', 'amaskcd']
        sqlstmt = "select " + ",".join(header) + " From {schema}.{table} WHERE anndats between " \
                                                 "'{start_date}' and '{end_date}'"
        sqlstmt = sqlstmt.format(schema='ibes', table='recddet',
                                 start_date=stocks_forecasts_parameter.start_date,
                                 end_date=stocks_forecasts_parameter.end_date)

    elif stocks_forecasts_parameter.type == TYPE_PRICE_TARGET:

        header = ['ticker', 'cusip', 'estimid', 'horizon', 'value', 'estcur', 'anndats', 'amaskcd']
        sqlstmt = "select " + ",".join(header) + " From {schema}.{table} WHERE anndats between " \
                                                 "'{start_date}' and '{end_date}'"
        sqlstmt = sqlstmt.format(schema='ibes', table='ptgdet',
                                 start_date=stocks_forecasts_parameter.start_date,
                                 end_date=stocks_forecasts_parameter.end_date)
    else:
        error = "Incorrect input argument. parameter.type must be {} or {}. Please refer to the documentation" \
                "of the function to have a better understanding."
        raise ValueError(error.format(TYPE_CONSENSUS, TYPE_PRICE_TARGET))

    db = wrds.Connection()
    stocks_forecasts_df = pd.DataFrame(None, columns=['measure', 'anndats', 'ibtic', 'cusip_8', 'analyst',
                                                      'analyst_company', 'horizon', 'value', 'estcur'])
    try:
        # Download data from the ibes db
        stocks_forecasts_df = db.raw_sql(sqlstmt)

    except exc.SQLAlchemyError as e:
        print("Error Loading File for : ", stocks_forecasts_parameter.type)
    finally:
        db.close()

    # Reformat header
    header = {'ticker': 'ibtic', 'cusip': 'cusip_8', 'emaskcd': 'analyst_company', 'ireccd': 'value',
              'amaskcd': 'analyst', 'estimid': 'analyst_company', 'estcur': 'curr'}

    stocks_forecasts_df.rename(columns=header, inplace=True)
    stocks_forecasts_df['measure'] = stocks_forecasts_parameter.type

    if stocks_forecasts_parameter.type == TYPE_CONSENSUS:
        stocks_forecasts_df.loc[:, 'horizon'] = 6
        stocks_forecasts_df.loc[:, 'curr'] = None

    stocks_forecasts_df.loc[stocks_forecasts_df['analyst'].isna(), 'analyst'] = \
        stocks_forecasts_df.loc[:, 'analyst_company']

    return stocks_forecasts_df[['measure', 'anndats', 'ibtic', 'cusip_8', 'analyst', 'analyst_company', 'horizon',
                                'value', 'curr']]


def add_exchange_rates_to_stocks_forecasts(stocks_forecasts_df: pd.DataFrame) -> pd.DataFrame:

    """
    Description:
    ------------

    This function is used to add the exchange rates USD/curr to the value in the stocks_forecasts_df value

    Parameter:
    ----------

    :param stocks_forecasts_df: DataFrame with the stocks_forecasts values
    :type stocks_forecasts_df: pd.DataFrame

    Return:
    -------

    :return: stocks_forecasts_df with one more column usd_to_curr
    :rtype pd.DataFrame

    Usage:
    ------
    df = add_exchange_rates_to_stocks_forecasts(stocks_forecasts_df)


    """

    # Convert end of the week day to the next monday
    stocks_forecasts_df['anndats'] = pd.to_datetime(stocks_forecasts_df['anndats'])
    stocks_forecasts_df['anndats'] = stocks_forecasts_df['anndats'] - 1 * pd.offsets.BDay(0)

    # Get exchange rates TODO: add date filter!
    query = {}
    to_display = {'_id': 0}
    client_db = motor.motor_tornado.MotorClient(CONNECTION_STRING)
    db = client_db[CURRENCIES_EXCHANGE_RATES_DB_NAME][CURRENCIES_EXCHANGE_RATES_DB_COL_NAME]
    exchange_rates_df = tornado.ioloop.IOLoop.current().run_sync(
        DataFromMongoDB(db, query, to_display).get_data_from_db)
    exchange_rates_df['anndats'] = pd.to_datetime(exchange_rates_df["date"].dt.strftime('%Y-%m-%d'))
    exchange_rates_df.rename(columns={'to': 'curr'}, inplace=True)

    # Merge stocks_forecasts_df with the exchange rates
    stocks_forecasts_df = pd.merge(stocks_forecasts_df,
                                   exchange_rates_df[['curr', 'anndats', 'rate']],
                                   'left',
                                   on=['curr', 'anndats'])

    return stocks_forecasts_df


def calculate_variation_(name, group):

    group.sort_values(['anndats'], inplace=True)
    if group['curr'].nunique() > 1:
        print(name)
        print(group['curr'])
        print('')
    # group.loc[:, 'sample_anndats'] = group.loc[:, 'anndats']
    # group.set_index('sample_anndats', inplace=True)
    # group = group.resample('1M').ffill(limit=6)
    # group['var'] = group[['value']].pct_change(periods=1, freq='M')
    # group = group.drop_duplicates(['anndats'], keep='first')
    # group = group.resample('1M').ffill(limit=6)
    #
    # return group

@profile
def calculate_stocks_forecasts_variation(stocks_forecasts_df: pd.DataFrame) -> pd.DataFrame:

    def calculate_variation(group):

        group.sort_values(['anndats'], inplace=True)
        # group.loc[:, 'sample_anndats'] = group.loc[:, 'anndats']
        # group.set_index('sample_anndats', inplace=True)
        group = group.resample('1M').ffill(limit=6)
        # group['var'] = group[['value']].pct_change(periods=1, freq='M')
        # group = group.drop_duplicates(['anndats'], keep='first').reset_index()
        return group

    def groupby_parallel(groupby_df: pd.core.groupby.DataFrameGroupBy,
                         func: Callable[[Tuple[str, pd.DataFrame]], Union[pd.DataFrame, pd.Series]],
                         num_cpus: int = multiprocessing.cpu_count() - 1,
                         logger: Callable[[str], None] = print) -> pd.DataFrame:
        """
        Performs a Pandas groupby operation in parallel.
        Example usage:
            import pandas as pd
            df = pd.DataFrame({'A': [0, 1], 'B': [100, 200]})
            df.groupby(df.groupby('A'), lambda row: row['B'].sum())
        Authors: Tamas Nagy and Douglas Myers-Turnbull
        """
        start = time.time()
        logger("\nUsing {} CPUs in parallel...".format(num_cpus))

        with multiprocessing.Pool(num_cpus) as pool:
            queue = multiprocessing.Manager().Queue()
            result = pool.starmap_async(func, [(name, group) for name, group in groupby_df])
            cycler = itertools.cycle('\|/―')
            while not result.ready():
                logger("Percent complete: {:.0%} {}".format(100 * queue.qsize() / len(groupby_df), next(cycler)),
                       end="\r")
                time.sleep(1)
            got = result.get()
        logger("\nProcessed {} rows in {:.1f}s".format(len(got), time.time() - start))
        return pd.concat(got)

    # Convert end of the week day to the next monday
    stocks_forecasts_df['anndats'] = pd.to_datetime(stocks_forecasts_df['anndats'])
    stocks_forecasts_df['anndats'] = stocks_forecasts_df['anndats'] - 1 * pd.offsets.BDay(0)

    stocks_forecasts_df.sort_values(['ibtic', 'analyst', 'anndats', 'value'], inplace=True)
    stocks_forecasts_df.loc[:, 'key'] = stocks_forecasts_df['ibtic'] + stocks_forecasts_df['analyst'].astype(str)
    stocks_forecasts_df.drop_duplicates(['anndats', 'key'], keep='last', inplace=True)
    stocks_forecasts_df.loc[:, 'sample_anndats'] = stocks_forecasts_df.loc[:, 'anndats']
    stocks_forecasts_df.set_index('sample_anndats', inplace=True)

    stocks_forecasts_df = groupby_parallel(stocks_forecasts_df.groupby('key'), calculate_variation_)
    print(stocks_forecasts_df)
    return
    stocks_forecasts_df = stocks_forecasts_df.groupby(stocks_forecasts_df['key']).\
        resample('1M').ffill(limit=6).reset_index(level=1).\
        reset_index(drop=True).set_index('sample_anndats')

    print(stocks_forecasts_df.head(15))
    stocks_forecasts_df['variation'] = stocks_forecasts_df[['value']].groupby(stocks_forecasts_df['key']).\
        pct_change(periods=1, freq='M')

    return

    arr_slice = stocks_forecasts_df[['ibtic', 'analyst']].values
    # print(arr_slice)
    # lidx = np.ravel_multi_index(arr_slice.T, arr_slice.max(0) + 1)
    # unq, unqtags, counts = np.unique(lidx, return_inverse=True, return_counts=True)

    # return
    # print(stocks_forecasts_df.head(15))
    dd5 = dd.from_pandas(stocks_forecasts_df, npartitions=16)
    cols = list(stocks_forecasts_df.columns) + ['var']
    print(cols)
    meta = {'measure': 'str', 'anndats': 'datetime64[ns]', 'ibtic': 'str', 'cusip_8': 'str', 'analyst': 'str',
            'analyst_company': 'str', 'horizon': 'int', 'value': 'float', 'curr': 'str', 'var': 'float',
            'sample_anndats': 'datetime64[ns]', 'key': 'str'}
    dd5.groupby(['key']).apply(calculate_variation, meta=meta).compute()
    # stocks_forecasts_df = stocks_forecasts_df.groupby(['ibtic', 'analyst']).apply(calculate_variation,
    #                                                                               columns=cols).reset_index(drop=True)
    return
    stocks_forecasts_df.loc[:, 'sample_anndats'] = stocks_forecasts_df.loc[:, 'anndats']
    stocks_forecasts_df.set_index('sample_anndats', inplace=True)

    stocks_forecasts_df = stocks_forecasts_df.groupby(['ibtic', 'analyst']).\
        resample('1M').ffill(limit=6)[['value']].pct_change(periods=1, freq='M').reset_index(level=2).reset_index(drop=True).set_index('sample_anndats')

    stocks_forecasts_df['var'] = stocks_forecasts_df[['value']].pct_change(periods=1)
    print(stocks_forecasts_df[['ibtic', 'analyst', 'anndats', 'value', 'var']].head(15))

    stocks_forecasts_df = stocks_forecasts_df.drop_duplicates(['ibtic', 'analyst', 'anndats'], keep='first')

    # print(stocks_forecasts_df[['ibtic', 'analyst', 'anndats', 'value', 'var']].head(15))
    # stocks_forecasts_df.groupby(['ibtic', 'analyst']).rolling('m')


from dask import dataframe as dd

if __name__ == '__main__':

    ###################################################################################################################
    #
    # Download and save stocks forecasts info in the mongo db from the IBES DB.
    #
    ###################################################################################################################
    # data = concat_stocks_forecast_info()
    # merge_stocks_forecast_info_with_gvkey(data)
    # print(download_stocks_forecasts_info_from_ibes.__doc__)
    # print(DataFromMongoDB().__doc__)
    stocks_forecasts_parameter = stocks_forecasts_parameter(type=TYPE_PRICE_TARGET, start_date=date(1998, 1, 1),
                                                            end_date=date(2015, 12, 31))

    data = download_stocks_forecasts_from_ibes(stocks_forecasts_parameter)
    calculate_stocks_forecasts_variation(data)
    # add_exchange_rates_to_stocks_forecasts(data)
