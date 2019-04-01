__author__ = 'pougomg'

import wrds
import motor
import tornado
import collections
import pandas as pd
from datetime import date
from sqlalchemy import exc
from pymongo import InsertOne

from a_blackfire_capital_class.data_from_mongodb import DataFromMongoDB
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import STOCKS_MARKET_DATA_DB_NAME, \
    STOCKS_MARKET_DATA_INFOS_DB_COL_NAME

###################################################################################################################
#
# This script is used to download the stocks price data from wrds and save it to the mongo db.
#
###################################################################################################################

stocks_info_params = collections.namedtuple('stocks_info_params', ['table', 'library', 'global_wrds'])
stocks_price_params = collections.namedtuple('stocks_price_params', ['table', 'library', 'global_wrds', 'start_date',
                                                                     'end_date'])
CONNECTION_STRING = TEST_CONNECTION_STRING

###################################################################################################################
#
# Stocks price info zone.
#
###################################################################################################################


def download_stocks_price_info_from_compustat(stocks_price_info_parameter: collections) -> pd.DataFrame:

    """
    This function is used to download the prices information from COMPUSTAT.

    Parameter:
    ---------

    :type stocks_price_info_parameter: collections.
    :param stocks_price_info_parameter: collections containing the useful information to download the stocks price
    information.
    1. library: comp;
    2. table: ['g_security', 'g_names'] or ['security', 'names'];
    3. global_wrds: True/False True if global database and False otherwise.

    Return:
    ------

    :return: DataFrame of all the stocks info with the columns ['tic', 'gvkey', 'iid', 'cusip', 'dlrsni', 'dsci',
    'epf', 'exchg', 'excntry', 'ibtic', 'isin', 'secstat', 'sedol', 'tpci', 'dldtei', 'conm', 'sic', 'naics',
    'gsubind', 'gind', 'fic', 'global_wrds']

    :rtype pandas.DataFrame

    Raise error:
    -----------

    :raise ValueError if input parameter are wrong

    Source:
    ------

    1. wrds global security: https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95598
    2. wrds north america security: https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95757
    3. wrds global names: https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95563
    4. wrds north america names: https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95617

    Usage:
    -----

    parameter = stocks_info_params(library='comp', table=['security', 'names'], global_wrds=False)
    na_stocks_info = download_stocks_price_info_from_compustat(parameter)
    na_stocks_info.head()

         tic   gvkey iid      cusip     ...        gsubind    gind  fic  global_wrds
    0  AMFD.  001001  01  000165100     ...       25301040  253010  USA        False
    1   ANTQ  001003  01  000354100     ...       25504040  255040  USA        False
    2    AIR  001004  01  000361105     ...       20101010  201010  USA        False
    3   ABSI  001009  01  000781104     ...       15104020  151040  USA        False
    4  4165A  001010  01  00099V004     ...       20304010  203040  USA        False
    ...
    """

    if stocks_price_info_parameter.global_wrds:
        if stocks_price_info_parameter.library is not 'comp' or stocks_price_info_parameter.table is not ['g_security', 'g_names']:
            raise ValueError("The parameter for the Global data must be " +
                             "stocks_info_params(library='comp', table=['g_security', 'g_names'], global_wrds=True)")
    else:
        if stocks_price_info_parameter.library is not 'comp' or stocks_price_info_parameter.table is not ['security', 'names']:
            raise ValueError("The parameter for the North America data must be " +
                             "stocks_info_params(library='comp', table=['security', 'names'], global_wrds=False)")

    db = wrds.Connection()
    security = db.get_table(library=stocks_price_info_parameter.library, table=stocks_price_info_parameter.table[0])
    db.close()

    db = wrds.Connection()
    name = db.get_table(library=stocks_price_info_parameter.library, table=stocks_price_info_parameter.table[1])
    db.close()

    if stocks_price_info_parameter.global_wrds:

        header = ['gvkey', 'conm', 'fic', 'sic', 'naics', 'gsubind', 'gind']
        result = pd.merge(security, name[header], on=['gvkey'])

    else:

        header = ['gvkey', 'conm', 'sic', 'naics', 'gsubind', 'gind']
        result = pd.merge(security, name[header], on=['gvkey'])

        db = wrds.Connection()
        fic = db.raw_sql("select a.gvkey, a.fic  from comp.secd a group by a.gvkey, a.fic")
        db.close()

        result = pd.merge(result, fic, on=['gvkey'])
    result.loc[:, 'global_wrds'] = stocks_price_info_parameter.global_wrds

    return result


def concat_stocks_price_info():

    """
    This function is used to concat the North America and the Global stocks info.
    :return: Data Frame of all the stocks price info.
    """

    parameter = stocks_info_params(library='comp', table=['security', 'names'], global_wrds=False)
    na_stocks_info = download_stocks_price_info_from_compustat(parameter)

    parameter = stocks_info_params(library='comp', table=['g_security', 'g_names'], global_wrds=True)
    wld_stocks_info = download_stocks_price_info_from_compustat(parameter)

    result = pd.concat([na_stocks_info, wld_stocks_info[na_stocks_info.columns]], ignore_index=True)

    return result


def save_stocks_price_info_in_mongodb(stocks_price_info_df: pd.DataFrame):

    """
    This function is used to save the stocks price info to the mongo DB.
    :param stocks_price_info_df: Data Frame containing the stocks price information to save in the DB.
    :type pd.DataFrame.
    :return: None
    """

    def get_stocks_identification(ticker, ib_ticker, iid, cusip, exchg, exch_country, isin, secstat,
                                   sedol, tpci):
        if cusip is None and isin is not None:
            cusip = isin[2:11]
        if cusip is not None:
            cusip_8 = cusip[0:8]
        else:
            cusip_8 = None

        return {'ticker': ticker, 'ib_ticker': ib_ticker, 'iid': iid, 'cusip': cusip, 'exhg': exchg,
                'excntry': exch_country, 'isin': isin, 'secstat': secstat, 'sedol': sedol, 'tpci': tpci,
                'cusip_8': cusip_8}

    def group_stocks_price_info(group):

        """
        This function is used to group the stocks price info by GVKEY.
        :param group: Data Frame of stocks price info grouped by GVKEY.
        :return:
        """

        _id = group.name
        conm = group.loc[:, 'conm'].head(1).values[0]
        fic = group.loc[:, 'fic'].head(1).values[0]
        naics = group.loc[:, 'naics'].head(1).values[0]
        sic = group.loc[:, 'sic'].head(1).values[0]
        eco_zone = group.loc[:, 'eco zone'].head(1).values[0]

        return InsertOne({'_id': _id, 'company name': conm, 'incorporation location': fic, 'naics': naics,
                          'sic': sic, 'gic sector': None, 'gic ind': None, 'eco zone': eco_zone,
                          'stock identification': group['stock identification'].values.tolist()})

    # Create a dict for the stocks identification (ticker, ib_ticker, iid, cusip, exchg, exch_country, isin, secstat,
    # sedol, tpci) of each stock
    header = ['tic', 'ibtic', 'iid', 'cusip', 'exchg', 'excntry', 'isin', 'secstat', 'sedol', 'tpci']
    stocks_price_info_df.loc[:, 'stock identification'] = stocks_price_info_df.apply(lambda x: get_stocks_identification(*x[header]), axis=1)

    # TODO: Add eco zone
    stocks_price_info_df.loc[:, 'eco zone'] = None
    # Group all the stocks infos by GVKEY
    result = stocks_price_info_df.groupby('gvkey').apply(group_stocks_price_info).reset_index(drop=True)

    # Add stocks price info to mongo DB.
    client_db = motor.motor_tornado.MotorClient(CONNECTION_STRING)
    db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_DATA_INFOS_DB_COL_NAME]
    tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, result.values.tolist(), None).set_data_in_db)


###################################################################################################################
#
# Stocks price zone.
#
###################################################################################################################


def download_stocks_price_from_compustat(stocks_price_parameter: collections) -> pd.DataFrame:

    """
    This function is used to download the stocks price for the stocks in COMPUSTAT between two dates.

    Parameter:
    ---------

    :param stocks_price_parameter collections containing data to download all the useful information.
    These information are :

    1. global_wrds = True/False Indicates if we use the Global or the US database for the stocks.
    2. library: Library to get the stocks from comp
    3. table: Table name g_secd or secd
    4. start_date: start date of the stocks
    5. end_date

    Return:
    ------

    :return: DataFrame of all the stocks with the columns ['gvkey', 'datadate', 'ajexdi', 'cshoc', 'cshtrd', 'prccd',
     'prchd', 'prcld', 'curcdd', 'cusip', 'iid', 'exrat', 'global']
    :rtype pandas.DataFrame

    Raise error:
    -----------

    :raise ValueError if input parameter are wrong

    Source:
    ------

    1. wrds global: https://wrds-web.wharton.upenn.edu/wrds/ds/compd/g_secd/index.cfm?navId=73
    2. wrds north america: https://wrds-web.wharton.upenn.edu/wrds/ds/compd/secm/index.cfm?navId=83

    Usage:
    -----

    parameter = stocks_price_params(library='comp', table='secd', global_wrds=False,
                                    start_date=str(datetime.date(2018, 1, 31)),
                                    end_date=str(datetime.date(2018, 1, 31)))
    na_price = download_stocks_price_from_compustat(parameter)
    na_price.head()
        gvkey    datadate  ajexdi   ...    iid  exrat  global
    0  001004  2018-01-31     1.0   ...     01    1.0   False
    1  001019  2018-01-31     1.0   ...     01    1.0   False
    2  001021  2018-01-31     1.0   ...     01    1.0   False
    3  001045  2018-01-31     1.0   ...     04    1.0   False
    4  001050  2018-01-31     1.0   ...     01    1.0   False
    ...
    """
    # Make sure inout are in correct form or raise a ValueError.
    if stocks_price_parameter.global_wrds:
        if stocks_price_parameter.library is not 'comp' or stocks_price_parameter.table is not 'g_secd':
            raise ValueError("The parameter for the Global data must be " +
                             "stocks_price_params(library='comp', table='g_secd', global_wrds=True, " +
                             "start_date=str(start_date), end_date=str(end_date))")
    else:
        if stocks_price_parameter.library is not 'comp' or stocks_price_parameter.table is not 'secd':
            raise ValueError("The parameter for the North America data must be " +
                             "stocks_price_params(library='comp', table='secd', global_wrds=False, " +
                             "start_date=str(start_date), end_date=str(end_date)")

    # data to download
    if stocks_price_parameter.global_wrds:
        header = ['gvkey', 'datadate', 'ajexdi', 'cshoc', 'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'isin', 'iid']
    else:
        header = ['gvkey', 'datadate', 'ajexdi', 'cshoc', 'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'cusip', 'iid']
    result = pd.DataFrame(None, columns=header)

    # download data from compustat.
    try:

        header = ','.join(header)

        # SQL Statement to get Data
        sql_statement = "select pt.*, B.exrat " \
                        "FROM(select {header} FROM {schema}.{table}) As pt " \
                        "LEFT JOIN ibes.hdxrati B ON (pt.datadate = B.anndats AND pt.curcdd = B.curr) " \
                        "WHERE pt.datadate between '{start_date}' and '{end_date}'"

        sql_statement = sql_statement.format(header=header, schema=stocks_price_parameter.library, table=stocks_price_parameter.table,
                                             start_date=stocks_price_parameter.start_date, end_date=stocks_price_parameter.end_date)
        db = wrds.Connection()
        result = db.raw_sql(sql_statement)
        db.close()

        result = result[result['curcdd'].notnull()]
        result.dropna(subset=['curcdd'], inplace=True)
        result['global'] = stocks_price_parameter.global_wrds

    except exc.SQLAlchemyError as e:
        print(e.args[0])
    finally:
        db.close()

    result.rename(columns={'cusip': 'isin_or_cusip', 'isin': 'isin_or_cusip'}, inplace=True)
    result.dropna(subset=['isin_or_cusip'], inplace=True)
    result.dropna(subset=['prccd'], inplace=True)

    return result


def concat_stocks_price(start_date: date, end_date: date) -> pd.DataFrame:

    """
    This function is used to concat the stocks price from north america and global data bases from COMPUSTAT.

    :param start_date: start date of data to download
    :param end_date: end date of data to download

    :return: Data containing both global and noth americas data between start data and end date
    :rtype pd.DataFrame.
    """

    parameter = stocks_price_params(library='comp', table='secd', global_wrds=False, start_date=str(start_date),
                                    end_date=str(end_date))
    na_price = download_stocks_price_from_compustat(parameter)

    parameter = stocks_price_params(library='comp', table='g_secd', global_wrds=True, start_date=str(start_date),
                                    end_date=str(end_date))
    wld_price = download_stocks_price_from_compustat(parameter)

    return pd.concat([na_price, wld_price], ignore_index=True)


def save_stocks_price_in_mongodb(stocks_price_df: pd.DataFrame):

    """
    This function is used to save the price data in the mongo db
    :param stocks_price_df: data containing all the stocks price
    :return: None
    """

    def group_data_to_insert(isin_or_cusip, gvkey, date, curr, csho, vol, adj_factor, price_close,
                             price_high, price_low, iid, exrate, global_):
        """
        This function is used to construct all the infos to insert in the DB using MongoDB

        :param isin_or_cusip: isin or cusip of the stocks
        :param gvkey: gvkey of the stocks
        :param date: data of the price
        :param curr: currency of the stocks
        :param csho: common shares
        :param vol: volume trade of the stocks
        :param adj_factor: adjusted factor to compute the split of the stocks price
        :param price_close: price close at the end of the day
        :param price_high: high price for the daya
        :param price_low: low price for the day
        :param iid:
        :param exrate: exchange rate USD/curr for the day
        :param global_: Global/North America stocks

        :return: Data frame of pymongo.Insertone()
        """
        return InsertOne({'isin_or_cusip': isin_or_cusip, 'gvkey': gvkey, 'date': date, 'curr': curr, 'csho': csho,
                          'vol': vol, 'adj_factor': adj_factor, 'price_close': price_close, 'price_high': price_high,
                          'price_low': price_low, 'iid': iid, 'USD_to_curr': exrate, 'global': global_})

    def save_to_mongodb(group):
        """
        This function is used to save the stocks price into the mongo db
        :param group: group of stocks to save for a particular month
        :return: None
        """
        Date = group.name
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][str(Date)]
        tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, group.loc[:, 'to_save'].values.tolist()).set_data_in_db)

    stocks_price_df.loc[:, 'datadate'] = pd.to_datetime(stocks_price_df['datadate'])
    stocks_price_df.loc[:, 'date'] = stocks_price_df.loc[:, 'datadate'].dt.strftime('%Y-%m')
    client_db = motor.motor_tornado.MotorClient(CONNECTION_STRING)

    # set the insertOne for the bulkWrite
    header = ['isin_or_cusip', 'gvkey', 'datadate', 'curcdd', 'cshoc', 'cshtrd', 'ajexdi', 'prccd',
              'prchd', 'prcld', 'iid', 'exrat', 'global']
    stocks_price_df.loc[:, 'to_save'] = stocks_price_df.apply(lambda x: group_data_to_insert(*x[header]), axis=1)

    # save data in the mongo db
    stocks_price_df[['date', 'to_save']].groupby('date').apply(save_to_mongodb)


if __name__ == '__main__':

    ###################################################################################################################
    #
    # Download and save stocks price info in the mongo db from the COMPUSTAT DB.
    #
    ###################################################################################################################

    # data = concat_stocks_price_info()
    # save_stocks_price_info_in_mongodb(data)

    ###################################################################################################################
    #
    # Download and save stocks price in the mongo db from the COMPUSTAT DB between two dates.
    #
    ###################################################################################################################

    # data = concat_stocks_price(date(2018, 1, 31), date(2018, 1, 31))
    # save_stocks_price_in_mongodb(data)
    file = open('description templates.txt', 'w')
    file.write(download_stocks_price_from_compustat.__doc__)
