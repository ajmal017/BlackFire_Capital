import sys
import wrds
import time
import motor
import tornado
import collections
import numpy as np
import pandas as pd
from datetime import date
from sqlalchemy import exc
from typing import Callable
from pymongo import InsertOne

from a_blackfire_capital_class.data_from_mongodb import DataFromMongoDB
from a_blackfire_capital_class.useful_class import CustomMultiprocessing, SendSimulationState
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING, PROD_CONNECTION_STRING
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import STOCKS_MARKET_DATA_DB_NAME, \
    STOCKS_MARKET_DATA_INFO_DB_COL_NAME, STOCKS_MARKET_FORECASTS_INFO_DB_COL_NAME, TYPE_PRICE_TARGET, TYPE_CONSENSUS, \
    SUMMARY_DB_COL_NAME, M_SUMMARY_DB_COL_NAME, NAICS

####################################################################################################################
#
# Collections to use in the class
#
#####################################################################################################################

CONNECTION_STRING = PROD_CONNECTION_STRING

# Stocks price collections
stocks_price_info_params = collections.namedtuple('stocks__price_info_params', ['table', 'library', 'global_wrds'])
stocks_price_params = collections.namedtuple('stocks_price_params', ['table', 'library', 'global_wrds', 'start_date',
                                                                     'end_date'])
# Stocks forecasts collections
stocks_forecast_info_params = collections.namedtuple('stocks_forecast_info_params', ['type'])
stocks_forecasts_params = collections.namedtuple('stocks_forecasts_params', ['type', 'start_date', 'end_date'])


class Stocks:

    """
    Description:
    ------------

    This class is used to get and to save all the type of information related to the stocks. The information are
    divided in 3 categories:

    1. The stocks price information which contain daily price close, price high, price low, volume, common shares,
    currency, exchange rates with USD.

    2. The fundamentals information which contains all the useful ratios for the firm evaluation.

    3. The forecasts information which contain the information about the analyst recommendations, the price target and
    the forecasts of the fundamentals of the firms.

    To get those information we refer to WRDS database.

    The class is divided in 3 main methods:

    1. download method is used to download the data from the source DB (ex: COMPUSTAT, IBES)

    2. Save method is used to save the download data into the Mongo DB.

    3. Get methods is used to get the Data we store in the Mongo DB.

    """

    def __init__(self, *connection_string):
        self._connection_string = connection_string

    ###################################################################################################################
    #
    # Stocks price info zone.
    #
    ###################################################################################################################

    @staticmethod
    def _download_stocks_price_info_from_compustat(stocks_price_info_parameter: collections) -> pd.DataFrame:

        """
        Description:
        ------------

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

        1. wrds global security:
         https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95598
        2. wrds north america security:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95757
        3. wrds global names:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95563
        4. wrds north america names:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95617

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
            if stocks_price_info_parameter.library != 'comp' or \
                    stocks_price_info_parameter.table != ['g_security', 'g_names']:
                raise ValueError("The parameter for the Global data must be " +
                                 "stocks_info_params(library='comp', table=['g_security', 'g_names'], "
                                 "global_wrds=True)")
        else:
            if stocks_price_info_parameter.library != 'comp' or \
                    stocks_price_info_parameter.table != ['security', 'names']:
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

    def download_stocks_price_info_from_compustat(self) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to download the North America and the Global stocks info.

        Return:
        ------

        :return: DataFrame of all the stocks info with the columns ['tic', 'gvkey', 'iid', 'cusip', 'dlrsni', 'dsci',
        'epf', 'exchg', 'excntry', 'ibtic', 'isin', 'secstat', 'sedol', 'tpci', 'dldtei', 'conm', 'sic', 'naics',
        'gsubind', 'gind', 'fic', 'global_wrds']

        :rtype pandas.DataFrame

        Source:
        ------

        1. wrds global security:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95598
        2. wrds north america security:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95757
        3. wrds global names:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95563
        4. wrds north america names:
        https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=162&file_id=95617

        Usage:
        -----

        stocks_info = Stocks().download_stocks_price_info_from_compustat()
        stocks_info.head()

             tic   gvkey iid      cusip     ...        gsubind    gind  fic  global_wrds
        0  AMFD.  001001  01  000165100     ...       25301040  253010  USA        False
        1   ANTQ  001003  01  000354100     ...       25504040  255040  USA        False
        2    AIR  001004  01  000361105     ...       20101010  201010  USA        False
        3   ABSI  001009  01  000781104     ...       15104020  151040  USA        False
        4  4165A  001010  01  00099V004     ...       20304010  203040  USA        False
        ...

        """
        parameter = stocks_price_info_params(library='comp', table=['security', 'names'], global_wrds=False)
        na_stocks_info = self._download_stocks_price_info_from_compustat(parameter)

        parameter = stocks_price_info_params(library='comp', table=['g_security', 'g_names'], global_wrds=True)
        wld_stocks_info = self._download_stocks_price_info_from_compustat(parameter)

        result = pd.concat([na_stocks_info, wld_stocks_info[na_stocks_info.columns]], ignore_index=True)

        return result

    def _save_stocks_price_info_in_mongodb(self, stocks_price_info_df: pd.DataFrame):

        """
        Description:
        ------------
        This function is used to save the stocks price info to the mongo DB.

        Parameter:
        ---------
        :param stocks_price_info_df: Data Frame containing the stocks price information to save in the DB.
        :type stocks_price_info_df: pd.DataFrame.

        Return:
        -------

        :return: None

        Usage:
        ------
        Stocks()._save_stocks_price_info_in_mongodb()
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

        # Create a dict for the stocks identification (ticker, ib_ticker, iid, cusip, exchg, exch_country, isin,
        # secstat, sedol, tpci) of each stock
        header = ['tic', 'ibtic', 'iid', 'cusip', 'exchg', 'excntry', 'isin', 'secstat', 'sedol', 'tpci']
        stocks_price_info_df.loc[:, 'stock identification'] = stocks_price_info_df.apply(
            lambda x: get_stocks_identification(*x[header]), axis=1)

        # TODO: Add eco zone
        stocks_price_info_df.loc[:, 'eco zone'] = None
        # Group all the stocks infos by GVKEY
        result = stocks_price_info_df.groupby('gvkey').apply(group_stocks_price_info).reset_index(drop=True)

        # Add stocks price info to mongo DB.
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_DATA_INFO_DB_COL_NAME]
        tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, result.values.tolist(), None).set_data_in_db)

    def save_stocks_price_info_in_mongodb(self):
        """
        Description:
        ------------

        This function is used to download and save the stocks price info in the mongo DB.
        The function calls first the method download_stocks_price_info_from_compustat() to get
        all the stocks price info and perform the saving in the mongo DB.

        Usage
        -----

        Stocks().save_stocks_price_info_in_mongodb()

        :return: None
        """
        stocks_price_info_df = self.download_stocks_price_info_from_compustat()
        self._save_stocks_price_info_in_mongodb(stocks_price_info_df)

    def get_stocks_price_info_from_mongodb(self, query: dict(), to_display: dict()) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to stocks price info data from the mongo DB.

        Parameter:
        ----------

        :param query: query to perform in the DB
        :param to_display: data to display in the returned data

        Type:
        -----

        :type query: dict()
        :type to_display: dict()

        Source:
        -------
        cloud mongo db.

        Usage:
        ------
        query = {}
        display = {'_id': 1, 'stock identification': 1}
        stocks_info = Stocks(CONNECTION_STRING).get_stocks_price_info_from_mongodb(query, display)
        stocks_info.head(10)

               gvkey                               stock identification
        0     001661  [{'ticker': None, 'ibtic': '@66K', 'iid': '01W...
        1     004312  [{'ticker': None, 'ibtic': None, 'iid': '02W',...
        2     002721  [{'ticker': None, 'ibtic': '@CAN', 'iid': '01W...
        3     002000  [{'ticker': None, 'ibtic': '@BCE', 'iid': '01W...
        4     005180  [{'ticker': None, 'ibtic': '@GLX', 'iid': '02W...
        5     006972  [{'ticker': None, 'ibtic': '@MWE', 'iid': '01W...
        6     008594  [{'ticker': None, 'ibtic': '@PIO', 'iid': '01W...
        7     009098  [{'ticker': None, 'ibtic': '@TFO', 'iid': '01W...
        8     001855  [{'ticker': None, 'ibtic': None, 'iid': '01W',...
        9     007652  [{'ticker': None, 'ibtic': '@NEC', 'iid': '01W...
        ...

        Return:
        -------

        :return: stocks info dataFrame
        :rtype: pd.DataFrame
        """
        # Download stocks infos fron the mongo db
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_DATA_INFO_DB_COL_NAME]
        stocks_price_info = tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, query, to_display).get_data_from_db)
        stocks_price_info.rename(columns={'_id': 'gvkey'}, inplace=True)

        return stocks_price_info

    ###################################################################################################################
    #
    # Stocks price zone.
    #
    ###################################################################################################################

    @staticmethod
    def _download_stocks_price_from_compustat(stocks_price_parameter: collections) -> pd.DataFrame:

        """
        Description:
        ------------

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

            sql_statement = sql_statement.format(header=header, schema=stocks_price_parameter.library,
                                                 table=stocks_price_parameter.table,
                                                 start_date=stocks_price_parameter.start_date,
                                                 end_date=stocks_price_parameter.end_date)
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

    def download_stocks_price_from_compustat(self, start_date: date, end_date: date) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to download the stocks price from north america and global data bases from COMPUSTAT.

        Parameter:
        ----------

        :param start_date: start date of data to download
        :param end_date: end date of data to download

        :type start_date: date
        :type end_date: date

        Return:
        ------

        :return: DataFrame of all the stocks with the columns ['gvkey', 'datadate', 'ajexdi', 'cshoc', 'cshtrd', 'prccd',
         'prchd', 'prcld', 'curcdd', 'cusip', 'iid', 'exrat', 'global']
        :rtype pandas.DataFrame

        Source:
        ------

        1. wrds global: https://wrds-web.wharton.upenn.edu/wrds/ds/compd/g_secd/index.cfm?navId=73
        2. wrds north america: https://wrds-web.wharton.upenn.edu/wrds/ds/compd/secm/index.cfm?navId=83

        Usage:
        -----

        price = Stocks().download_stocks_price_from_compustat()
        price.head()
            gvkey    datadate  ajexdi   ...    iid  exrat  global
        0  001004  2018-01-31     1.0   ...     01    1.0   False
        1  001019  2018-01-31     1.0   ...     01    1.0   False
        2  001021  2018-01-31     1.0   ...     01    1.0   False
        3  001045  2018-01-31     1.0   ...     04    1.0   False
        4  001050  2018-01-31     1.0   ...     01    1.0   False
        ...
        """

        parameter = stocks_price_params(library='comp', table='secd', global_wrds=False, start_date=str(start_date),
                                        end_date=str(end_date))
        na_price = self._download_stocks_price_from_compustat(parameter)

        parameter = stocks_price_params(library='comp', table='g_secd', global_wrds=True, start_date=str(start_date),
                                        end_date=str(end_date))
        wld_price = self._download_stocks_price_from_compustat(parameter)

        return pd.concat([na_price, wld_price], ignore_index=True)

    def _save_stocks_price_in_mongodb(self, stocks_price_df: pd.DataFrame):

        """
        Description:
        ------------

        This function is used to save the price data in the mongo db

        Parameter:
        ----------

        :param stocks_price_df: data containing all the stocks price
        :type stocks_price_df: pd.DataFrame

        Return:
        -------

        :return: None

        Usage:
        ------

        Stocks(self._connection_string)._save_stocks_price_in_mongodb()
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
                              'vol': vol, 'adj_factor': adj_factor, 'price_close': price_close,
                              'price_high': price_high,
                              'price_low': price_low, 'iid': iid, 'USD_to_curr': exrate, 'global': global_})

        def save_to_mongodb(group):
            """
            This function is used to save the stocks price into the mongo db
            :param group: group of stocks to save for a particular month
            :return: None
            """
            Date = group.name
            db = client_db[STOCKS_MARKET_DATA_DB_NAME][SUMMARY_DB_COL_NAME][str(Date)]
            tornado.ioloop.IOLoop.current().run_sync(
                DataFromMongoDB(db, group.loc[:, 'to_save'].values.tolist()).set_data_in_db)

        stocks_price_df.loc[:, 'datadate'] = pd.to_datetime(stocks_price_df['datadate'])
        stocks_price_df.loc[:, 'date'] = stocks_price_df.loc[:, 'datadate'].dt.strftime('%Y-%m')
        client_db = motor.motor_tornado.MotorClient(self._connection_string)

        # set the insertOne for the bulkWrite
        header = ['isin_or_cusip', 'gvkey', 'datadate', 'curcdd', 'cshoc', 'cshtrd', 'ajexdi', 'prccd',
                  'prchd', 'prcld', 'iid', 'exrat', 'global']
        stocks_price_df.loc[:, 'to_save'] = stocks_price_df.apply(lambda x: group_data_to_insert(*x[header]), axis=1)

        # save data in the mongo db
        stocks_price_df[['date', 'to_save']].groupby('date').apply(save_to_mongodb)

    def save_stocks_price_in_mongodb(self, start_date: date, end_date: date):

        """
        Description:
        ------------

        This function is used to download and save the stocks price data in the Mongo DB. This function calls
        the methods download_stocks_price_from_compustat(start_date, end_date) to download the data, then save
        the result in the mongo DB.

        Parameter:
        ---------

        :param start_date
        :param end_date

        Type:
        -----

        :type start_date: date
        :type end_date: date

        Usage:
        ------

        Stocks(self._connection_string).save_stocks_price_in_mongodb(date(2018, 1,31), date(2018, 3,31))

        Return:
        -------

        :return: None
        """
        stocks_price_df = self.download_stocks_price_from_compustat(start_date, end_date)
        self._save_stocks_price_in_mongodb(stocks_price_df)

    # TODO: implement get daily stocks price.
    def get_daily_stocks_price_from_mongodb(self, my_date: date, query: dict, to_display: dict):

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][SUMMARY_DB_COL_NAME][str(my_date)]
        _ = tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, query, to_display).get_data_from_db)

        return _

    ###################################################################################################################
    #
    # Stocks Forecasts info zone.
    #
    ###################################################################################################################

    @staticmethod
    def _download_stocks_forecasts_info_from_ibes(parameter: collections) -> pd.DataFrame:

        """
        Description:
        ------------

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

    def download_stocks_forecasts_info_from_ibes(self) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is download the forecast stocks info for all the IBES DB.

        Return:
        ------

        :return: DataFrame of all the stocks info with the columns ['cusip', 'ticker']

        :rtype pandas.DataFrame


        Source:
        ------

        1. IBES recommendation: https://wrds-web.wharton.upenn.edu/wrds/ds/ibes/recddet/index.cfm?navId=232
        2. IBES price target: https://wrds-web.wharton.upenn.edu/wrds/ds/ibes/ptgdet/index.cfm?navId=223

        Usage:
        -----

        stocks_info = Stocks().download_stocks_forecasts_info_from_ibes()
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
        params = stocks_forecast_info_params(type=TYPE_CONSENSUS)
        cons = self._download_stocks_forecasts_info_from_ibes(params)

        params = stocks_forecast_info_params(type=TYPE_PRICE_TARGET)
        pt = self._download_stocks_forecasts_info_from_ibes(params)

        result = pd.concat([cons, pt[cons.columns]], ignore_index=True)
        result = result.drop_duplicates(result.columns)

        return result

    def _merge_stocks_forecast_info_with_gvkey(self, stocks_forecast_info_df: pd.DataFrame) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to assign a gvkey to all the stocks forecasts info

        Parameter:
        ----------

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

        query = {}
        to_display = {'_id': 1, 'stock identification': 1}
        stocks_price_info = self.get_stocks_price_info_from_mongodb(query, to_display)

        # Unstack the stock identification from the result.
        stocks_price_info = stocks_price_info.groupby('gvkey').apply(split_identification).reset_index(drop=True)

        # Add Gvkey to the stocks forecast info. TODO: Upgrade by adding isin and cusip.
        ib_tic_df = pd.merge(stocks_forecast_info_df, stocks_price_info[['gvkey', 'ibtic']],
                             left_on='ticker', right_on='ibtic')
        cusip_df = pd.merge(stocks_forecast_info_df, stocks_price_info[['gvkey', 'cusip_8']],
                            left_on='cusip', right_on='cusip_8')

        stocks_forecast_info_df = pd.concat([ib_tic_df[['cusip', 'ticker', 'gvkey']],
                                             cusip_df[['cusip', 'ticker', 'gvkey']]],
                                            ignore_index=True)

        stocks_forecast_info_df['gvkey'] = stocks_forecast_info_df.groupby('ticker')[['gvkey']].apply(
            lambda x: x.bfill().ffill())

        stocks_forecast_info_df = stocks_forecast_info_df.drop_duplicates(['ticker', 'cusip'])

        return stocks_forecast_info_df

    def _save_stocks_forecasts_info_in_mongodb(self, stocks_forecast_info_df: pd.DataFrame):

        """
        Description:
        ------------

        This function is used to save stocks forecasts information in the mongo DB.

        Parameter:
        ----------

        :param stocks_forecast_info_df: DataFrame of stocks forecasts information containing the columns
        [gvkey, cusip, ticker]
        :type stocks_forecast_info_df: pd.DataFrame

        Raise Error:
        ------------

        :raise ValueError when the column doesn't contain gvkey, cusip, ticker.

        Return:
        -------

        :return: none

        Usage:
        ------
        Stocks(self._connection_string)._save_stocks_forecasts_info_in_mongodb()
        """

        # TODO: implements raiseError

        def group_stocks_forecast_info(gvkey, cusip, ticker):
            return InsertOne({'gvkey': gvkey, 'cusip_8': cusip, 'ibtic': ticker, '_id': ticker + '_' + cusip})

        stocks_forecast_info_df.loc[:, 'to_save'] = stocks_forecast_info_df. \
            apply(lambda x: group_stocks_forecast_info(*x[['gvkey', 'cusip', 'ticker']]), axis=1)

        # Add stocks forecast info to mongo DB.
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_FORECASTS_INFO_DB_COL_NAME]
        tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db,
                                                                 stocks_forecast_info_df.
                                                                 loc[:, 'to_save'].values.tolist(),
                                                                 None).set_data_in_db)

    def save_stocks_forecasts_info_in_mongodb(self):

        """
        Description:
        ------------

        This function is used to download and save all the stocks forecasts info in Mongo DB.
        The function calls the method download_stocks_forecasts_info_from_ibes to download all
        the stocks forecast information. The _merge_stocks_forecast_info_with_gvkey method is
        called to add the GVKEY in the dataFrame.

        Usage:
        ------

        Stocks(CONNECTION_STRING).save_stocks_forecasts_info_in_mongodb()

        :return: None
        """
        stocks_forecast_info_df = self.download_stocks_forecasts_info_from_ibes()
        stocks_forecast_info_df = self._merge_stocks_forecast_info_with_gvkey(stocks_forecast_info_df)
        self._save_stocks_forecasts_info_in_mongodb(stocks_forecast_info_df)

    def get_stocks_forecast_info_from_mongodb(self, query, to_display):

        """
        Description:
        ------------

        This function is used to get stocks forecasts info data from the mongo DB.

        Parameter:
        ----------

        :param query: query to perform in the DB
        :param to_display: data to display in the returned data

        Type:
        -----

        :type query: dict()
        :type to_display: dict()

        Source:
        -------
        cloud mongo db.

        Usage:
        ------
        query = {'gvkey': '019117'}
        display = None
        stocks_info = Stocks(CONNECTION_STRING).get_stocks_forecast_info_from_mongodb(query, display)
        stocks_info.head(10)

                      _id   cusip_8   gvkey ibtic
        0  000Z_09072V20  09072V20  019117  000Z
        1  000Z_09072V10  09072V10  019117  000Z
        2  000Z_09072V40  09072V40  019117  000Z
        ...

        Return:
        -------

        :return: stocks info dataFrame
        :rtype: pd.DataFrame
        """
        # Download stocks infos fron the mongo db
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][STOCKS_MARKET_FORECASTS_INFO_DB_COL_NAME]
        stocks_price_info = tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, query, to_display).get_data_from_db)

        return stocks_price_info

    ###################################################################################################################
    #
    # Stocks Forecasts info zone.
    #
    ###################################################################################################################

    @staticmethod
    def _download_stocks_forecasts_from_ibes(stocks_forecasts_parameter: collections) -> pd.DataFrame:

        """
            Description:
            ------------

            This function is used to download the stocks forecasts information from the market analysts. The information
            could be price target, recommendations or fundamentals forecasts for a certain horizon of time. We use the IBES
            DB to download the information from the table ptgdet(price target), recddet(consensus).

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
            sql_statement = "select " + ",".join(header) + " From {schema}.{table} WHERE anndats between " \
                                                           "'{start_date}' and '{end_date}'"
            sql_statement = sql_statement.format(schema='ibes', table='recddet',
                                                 start_date=stocks_forecasts_parameter.start_date,
                                                 end_date=stocks_forecasts_parameter.end_date)

        elif stocks_forecasts_parameter.type == TYPE_PRICE_TARGET:

            header = ['ticker', 'cusip', 'estimid', 'horizon', 'value', 'estcur', 'anndats', 'amaskcd']
            sql_statement = "select " + ",".join(header) + " From {schema}.{table} WHERE anndats between " \
                                                           "'{start_date}' and '{end_date}'"
            sql_statement = sql_statement.format(schema='ibes', table='ptgdet',
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
            stocks_forecasts_df = db.raw_sql(sql_statement)

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

    ###################################################################################################################
    #
    # Stocks monthly summary.
    #
    ###################################################################################################################

    @staticmethod
    def _remove_non_last_date(group: pd.DataFrame) -> pd.DataFrame:
        """
        Description:
        ------------

        This function is used to removed all the value that are not in the end of the month.

        Parameter:
        ----------

        :param group: Group of dataFrame
        :type group: pd.DataFrame

        Return:
        -------

        :return: DataFrame with removed value
        :rtype: pd.DataFrame
        """
        id_max = group['date'].idxmax()
        date_m = group.loc[id_max, 'date']

        return group[group['date'] == date_m]

    def _get_monthly_stocks_price_from_mongodb(self, my_date: date) -> pd.DataFrame:

        """
        Description:
        -----------

        This function is used to download monthly stocks summary for a particular month.

        Parameter:
        ----------
        :param my_date: month we want to query the data
        :type my_date: date

        Return:
        -------

        :return: DataFrame of price data
        :rtype: pd.DataFrame

        """

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
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][SUMMARY_DB_COL_NAME][str(my_date)]
        data = tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, pipeline).get_data_from_db_with_pipeline)

        return data

    def _get_brute_monthly_stocks_price_from_mongodb(self, start_date: date, end_date: date,
                                                     logger: Callable[[str], None] = sys.stdout) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to download the last stocks price data from the mongo db.

        Parameter:
        ----------

        :param start_date: start date
        :param end_date: end date
        :param logger: function used to print the state of the downloading

        Return:
        -------

        :return: DataFrame with the monthly price
        :rtype: pd.DataFrame

        Usage:
        ------
        price = Stocks(CONNECTION_STRING).get_monthly_stocks_price_from_mongodb(date(2015, 1, 1), date(2015, 12, 31))
        price.head(10)

           USD_to_curr  adj_factor          csho   ...   rcvar nrec  nrcvar
        0         1.000     1.00000  1.238157e+09   ...    None  NaN     NaN
        1         1.000     1.00000  5.415934e+08   ...    None  NaN     NaN
        2         1.000     1.00000  2.457172e+09   ...    None  NaN     NaN
        3         1.000     1.00000  1.094957e+09   ...    None  NaN     NaN
        4         1.000     1.51037  9.093184e+08   ...    None  NaN     NaN
        5         1.000     1.00000  8.617718e+08   ...    None  NaN     NaN
        6         1.000     1.00000  8.000000e+07   ...    None  NaN     NaN
        7         1.000     1.00000  1.220295e+08   ...    None  NaN     NaN
        8        15.871     1.00000  8.506744e+06   ...    None  NaN     NaN
        9         1.000     1.00000  6.719499e+08   ...    None  NaN     NaN
        ...
        """
        # Create datetime range between start and end date.
        date_tab = pd.date_range(start_date, end_date, freq='MS').strftime('%Y-%m').tolist()
        tab_parameter = [(my_date,) for my_date in date_tab]

        # Download Data using multiprocessing.
        summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._get_monthly_stocks_price_from_mongodb)
        summary.reset_index(drop=True, inplace=True)

        # Unstack _id, price target and consensus.
        start = time.time()

        # Unstack price target
        pt = {'price': None, 'num_price': None, 'pmean_var': None, 'pnum_var': None, 'mean_var': None, 'num_var': None}
        pt = pd.DataFrame({TYPE_PRICE_TARGET: np.repeat(pt, summary.shape[0])}, index=summary.index)
        summary.loc[:, TYPE_PRICE_TARGET] = \
            summary[TYPE_PRICE_TARGET].where(summary[TYPE_PRICE_TARGET].isna() == False, pt[TYPE_PRICE_TARGET])
        pt = pd.DataFrame(summary[TYPE_PRICE_TARGET].tolist(), index=summary.index)
        pt.rename(columns={'price': 'pt', 'num_price': 'npt', 'pmean_var': 'pptvar', 'pnum_var': 'npptvar',
                           'mean_var': 'ptvar', 'num_var': 'nptvar'}, inplace=True)

        # Unstack Consensus
        cs = {'mean_recom': None, 'num_recom': None, 'mean_var': None, 'num_var': None}
        cs = pd.DataFrame({TYPE_CONSENSUS: np.repeat(cs, summary.shape[0])}, index=summary.index)
        summary.loc[:, TYPE_CONSENSUS] = \
            summary[TYPE_CONSENSUS].where(summary[TYPE_CONSENSUS].isna() == False, cs[TYPE_CONSENSUS])
        cs = pd.DataFrame(summary[TYPE_CONSENSUS].tolist(), index=summary.index)
        cs.rename(columns={'mean_recom': 'rec', 'num_recom': 'nrec', 'mean_var': 'rcvar', 'num_var': 'nrcvar'},
                  inplace=True)

        other = [pd.DataFrame(summary['_id'].tolist(), index=summary.index), cs, pt]

        # Merge unstack data with summary table
        summary.drop(['_id', TYPE_PRICE_TARGET, TYPE_CONSENSUS], axis=1, inplace=True)
        summary = pd.concat([summary] + other, axis=1)
        summary.rename(columns={'isin_or_cusip': 'isin', 'price_close': 'pc', 'price_high': 'ph', "price_low": 'pl'},
                       inplace=True)

        logger.write("\nUnstack Price Target and Consensus in {:.1f}s".format(time.time() - start))
        summary.drop_duplicates(['isin', 'curr', 'date'], inplace=True)
        logger.write("\nDownload completed.\n")

        # Convert columns type as appropriate.
        type_str = ['ptvar', 'pptvar', 'pt', 'rec', 'rcvar']
        summary[type_str] = summary[type_str].astype(str)
        summary.replace({'None': np.nan}, inplace=True)

        type_float = ['USD_to_curr', 'adj_factor', 'csho', 'pc', 'ph', 'pl', 'vol', 'rcvar', 'npt', 'nrcvar', 'pptvar',
                      'ptvar', 'npptvar', 'pt', 'rec', 'rcvar', 'nrec', 'nrcvar']

        summary[type_float] = summary[type_float].astype(float)

        return summary

    @staticmethod
    def _correct_stocks(group: pd.DataFrame) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to correct all the mistakes inside the data and return a DataFrame of correct values.

        Parameter:
        ----------

        :param group: DataFrame of stocks price grouped by gvkey.
        :type group: pd.DataFrame

        Return:
        -------

        :return: DataFrame of corrects values.
        :rtype: pd.DataFrame
        """
        def calculate_return(data):
            """
            This function is used to fill NaN csho and compute monthly and price target returns.
            :param data:
            :return:
            """
            data['csho'] = data['csho'].fillna(method='ffill')
            data.set_index('date', inplace=True)
            data['return'] = data['adj_pc'].pct_change(freq='1M')
            data['pt_return'] = data[['adj_pc', 'pt']].pct_change(axis='columns')['pt'].fillna(0)
            data['mc'] = data['csho'] * data['adj_pc'] * data['adj_factor']
            return data.reset_index()

        result = group.groupby(['isin', 'curr']).apply(calculate_return).reset_index(drop=True)
        r = result.groupby(['isin', 'curr'])['return'].nunique().reset_index()
        r_mc = result.groupby(['isin', 'curr'])['mc'].sum().reset_index()
        index_max_ret = r['return'].idxmax()
        index_max_mc = r_mc['mc'].idxmax()

        if r.loc[index_max_mc, 'return'] == r.loc[index_max_ret, 'return']:
            index_max = index_max_mc
        elif r.loc[index_max_mc, 'return']/r.loc[index_max_ret, 'return'] >= .9:
            index_max = index_max_mc
        else:
            index_max = index_max_ret
        isin = r.loc[index_max, 'isin']
        curr = r.loc[index_max, 'curr']
        return result[(result['isin'] == isin) & (result['curr'] == curr)]

    def download_monthly_stocks_summary(self, start_date: date, end_date: date) -> pd.DataFrame:

        """
        Definition:
        -----------

        This function is used to correct all the imperfections found inside the data.

        Parameter:
        ----------

        :param start_date:
        :param end_date:

        :type start_date: date
        :type end_date: date

        Usage:
        ------
        summary = Stocks(CONNECTION_STRING).download_monthly_stocks_summary(date(2017, 12, 1), date(2017, 12, 31))
        summary.head(10)

                        date  USD_to_curr      ...       pt_return            mc
        0 2017-12-31 16:00:00          1.0      ...        0.124968  1.364502e+09
        0 2017-12-31 16:00:00          1.0      ...        0.000000  4.535500e+07
        0 2017-12-31 16:00:00          1.0      ...        0.000000  3.474360e+04
        0 2017-12-31 16:00:00          1.0      ...        0.115872  2.489630e+10
        0 2017-12-31 16:00:00          1.0      ...        0.730019  1.780520e+08
        0 2017-12-31 16:00:00          1.0      ...        0.000000  2.183628e+08
        0 2017-12-31 16:00:00          1.0      ...        0.040462  2.911659e+09
        0 2017-12-31 16:00:00          1.0      ...        0.036237  9.517161e+09
        0 2017-12-31 16:00:00          1.0      ...        0.198813  2.819906e+09
        0 2017-12-31 16:00:00          1.0      ...        0.050419  9.933610e+10

        Return:
        ------

        :return: Stocks price summary with corrections of values
        :rtype: pd.DataFrame
        """

        # print('\n************* Download of monthly data ******************')
        # data = np.load('data without duplicates dates.npy').item()
        # monthly_summary = pd.DataFrame(data['data'])
        # monthly_summary.columns = data['header']

        # monthly_summary['date'] = pd.DatetimeIndex(monthly_summary['date'])
        # f_header = ['adj_factor', 'csho', 'pc', 'pl', 'ph', 'vol', 'USD_to_curr', 'rec', 'rcvar', 'nrec', 'nrcvar',
        #             'ptvar', 'npt', 'nptvar', 'pptvar', 'npptvar', 'pt', 'adj_pc']
        # monthly_summary[f_header] = monthly_summary[f_header].astype(float)
        #
        # print(monthly_summary.info())
        #
        # monthly_summary['date'] = pd.DatetimeIndex(monthly_summary['date']) + pd.offsets.MonthEnd(0)
        # group = monthly_summary.groupby('gvkey')
        # tab_parameter = [(data,) for name, data in group]
        # patch_monthly_summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._correct_stocks)
        #
        # d = dict()
        # d['header'] = patch_monthly_summary.columns
        # d['data'] = patch_monthly_summary
        # np.save('data clean.npy', d)
        # message = "download completed"
        # SendSimulationState(message).send_email()
        # print(patch_monthly_summary.info())

        data = np.load('data clean.npy').item()
        patch_monthly_summary = pd.DataFrame(data['data'])
        patch_monthly_summary.columns = data['header']
        print(patch_monthly_summary.info())
        # patch_monthly_summary = patch_monthly_summary[patch_monthly_summary['gvkey'] == '001690']
        header = ['date', 'gvkey', 'isin', 'csho', 'vol', 'pc', 'ph', 'pl', 'adj_factor', 'adj_pc', 'mc',
                  'return', 'curr', 'USD_to_curr', 'rec', 'rcvar', 'nrec', 'nrcvar', 'ptvar', 'npt',
                  'nptvar', 'pptvar', 'npptvar', 'pt', 'pt_return']
        patch_monthly_summary.dropna(subset=['mc'], inplace=True)
        patch_monthly_summary.dropna(subset=['return'], inplace=True)

        patch_monthly_summary.loc[patch_monthly_summary['pt_return'] == 0, 'pt_return'] = None
        print(patch_monthly_summary.head())
        # patch_monthly_summary.loc[:, 'to_save'] = patch_monthly_summary.apply(
        #     lambda x: self._stack_summary(*x[header]), axis=1)
        #
        # print(patch_monthly_summary['to_save'].values[5])


        client_db = motor.motor_tornado.MotorClient(PROD_CONNECTION_STRING)

        def _save_to_mongodb(month, group):

            def stack_summary(date, gvkey, isin, csho, vol, pc, ph, pl, adj_factor, adj_pc, mc, ret, curr, USD_to_curr,
                              rec, rcvar, nrec, nrcvar, ptvar, npt, nptvar, pptvar, npptvar, pt, pt_return):

                return InsertOne({'date': date, '_id': gvkey, 'isin_or_cusip': isin, 'csho': csho, 'vol': vol, 'pc': pc,
                                  'ph': ph, 'pl': pl, 'adj_factor': adj_factor, 'adj_pc': adj_pc, 'mc': mc, 'ret': ret,
                                  'curr': curr, 'USD_to_curr': USD_to_curr,
                                  TYPE_CONSENSUS: {'rec': rec, 'rcvar': rcvar, 'nrec': nrec, 'nrcvar': nrcvar},
                                  TYPE_PRICE_TARGET: {'ptvar': ptvar, 'npt': npt, 'nptvar': nptvar, 'pptvar': pptvar,
                                                      'npptvar': npptvar, 'pt': pt, 'pt_ret': pt_return}})

            header = ['date', 'gvkey', 'isin', 'csho', 'vol', 'pc', 'ph', 'pl', 'adj_factor', 'adj_pc', 'mc', 'return',
                      'curr', 'USD_to_curr', 'rec', 'rcvar', 'nrec', 'nrcvar', 'ptvar', 'npt', 'nptvar', 'pptvar',
                      'npptvar', 'pt', 'pt_return']

            group.loc[:, 'to_save'] = group.apply(lambda x: stack_summary(*x[header]), axis=1)

            if group.shape[0] > 0:
                db = client_db[STOCKS_MARKET_DATA_DB_NAME][M_SUMMARY_DB_COL_NAME][month]
                tornado.ioloop.IOLoop.current().run_sync(
                    DataFromMongoDB(db, group.loc[:, 'to_save'].values.tolist()).set_data_in_db)
            return 0

        group = patch_monthly_summary.groupby('date_m')
        # tab_parameter = [(name, data) for name, data in group]


        for name, data in group:
            _save_to_mongodb(name, data)
            print('date {} completed'.format(name))
        # patch_monthly_summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._save_to_mongodb)

        # d = dict()
        # d['header'] = result.columns
        # d['data'] = result
        # np.save('data without duplicates dates.npy', d)
        message = "Save in mongo db completed"
        SendSimulationState(message).send_email()


        return None


        monthly_summary = self._get_brute_monthly_stocks_price_from_mongodb(start_date, end_date)
        monthly_summary.sort_values(by=['isin', 'curr', 'date'], ascending=[True, True, True], inplace=True)
        monthly_summary = monthly_summary[(monthly_summary['isin'].isna() == False) &
                                          (monthly_summary['gvkey'].isna() == False)].reset_index(drop=True)

        # adjusted prices
        monthly_summary.loc[:, 'adj_pc'] = monthly_summary['pc'] / monthly_summary['adj_factor'] / monthly_summary['USD_to_curr']

        # remove non last month dates
        print("\n*************** Remove date non end of month ************")
        monthly_summary.loc[:, 'date_m'] = monthly_summary.loc[:, 'date'].dt.strftime('%Y-%m')
        group = monthly_summary.groupby(['gvkey', 'date_m'])

        tab_parameter = [(data,) for name, data in group]
        monthly_summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._remove_non_last_date)

        # fill csho, take max historical data, calculate return
        print("\n*************** Correct monthly data. ********************")
        monthly_summary['date'] = pd.DatetimeIndex(monthly_summary['date']) + pd.offsets.MonthEnd(0)
        group = monthly_summary.groupby('gvkey')
        tab_parameter = [(data,) for name, data in group]
        patch_monthly_summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._correct_stocks)
        patch_monthly_summary.loc[patch_monthly_summary['pt_return'] == 0, 'pt_return'] = None

        return patch_monthly_summary


    @staticmethod
    def _stack_summary(date, gvkey, isin, csho, vol, pc, ph, pl, adj_factor, adj_pc, mc, ret, curr, USD_to_curr,
                          rec, rcvar, nrec, nrcvar, ptvar, npt, nptvar, pptvar, npptvar, pt, pt_return):

            return InsertOne({'date': date, '_id': gvkey, 'isin_or_cusip': isin, 'csho': csho, 'vol': vol, 'pc': pc,
                              'ph': ph, 'pl': pl, 'adj_factor': adj_factor, 'adj_pc': adj_pc, 'mc': mc, 'ret': ret,
                              'curr': curr, 'USD_to_curr': USD_to_curr,
                              TYPE_CONSENSUS: {'rec': rec, 'rcvar': rcvar, 'nrec': nrec, 'nrcvar': nrcvar},
                              TYPE_PRICE_TARGET: {'ptvar': ptvar, 'npt': npt, 'nptvar': nptvar, 'pptvar': pptvar,
                                                  'npptvar': npptvar, 'pt': pt, 'pt_ret': pt_return}})


    def save_monthly_stocks_summary(self, start_date: date, end_date: date):

        """
        Description:
        ------------

        This function is used to save monthly summary to the mongo db.

        Parameter:
        ----------

        :param start_date:
        :param end_date:

        :type start_date: date
        :type end_date: date

        Usage:
        ------

        Stocks(CONNECTION_STRING).save_monthly_stocks_summary()

        Return:
        -------

        :return: None
        """
        def save_to_mongodb(group):
            """
            This function is used to save the stocks price into the mongo db
            :param group: group of stocks to save for a particular month
            :return: None
            """
            Date = group.name
            db = client_db[STOCKS_MARKET_DATA_DB_NAME][M_SUMMARY_DB_COL_NAME][str(Date)]
            tornado.ioloop.IOLoop.current().run_sync(
                DataFromMongoDB(db, group.loc[:, 'to_save'].values.tolist()).set_data_in_db)

        def stack_summary(date, gvkey, isin, csho, vol, pc, ph, pl, adj_factor, adj_pc, mc, ret, curr, USD_to_curr,
                          rec, rcvar, nrec, nrcvar, ptvar, npt, nptvar, pptvar, npptvar, pt, pt_return):

            return InsertOne({'date': date, '_id': gvkey, 'isin_or_cusip': isin, 'csho': csho, 'vol': vol, 'pc': pc,
                              'ph': ph, 'pl': pl, 'adj_factor': adj_factor, 'adj_pc': adj_pc, 'mc': mc, 'ret': ret,
                              'curr': curr, 'USD_to_curr': USD_to_curr,
                              TYPE_CONSENSUS: {'rec': rec, 'rcvar': rcvar, 'nrec': nrec, 'nrcvar': nrcvar},
                              TYPE_PRICE_TARGET: {'ptvar': ptvar, 'npt': npt, 'nptvar': nptvar, 'pptvar': pptvar,
                                                  'npptvar': npptvar, 'pt': pt, 'pt_ret': pt_return}})

        summary = self.download_monthly_stocks_summary(start_date, end_date)
        header = ['date', 'gvkey', 'isin', 'csho', 'vol', 'pc', 'ph', 'pl', 'adj_factor', 'adj_pc', 'mc', 'return',
                  'curr', 'USD_to_curr', 'rec', 'rcvar', 'nrec', 'nrcvar', 'ptvar', 'npt', 'nptvar', 'pptvar',
                  'npptvar', 'pt', 'pt_return']
        summary['to_save'] = summary.apply(lambda x: stack_summary(*x[header]), axis=1)
        client_db = motor.motor_tornado.MotorClient(self._connection_string)


        print("\n******************* Save result in the DB ************************")
        summary.groupby('date_m').apply(save_to_mongodb)

    def drop_monthly_stocks_summary(self, start_date: date, end_date: date):

        """
        Description:

        This function is used to delete saved collections between start and end date.

        """
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME]
        date_tab = pd.date_range(start_date, end_date, freq='MS').strftime('%Y-%m').tolist()

        for name in date_tab:
            tornado.ioloop.IOLoop.current().run_sync(
                DataFromMongoDB(db, M_SUMMARY_DB_COL_NAME + '.' + name).drop_col_from_db)

    def _get_monthly_summary_from_mongodb(self, my_date: date, query: dict, to_display: dict) -> pd.DataFrame:

        """
        Description:
        -----------

        This function is used to download monthly stocks summary for a particular month.

        Parameter:
        ----------

        :param my_date: month we want to query the data
        :param query: query we want to perform
        :param to_display: data to display

        :type my_date: date
        :type query: dict
        :type to_display: dict

        Usage:
        ------
        my_date = date(2017, 12, 31)
        query = {}
        to_display = None
        result = Stocks(CONNECTION_STRING)._get_monthly_summary_from_mongodb(my_date, query, to_display)
        result.head(15)

           USD_to_curr     _id      ...            ret           vol
        0           1.0  001004      ...       0.075961  2.309660e+06
        1           1.0  001019      ...       0.000000  0.000000e+00
        2           1.0  001021      ...       0.000000  0.000000e+00
        3           1.0  001045      ...       0.047684  1.103207e+08
        4           1.0  001072      ...      -0.041975  3.301457e+06
        5           1.0  001076      ...      -0.118293  1.812095e+07
        6           1.0  001078      ...       0.079243  1.654483e+08
        7           1.0  001094      ...      -0.196438  1.050210e+07
        8           1.0  001097      ...       0.000000  0.000000e+00
        9           1.0  001104      ...       0.061166  9.372400e+04
        10          1.0  001109      ...      -0.500000  3.996046e+08
        11          1.0  001119      ...       0.037319  3.099422e+06
        12          1.0  001121      ...       0.045161  9.538900e+04
        13          1.0  001126      ...       0.000000  0.000000e+00
        14          1.0  001161      ...       0.394407  1.568686e+09

        Return:
        -------

        :return: DataFrame of monthly summary
        :rtype: pd.DataFrame

        """

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[STOCKS_MARKET_DATA_DB_NAME][M_SUMMARY_DB_COL_NAME][str(my_date)]
        data = tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, query, to_display).get_data_from_db)

        return data

    def get_monthly_summary_from_mongodb(self, start_date: date, end_date: date, query: dict, to_display: dict,
                                         logger: Callable[[str], None] = sys.stdout) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to download the summary infos from the mongo db.

        Parameter:
        ----------

        :param start_date: start date
        :param end_date: end date
        :param query: query of data to perform
        :param to_display: data to display after the query
        :param logger: function used to print the state of the downloading

        Return:
        -------

        :return: DataFrame with the monthly price
        :rtype: pd.DataFrame

        Usage:
        ------
        pc = Stocks(CONNECTION_STRING).get_monthly_summary_from_mongodb(date(2017, 1, 1), date(2017, 12, 12), {}, None)
        pc.head(10)

            USD_to_curr isin_or_cusip  adj_factor   ...         pt    pt_ret   ptvar
        0           1.0        001004         1.0   ...    34.5000  0.078462  0.1705
        1           1.0        001019         1.0   ...        NaN  0.000000     NaN
        2           1.0        001021         1.0   ...        NaN  0.000000     NaN
        3           1.0        001045         1.0   ...    52.2353  0.180459    0.18
        4           1.0        001072         1.0   ...    15.7500 -0.027778  0.0857
        5           1.0        001076         1.0   ...    34.3750  0.111021   0.113
        6           1.0        001078         1.0   ...    46.5000  0.113239  0.0022
        7           1.0        001094         1.0   ...    27.0000  0.414353  0.0549
        8           1.0        001097         1.0   ...        NaN  0.000000     NaN
        9           1.0        001104         1.0   ...    26.2500  0.077586  0.1218
        10          1.0        001109         1.0   ...        NaN  0.000000     NaN
        11          1.0        001119         1.0   ...        NaN  0.000000     NaN
        12          1.0        001121         1.0   ...        NaN  0.000000     NaN
        13          1.0        001126         1.0   ...        NaN  0.000000     NaN
        14          1.0        001161         1.0   ...    10.2619 -0.010424  0.5653
        ...
        """

        # Create datetime range between start and end date.
        date_tab = pd.date_range(start_date, end_date, freq='MS').strftime('%Y-%m').tolist()
        tab_parameter = [(my_date, query, to_display,) for my_date in date_tab]

        # Download Data using multiprocessing.
        summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._get_monthly_summary_from_mongodb)

        # Unstack _id, price target and consensus.
        start = time.time()
        pt = pd.DataFrame(summary[TYPE_PRICE_TARGET].tolist(), index=summary.index)
        cs = pd.DataFrame(summary[TYPE_CONSENSUS].tolist(), index=summary.index)

        # Merge unstack data with summary table
        other = [cs, pt]
        summary.drop([TYPE_PRICE_TARGET, TYPE_CONSENSUS], axis=1, inplace=True)
        summary = pd.concat([summary] + other, axis=1)
        summary.rename(columns={'_id': 'gvkey'}, inplace=True)

        logger.write("\nUnstack Price Target and Consensus in {:.1f}s".format(time.time() - start))
        logger.write("\nDownload completed.\n")

        return summary


if __name__ == '__main__':

    # Stocks(PROD_CONNECTION_STRING).drop_monthly_stocks_summary(date(1998, 1, 1), date(2017, 12, 31))

    # result = []
    #
    # for year in range(2017, 2018):
    #     print("\n *********** Year: {} Started **************".format(year))
    #     data = Stocks(PROD_CONNECTION_STRING)._get_brute_monthly_stocks_price_from_mongodb(date(year, 12, 1),
    #                                                                                        date(year, 12, 31))
    #
    #     result.append(data)
    #     print("\n *********** Year: {} Completed **************".format(year))
    #
    # result = pd.concat(result, ignore_index=True)

    # Stocks(PROD_CONNECTION_STRING).save_monthly_stocks_summary(date(1998, 1, 1), date(2017, 12, 31))

    # date_tab = pd.date_range(date(1998, 1, 1), date(2007, 12, 31), freq='MS').strftime('%Y-%m').tolist()
    # tab_parameter = [(my_date,) for my_date in date_tab]

    # Download Data using multiprocessing.
    # summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._get_monthly_stocks_price_from_mongodb)



    # Stocks(TEST_CONNECTION_STRING).save_monthly_stocks_summary(date(1998, 1, 1), date(2017, 12, 31))
    # print(v.head(15))
    # print(v.shape)
    # query = {'gvkey': '019117'}
    # print(Stocks(CONNECTION_STRING).get_stocks_forecast_info_from_mongodb(query, None))
    print(Stocks(PROD_CONNECTION_STRING).
          get_monthly_summary_with_eco_zone_and_sector_from_mongodb(start_date=date(2017, 1, 1),
                                                                    end_date=date(2017, 12, 31),
                                                                    sector=NAICS,
                                                                    query_sector_mapping={'eco zone': 'USD', 'level': '2'},
                                                                    to_display=None))
    # print(Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).
    #       get_stocks_summary_with_sector_and_eco_zone(start_date=date(2017, 1, 1), end_date=date(2017, 12, 31),
    #                                                   query_sector_mapping={'eco zone': 'USD', 'level': '2'},
    #                                                   to_display=None))
