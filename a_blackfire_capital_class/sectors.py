import motor
import tornado

import pandas as pd
from pathlib import Path
from datetime import date
from pymongo import InsertOne

from a_blackfire_capital_class.stocks import Stocks
from a_blackfire_capital_class.data_from_mongodb import DataFromMongoDB
from a_blackfire_capital_class.useful_class import CustomMultiprocessing
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING, PROD_CONNECTION_STRING
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import SECTORS_MARKET_DATA_DB_NAME, \
    SECTORS_MARKET_DATA_INFO_DB_COL_NAME, NAICS, ECONOMICS_ZONES_DB_NAME, ECONOMICS_ZONES_DB_COL_NAME, \
    SECTORS_MARKET_DATA_MAPPING_DB_COL_NAME


class Sectors:

    """
    Description:
    ------------

    This class is used to get and to save all the type of information related to the sectors. The sectors are grouped
    in NAICS, GIC and SIC. The information are divided in 3 categories:

    1. The sectors price information which contain monthly price close, volume, common shares,
    currency, exchange rates with USD.

    2. The fundamentals information which contains all the useful ratios for the sector evaluation.

    3. The forecasts information which contain the information about the analyst recommendations, the price target and
    the forecasts of the fundamentals of the sectors.

    To get those information we refer to the Mongo db.

    The class is divided in 3 main methods:

    1. compute method is used to download the data from the mongo db and perform a specific operation.

    2. Save method is used to save the data into the Mongo DB.

    3. Get methods is used to get the Data we store in the Mongo DB.

    """

    def __init__(self, **kwargs):

        """
        To initialize the class we need the connection string off the mongo db. This will be used to query the data.
        :param kwargs: args used to compute sectors data.
        1. connection_string
        2. by: sectors classifications (NAICS, GIC, SIC, etc...)
        """

        self._connection_string = kwargs.get('connection_string', TEST_CONNECTION_STRING)
        self._by = kwargs.get('by', NAICS)

    ###################################################################################################################
    #
    # Sectors info.
    #
    ###################################################################################################################

    def save_sectors_info_in_mongodb(self):

        """
        This function is used to save stocks information in the mongo db.

        :return: None.
        """
        def to_save(level, code, title, definition):
            return InsertOne({'_id': code, 'level': level, 'title': title, 'description': definition})

        if self._by == NAICS:
            my_path = Path(__file__).parent.parent.resolve()
            my_path = str(my_path) + '/e_blackfire_capital_files/naics.csv'
            data = pd.read_csv(my_path, encoding="ISO-8859-1")

            data['Level'] = data['Level'].astype(int)
            data = data[(data['Level'] < 3) & (data['Superscript'] != 'CAN')]
            data.to_excel('test.xlsx')
            data['to_save'] = data.apply(lambda x: to_save(*x[['Level', 'Code', 'Class title', 'Class definition']]),
                                         axis=1)

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[SECTORS_MARKET_DATA_DB_NAME][self._by][SECTORS_MARKET_DATA_INFO_DB_COL_NAME]
        tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, data.loc[:, 'to_save'].values.tolist()).set_data_in_db)

    def get_sectors_info_from_db(self, **kwargs: dict) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to get information from sectors.

        Parameter:
        ----------

        :param kwargs: argument used to query the sector info.
        1. query
        2. to_display

        Return:
        -------

        :return: DataFrame of query value.
        :rtype: pd.DataFrame

        Usage:
        ------
        info = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).get_sectors_info_from_db()
        info.head(15)

             _id                        ...                                                                      title
        0     11                        ...                                 Agriculture, forestry, fishing and hunting
        1    111                        ...                                                            Crop production
        2    112                        ...                                          Animal production and aquaculture
        3    113                        ...                                                       Forestry and logging
        4    114                        ...                                              Fishing, hunting and trapping
        5    115                        ...                            Support activities for agriculture and forestry
        6     21                        ...                              Mining, quarrying, and oil and gas extraction
        7    211                        ...                                                     Oil and gas extraction
        8    212                        ...                                  Mining and quarrying (except oil and gas)
        9    213                        ...                          Support activities for mining, and oil and gas...
        10    22                        ...                                                                  Utilities
        11   221                        ...                                                                  Utilities
        12    23                        ...                                                               Construction
        13   236                        ...                                                  Construction of buildings
        14   237                        ...                                   Heavy and civil engineering construction
        15   238                        ...                                                Specialty trade contractors
        ...

        """

        query = kwargs.get('query', {})
        to_display = kwargs.get('to_display', None)

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[SECTORS_MARKET_DATA_DB_NAME][self._by][SECTORS_MARKET_DATA_INFO_DB_COL_NAME]
        data = tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, query, to_display).get_data_from_db)

        return data

    @staticmethod
    def _map_sector_and_stocks(by, sector, zone, group):
        """
        Description:
        ------------

        This function is used to map the gvkey to sector and eco zone.

        Parameter:
        ----------

        :param by: sector we want to map (NAICS, GIC, etc...)
        :param sector: sector code
        :param zone: economics zone
        :param group: dataFrame with the stocks info

        Return:
        -------

        :return: pd.DataFrame of mapping for each zone and sector

        """
        dt = group[(group['eco zone'] == zone) & (group[by].str.startswith(sector, na=False))][['gvkey']]
        dt['eco zone'] = zone
        dt[by] = sector

        return dt

    def download_sectors_mapping_in_mongodb(self) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to save the relationship between sectors, eco zone and gvkey.

        :return: dataFrame of mapping between gvkey, sector and eco zone.
        """

        # get sector from the mongo DB.
        query = {"$or": [{"level": 1}, {"level": 2}]}
        to_display = {"_id": 1, "level": 1}
        sector_tab = self.get_sectors_info_from_db(query=query, to_display=to_display)
        sector_tab.rename(columns={'_id': self._by}, inplace=True)

        # get zone eco from mongo DB.
        query = {}
        to_display = {"_id": 1, "eco zone": 1}
        eco_zone_tab = self.get_eco_zone_from_mongodb(query=query, to_display=to_display)
        eco_zone_tab.rename(columns={'_id': 'country'}, inplace=True)

        # Create a DataFrame that map the sector and the eco zone
        mapping = pd.DataFrame([[zone, sector[0], sector[1]]
                                for zone in eco_zone_tab['eco zone'].unique()
                                for sector in sector_tab[[self._by, 'level']].values],
                               columns=['eco zone', self._by, 'level'])
        mapping = mapping.drop_duplicates(subset=['eco zone', self._by, 'level'])
        mapping = mapping.astype(str)

        # stocks info
        to_display = {'_id': 1, self._by: 1, 'eco zone': 1}
        stocks_info = Stocks(self._connection_string).get_stocks_price_info_from_mongodb({}, to_display)
        stocks_info.drop_duplicates(['gvkey'], inplace=True)

        group = mapping.groupby(['eco zone', self._by])
        tab_parameter = [(self._by, name[1], name[0], stocks_info) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._map_sector_and_stocks)
        mapping = pd.merge(result, sector_tab.astype(str), on=self._by)

        return mapping

    def save_sectors_mapping_in_mongodb(self):

        """
        Description:
        ------------

        This function is used to save mapping information in the mongo DB.

        :return: None.
        """
        def to_save(gvkey, zone, sector, level):

            return InsertOne({'_id': zone + gvkey + sector, 'level': level, 'sector': sector, 'eco zone': zone,
                              'gvkey': gvkey})

        data = self.download_sectors_mapping_in_mongodb()
        data['to_save'] = data.apply(lambda x: to_save(*x[['gvkey', 'eco zone', self._by, 'level']]), axis=1)
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[SECTORS_MARKET_DATA_DB_NAME][self._by][SECTORS_MARKET_DATA_MAPPING_DB_COL_NAME]
        tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, data.loc[:, 'to_save'].values.tolist()).set_data_in_db)

    def get_sectors_mapping_from_db(self, **kwargs: dict) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to get mapping information from the mongo db.

        Parameter:
        ----------

        :param kwargs: argument used to query the sector mapping.
        1. query
        2. to_display

        Return:
        -------

        :return: DataFrame of query value.
        :rtype: pd.DataFrame

        Usage:
        ------
        info = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).get_sectors_mapping_from_db()
        info.head(15)
                    _id eco zone   gvkey level sector
        0   USD00939111      USD  009391     1     11
        1   USD01924611      USD  019246     1     11
        2   USD02514311      USD  025143     1     11
        3   USD02966711      USD  029667     1     11
        4   USD10028611      USD  100286     1     11
        5   USD10031011      USD  100310     1     11
        6   USD10042211      USD  100422     1     11
        7   USD10048711      USD  100487     1     11
        8   USD10066311      USD  100663     1     11
        9   USD10068211      USD  100682     1     11
        10  USD10083711      USD  100837     1     11
        11  USD10135811      USD  101358     1     11
        12  USD10144411      USD  101444     1     11
        13  USD10145911      USD  101459     1     11
        14  USD10165411      USD  101654     1     11
        ...

        """

        query = kwargs.get('query', {})
        to_display = kwargs.get('to_display', None)

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[SECTORS_MARKET_DATA_DB_NAME][self._by][SECTORS_MARKET_DATA_MAPPING_DB_COL_NAME]
        data = tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, query, to_display).get_data_from_db)

        return data

    ##################################################################################################################
    #
    # Eco zone
    #
    ##################################################################################################################

    # TODO: implement this function
    def save_eco_zone_in_mongodb(self):

        """

        :return:
        """

    def get_eco_zone_from_mongodb(self, **kwargs: dict) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to get all the eco zone info from the DB.

        Parameter:
        ----------

        :param kwargs: parameter used to query al the eco zone parameter.
        1. query
        2. to_display: data to display form the query

        Usage:
        ------
        Sectors(by=NAICS, connection_string=TEST_CONNECTION_STRING).get_eco_zone_from_mongodb()

        Return:
        -------

        :return: DataFrame of eco zones.
        :rtype: pd.DataFrame
        """
        query = kwargs.get('query', {})
        to_display = kwargs.get('to_display', None)

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[ECONOMICS_ZONES_DB_NAME][ECONOMICS_ZONES_DB_COL_NAME]
        data = tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, query, to_display).get_data_from_db)

        return data

    ###################################################################################################################
    #
    # Sectors Data.
    #
    ###################################################################################################################
    @staticmethod
    def _shift_mc(group):
        """
        Description:
        -----------

        This function is used to shift the market for 1 period in a group of Data.
        This shift will be further used to compute the monthly return of the sectors in a month.

        Parameter:
        ----------

        :param group: DataFrame of containing the monthly prices information

        Return:
        -------
        :return: Data frame of one columns containing the market capitalisation shifted by n month.

        """

        group['index'] = group.index
        group = group.set_index('date')
        t = group[['mc']].shift(periods=1, freq='M')
        group.loc[:, 'mc'] = t['mc']

        return group.set_index('index')[['mc']]

    @staticmethod
    def _compute_sector_summary(name, group) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to compute all the summary informations for the sectors. With this function
        we compute:
        - return: sector return for month t
        - mc:  market capitalisation of the sector at month t
        - vol: volume traded for the sector at month t
        - pt_return: price target return of the sector at month t
        - mpt_return: mean of price target return of the sector at month t

        Parameter:
        ----------

        :param group: Data frame containing the stocks summary information's for month t

        Usage:
        ------
        Sectors(by=NAICS, connection_string=TEST_CONNECTION_STRING).download_monthly_sectors_summary(date(2017, 1, 1),
                                                                                                     date(2017, 12, 31))

                      mc        vol         ...          sector                date
        0   9.966922e+08  1545053.0         ...              11 2017-02-28 16:00:00
        0   9.667435e+08  3903463.0         ...              11 2017-03-31 16:00:00
        0   1.004848e+09  1763936.0         ...              11 2017-04-30 16:00:00
        0   8.677182e+08  3749191.0         ...              11 2017-05-31 16:00:00
        0   8.239144e+08  2896507.0         ...              11 2017-06-30 16:00:00
        0   8.669049e+08  2491082.0         ...              11 2017-07-31 16:00:00
        0   8.891579e+08  2146321.0         ...              11 2017-08-31 16:00:00
        0   8.744264e+08  1419599.0         ...              11 2017-09-30 16:00:00
        0   8.323584e+08  2562132.0         ...              11 2017-10-31 16:00:00
        ...

        Return:
        -------

        :return:  Data frame of sectors summary for month t.
        """
        zone = name[0]
        sector = name[1]
        date_ = name[2]

        sum = ['mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrec', 'nrcvar']

        if group['mc_s'].sum() != 0:
            ret = (group['mc_s'] * group['ret']).sum() / group['mc_s'].sum()
            pt_return = (group['mc_s'] * group['pt_ret']).sum() / group['mc_s'].sum()
        else:
            ret = None
            pt_return = None

        mpt_return = group['pt_ret'].mean()

        if group['npptvar'].sum() != 0:
            pptvar = (group['pptvar'] * group['npptvar']).sum() / group['npptvar'].sum()
        else:
            pptvar = None

        if group['nptvar'].sum() != 0:
            # ptvar = (group['ptvar'] * group['nptvar']).sum() / group['nptvar'].sum()
            ptvar=0
        else:
            ptvar = None

        if group['nrec'].sum() != 0:
            rc = (group['rec'] * group['nrec']).sum() / group['nrec'].sum()
        else:
            rc = None

        if group['nrcvar'].sum() != 0:
            rcvar = (group['rcvar'] * group['nrcvar']).sum() / group['nrcvar'].sum()
        else:
            rcvar = None

        tab = pd.DataFrame([[ret, pt_return, mpt_return, pptvar, ptvar, rc, rcvar]],
                           columns=['ret', 'pt_ret', 'mpt_ret', 'pptvar', 'ptvar', 'rec', 'rcvar'])

        tab = pd.concat([group[sum].sum().to_frame().transpose(), tab], axis=1, ignore_index=True)

        tab.columns = ['mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrec', 'nrcvar', 'ret', 'pt_ret', 'mpt_ret',
                       'pptvar', 'ptvar', 'rec', 'rcvar']

        tab['eco zone'] = zone
        tab['sector'] = sector
        tab['date'] = date_

        return tab

    def download_monthly_sectors_summary(self, start_date: date, end_date: date) -> pd.DataFrame:
        """

        :param start_date:
        :param end_date:
        :return:
        """
        mapping = self.get_sectors_mapping_from_db()
        m_summary = Stocks(self._connection_string).get_monthly_summary_from_mongodb(start_date, end_date, {}, None)

        # map summary data and mapping.
        m_summary = pd.merge(m_summary, mapping[['gvkey', 'eco zone', 'sector']], on=['gvkey'])

        # shift market cap
        group = m_summary[['date', 'sector', 'isin_or_cusip', 'mc']].groupby(['sector', 'isin_or_cusip'])
        tab_parameter = [(data, ) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._shift_mc)
        result.rename(columns={'mc': 'mc_s'}, inplace=True)
        m_summary = pd.merge(m_summary, result, left_on=m_summary.index, right_on=result.index)

        # calculate sector summary
        header = ['date', 'sector', 'eco zone', 'mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrec', 'nrcvar',
                  'pt_ret', 'pptvar', 'ptvar', 'ret', 'rcvar', 'rec', 'mc_s']
        group = m_summary[header].groupby(['eco zone', 'sector', 'date'])
        tab_parameter = [(name, data) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._compute_sector_summary)
        result.dropna(subset=['ret'], inplace=True)

        return result

    def save_monthly_sectors_summary(self, start_date: date, end_date: date) -> pd.DataFrame:

        """

        :param start_date:
        :param end_date:
        :return:
        """

    def get_monthly_sectors_summary(self):

        """

        :return:
        """

if __name__ == '__main__':

    Sectors(by=NAICS, connection_string=TEST_CONNECTION_STRING).download_monthly_sectors_summary(date(2017, 1, 1),
                                                                                                     date(2017, 12, 31))
