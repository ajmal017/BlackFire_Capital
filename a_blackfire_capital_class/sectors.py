import time
import motor
import tornado
import numpy as np
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
    SECTORS_MARKET_DATA_MAPPING_DB_COL_NAME, TYPE_CONSENSUS, TYPE_PRICE_TARGET, M_SUMMARY_DB_COL_NAME


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

    Usage:
    ------
    1. save sector infos: Sectors(by=NAICS, connection_string=TEST_CONNECTION_STRING).save_sectors_info_in_mongodb()
    2. save mapping sector and gvkey .save_sectors_mapping_in_mongodb()
    3. save sector summary .save_monthly_sectors_summary()


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

        This function is used to download the relationship between sectors, eco zone and gvkey.

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
        print("\n########### Mapping eco zone and sectors ###########")
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
        print("\n########### save Mapping eco zone and sectors in mongo DB. ###########")
        db = client_db[SECTORS_MARKET_DATA_DB_NAME][self._by][SECTORS_MARKET_DATA_MAPPING_DB_COL_NAME]
        tornado.ioloop.IOLoop.current().run_sync(
            DataFromMongoDB(db, data.loc[:, 'to_save'].values.tolist()).set_data_in_db)
        print("\n########### Mapping successfully saved in mongo DB. ###########")

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
    # Merge stocks summary with sector and eco zone.
    #
    ###################################################################################################################
    def get_stocks_summary_with_sector_and_eco_zone(self, start_date: date, end_date: date, query_sector_mapping: dict,
                                                    to_display: dict) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to merge stocks summary with eco zone and sector

        Parameter:
        ----------

        :param start_date: start date of the stocks summary
        :param end_date: end date of stocks summary
        :param query_sector_mapping: sector and zone to query from the mongo DB.
        :param to_display: data to display from the query

        Usage:
        -----
        data = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).
        get_stocks_summary_with_sector_and_eco_zone(start_date=date(2017, 1, 1), end_date=date(2017, 12, 31),
                                                    query_sector_mapping={'eco zone': 'USD', 'level': '2'},
                                                    to_display=None)
        data.head(15)

               USD_to_curr   gvkey  adj_factor  ...   eco zone  sector level
        0              1.0  001225         1.0  ...        USD     221     2
        1              1.0  001225         1.0  ...        USD     221     2
        2              1.0  001225         1.0  ...        USD     221     2
        3              1.0  001225         1.0  ...        USD     221     2
        4              1.0  001225         1.0  ...        USD     221     2
        5              1.0  001225         1.0  ...        USD     221     2
        6              1.0  001225         1.0  ...        USD     221     2
        7              1.0  001225         1.0  ...        USD     221     2
        8              1.0  001225         1.0  ...        USD     221     2
        9              1.0  001254         1.0  ...        USD     483     2
        10             1.0  001254         1.0  ...        USD     483     2
        11             1.0  001254         1.0  ...        USD     483     2
        12             1.0  001254         1.0  ...        USD     483     2
        13             1.0  001254         1.0  ...        USD     483     2
        14             1.0  001254         1.0  ...        USD     483     2
        ...

        Return:
        -------
        :return: DataFrame of the stocks summary with the eco zone and sector
        :rtype: pd.DataFrame
        """

        print("\n########### Downloading sector and eco zone mapping from mongo DB. ###########")
        mapping = self.get_sectors_mapping_from_db(query=query_sector_mapping, to_display=to_display)

        print("\n########### Downloading monthly stocks summary from mongo DB. ###########")
        m_summary = Stocks(self._connection_string).get_monthly_summary_from_mongodb(start_date, end_date, {}, None)
        m_summary.reset_index(drop=True, inplace=True)

        # map summary data and mapping.
        print("\n########### Join summary data and sector. ###########")
        m_summary = pd.merge(m_summary, mapping[['gvkey', 'eco zone', 'sector', 'level']], on=['gvkey'])

        return m_summary

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
            ptvar = (group['ptvar'] * group['nptvar']).sum() / group['nptvar'].sum()
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
        Description:
        ------------
        This function is used to download the stocks summary and aggregate that at the sectors level.

        Parameter:
        ----------

        :param start_date: begin date
        :param end_date: end date

        Usage:
        -----
        m_summary = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).download_monthly_sectors_summary(
                    date(2017, 1, 1), date(2017, 12, 31))

        m_summary.head(15)

                       mc         vol    npt  ...    sector                date  level
        0   1.968753e+09         0.0   20.0  ...        11 2017-12-31 16:00:00      1
        1   1.013969e+09         0.0   12.0  ...       111 2017-12-31 16:00:00      2
        2   6.009197e+07         0.0    3.0  ...       112 2017-12-31 16:00:00      2
        3   9.498733e+07         0.0    0.0  ...       113 2017-12-31 16:00:00      2
        4   9.012113e+06         0.0    0.0  ...       114 2017-12-31 16:00:00      2
        5   7.906921e+08         0.0    5.0  ...       115 2017-12-31 16:00:00      2
        6   3.854586e+11  55423664.0  516.0  ...        21 2017-12-31 16:00:00      1
        7   3.830703e+10         0.0   59.0  ...       211 2017-12-31 16:00:00      2
        8   3.425986e+11  55423664.0  441.0  ...       212 2017-12-31 16:00:00      2
        9   4.545411e+09         0.0   16.0  ...       213 2017-12-31 16:00:00      2
        10  2.371846e+10         0.0   43.0  ...        22 2017-12-31 16:00:00      1
        11  2.371846e+10         0.0   43.0  ...       221 2017-12-31 16:00:00      2
        12  5.680426e+09         0.0   31.0  ...        23 2017-12-31 16:00:00      1
        13  3.948956e+08         0.0    3.0  ...       236 2017-12-31 16:00:00      2
        14  4.003965e+09         0.0   21.0  ...       237 2017-12-31 16:00:00      2
        ...

        Return:
        -------

        :return: DataFrame of sectors summary information.
        :rtype: pd.DataFrame.
        """

        print("\n########### Downloading sector and eco zone mapping from mongo DB. ###########")
        mapping = self.get_sectors_mapping_from_db()

        print("\n########### Downloading monthly stocks summary from mongo DB. ###########")
        m_summary = Stocks(self._connection_string).get_monthly_summary_from_mongodb(start_date, end_date, {}, None)
        m_summary.reset_index(drop=True, inplace=True)

        # path = 'C:/Users/Ghislain/Google Drive/BlackFire Capital/Data/data clean.npy'
        # df = np.load(path).item()
        # m_summary = pd.DataFrame(df['data'], columns=df['header'])
        # m_summary.loc[m_summary['pt_return'] == 0, 'pt_return'] = None
        # m_summary.rename(columns={'isin': 'isin_or_cusip', 'return': 'ret', 'pt_return': 'pt_ret'}, inplace=True)
        # m_summary.reset_index(drop=True, inplace=True)

        # shift market cap
        print("\n########### shifting market cap for 1 month. ###########")
        group = m_summary[['date', 'isin_or_cusip', 'mc']].groupby(['isin_or_cusip'])
        tab_parameter = [(data,) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._shift_mc)
        result.rename(columns={'mc': 'mc_s'}, inplace=True)
        m_summary = pd.merge(m_summary, result, left_on=m_summary.index, right_on=result.index, how='inner')

        # map summary data and mapping.
        print("\n########### Join summary data and sector. ###########")
        m_summary = pd.merge(m_summary, mapping[['gvkey', 'eco zone', 'sector']], on=['gvkey'])

        # calculate sector summary
        print("\n########### Compute sector summary ###########")
        header = ['date', 'sector', 'eco zone', 'mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrec', 'nrcvar',
                  'pt_ret', 'pptvar', 'ptvar', 'ret', 'rcvar', 'rec', 'mc_s']
        sector_m_summary = []
        for zone in mapping['eco zone'].unique():
            data = m_summary[m_summary['eco zone'] == zone]
            group = data[header].groupby(['eco zone', 'sector', 'date'])
            tab_parameter = [(name, data) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._compute_sector_summary)
            result.dropna(subset=['ret'], inplace=True)
            sector_m_summary.append(result)
            print('Done for economics zone: {}'.format(zone))

        sector_m_summary = pd.concat(sector_m_summary, ignore_index=True)
        sector_m_summary = pd.merge(left=sector_m_summary,
                                    right=mapping.drop_duplicates(subset=['sector'])[['sector', 'level']],
                                    on=['sector'])
        sector_m_summary.loc[sector_m_summary['pt_ret'] == 0, 'pt_ret'] = None

        return sector_m_summary

    def save_monthly_sectors_summary(self, start_date: date, end_date: date) -> pd.DataFrame:

        """
        Description:
        -----------

        This function is used to save sectors summary in the mongo DB.

        Parameter:
        ----------

        :param start_date: begin date
        :param end_date: end date

        Usage:
        -----
        Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).save_monthly_sectors_summary(date(2017, 11, 1),
         date(2017, 12, 31))

         Return:
         -------
         :return None
        """

        def save_to_mongodb(group, by):
            """
            This function is used to save the stocks price into the mongo db
            :param group: group of stocks to save for a particular month
            :return: None
            """
            month = group.name
            db = client_db[SECTORS_MARKET_DATA_DB_NAME][by][M_SUMMARY_DB_COL_NAME][str(month)]
            tornado.ioloop.IOLoop.current().run_sync(
                DataFromMongoDB(db, group.loc[:, 'to_save'].values.tolist()).set_data_in_db)

        def stack_summary(mc, vol, npt, npptvar, nptvar, nrec, nrcvar, ret, pt_ret, mpt_ret, pptvar, ptvar, rec, rcvar,
                          eco_zone, sector, month, level):

            return InsertOne({'date': month, '_id': eco_zone + '_' + sector, 'vol': vol, 'ret': ret, 'mc': mc,
                              'level': level, 'eco zone': eco_zone, 'sector': sector,
                              TYPE_CONSENSUS: {'nrec': nrec, 'nrcvar': nrcvar, 'rcvar': rcvar, 'rec': rec},
                              TYPE_PRICE_TARGET: {'npt': npt, 'npptvar': npptvar, 'nptvar': nptvar, 'pt_ret': pt_ret,
                                                  'mpt_ret': mpt_ret, 'pptvar': pptvar, 'ptvar': ptvar}})

        monthly_summary = self.download_monthly_sectors_summary(start_date, end_date)
        monthly_summary.loc[:, 'date_m'] = monthly_summary.loc[:, 'date'].dt.strftime('%Y-%m')

        header = ['mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrec', 'nrcvar', 'ret', 'pt_ret', 'mpt_ret', 'pptvar',
                  'ptvar', 'rec', 'rcvar', 'eco zone', 'sector', 'date', 'level']

        monthly_summary.loc[:, 'to_save'] = monthly_summary.apply(lambda x: stack_summary(*x[header]), axis=1)
        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        monthly_summary.groupby('date_m').apply(save_to_mongodb, self._by)

    def _get_monthly_sectors_summary(self, month: date,  query: dict, to_display: dict):

        """
        Description:
        ------------

        This function is used to download sectors summary information for one particular month.

        Parameter:
        ----------

        :param month: date where to query the information
        :param query: information to query from the DB
        :param to_display: information to display from the query.

        Usage:
        -----
        data = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING)._get_monthly_sectors_summary(month='2017-01',
                query={'eco zone': 'USD'}, to_display=None)
        data.head(15)

                _id                date eco zone    ...       pptvar    pt_ret     ptvar
        0   USD_211 2017-01-31 16:00:00      USD    ...     0.082008  0.169854  0.083369
        1   USD_213 2017-01-31 16:00:00      USD    ...     0.113839  0.087910  0.113839
        2   USD_221 2017-01-31 16:00:00      USD    ...     0.031461  0.043345  0.031160
        3   USD_236 2017-01-31 16:00:00      USD    ...     0.049721  0.092407  0.049721
        4   USD_237 2017-01-31 16:00:00      USD    ...     0.103255  0.139774  0.103255
        5   USD_238 2017-01-31 16:00:00      USD    ...     0.171608  0.006382  0.171608
        6   USD_311 2017-01-31 16:00:00      USD    ...     0.016779  0.080997  0.016779
        7   USD_312 2017-01-31 16:00:00      USD    ...     0.011832  0.053955  0.011832
        8   USD_325 2017-01-31 16:00:00      USD    ...    -0.039207  0.142147 -0.038148
        9   USD_327 2017-01-31 16:00:00      USD    ...     0.068551  0.050851  0.068551
        10  USD_336 2017-01-31 16:00:00      USD    ...     0.070346  0.068410  0.068153
        11  USD_445 2017-01-31 16:00:00      USD    ...    -0.063699  0.067507 -0.063699
        12  USD_448 2017-01-31 16:00:00      USD    ...    -0.003185  0.461937 -0.001205
        13  USD_481 2017-01-31 16:00:00      USD    ...     0.117670  0.186284  0.117670
        14  USD_483 2017-01-31 16:00:00      USD    ...    -0.115521 -0.009296 -0.115521
        ....

        Return:
        -------

        :return: DataFrame with the sectors information for the month
        :rtype pd.DataFrame
        """

        client_db = motor.motor_tornado.MotorClient(self._connection_string)
        db = client_db[SECTORS_MARKET_DATA_DB_NAME][self._by][M_SUMMARY_DB_COL_NAME][str(month)]
        data = tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, query, to_display).get_data_from_db)

        return data

    def get_monthly_sectors_summary(self, start_date: date, end_date: date, query: dict, to_display: dict) \
            -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to get the sectors summary information from the mongo DB.

        Parameter:
        ----------

        :param start_date: begin date
        :param end_date: end date
        :param query: dict of data to query
        :param to_display: data to display from the query

        Usage:
        -----
        data = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).get_monthly_sectors_summary(
                start_date=date(2017, 1, 1), end_date=date(2017, 12, 31), query={'eco zone': 'USD'}, to_display=None)
        data.head(15)

                _id                date eco zone    ...       pptvar    pt_ret     ptvar
        0   USD_211 2017-01-31 16:00:00      USD    ...     0.082008  0.169854  0.083369
        1   USD_213 2017-01-31 16:00:00      USD    ...     0.113839  0.087910  0.113839
        2   USD_221 2017-01-31 16:00:00      USD    ...     0.031461  0.043345  0.031160
        3   USD_236 2017-01-31 16:00:00      USD    ...     0.049721  0.092407  0.049721
        4   USD_237 2017-01-31 16:00:00      USD    ...     0.103255  0.139774  0.103255
        5   USD_238 2017-01-31 16:00:00      USD    ...     0.171608  0.006382  0.171608
        6   USD_311 2017-01-31 16:00:00      USD    ...     0.016779  0.080997  0.016779
        7   USD_312 2017-01-31 16:00:00      USD    ...     0.011832  0.053955  0.011832
        8   USD_325 2017-01-31 16:00:00      USD    ...    -0.039207  0.142147 -0.038148
        9   USD_327 2017-01-31 16:00:00      USD    ...     0.068551  0.050851  0.068551
        10  USD_336 2017-01-31 16:00:00      USD    ...     0.070346  0.068410  0.068153
        11  USD_445 2017-01-31 16:00:00      USD    ...    -0.063699  0.067507 -0.063699
        12  USD_448 2017-01-31 16:00:00      USD    ...    -0.003185  0.461937 -0.001205
        13  USD_481 2017-01-31 16:00:00      USD    ...     0.117670  0.186284  0.117670
        14  USD_483 2017-01-31 16:00:00      USD    ...    -0.115521 -0.009296 -0.115521
        ....

        Return:
        -------

        :return: DataFrame with the sectors information for the month
        :rtype pd.DataFrame
        """

        # Create datetime range between start and end date.
        date_tab = pd.date_range(start_date, end_date, freq='MS').strftime('%Y-%m').tolist()
        tab_parameter = [(my_date, query, to_display,) for my_date in date_tab]

        # Download Data using multiprocessing.
        summary = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._get_monthly_sectors_summary)

        # Unstack _id, price target and consensus.
        start = time.time()
        pt = pd.DataFrame(summary[TYPE_PRICE_TARGET].tolist(), index=summary.index)
        cs = pd.DataFrame(summary[TYPE_CONSENSUS].tolist(), index=summary.index)

        # Merge unstack data with summary table
        other = [cs, pt]
        summary.drop([TYPE_PRICE_TARGET, TYPE_CONSENSUS], axis=1, inplace=True)
        summary = pd.concat([summary] + other, axis=1)

        print("\nUnstack Price Target and Consensus in {:.1f}s".format(time.time() - start))
        print("\nDownload completed.\n")

        return summary


if __name__ == '__main__':

    # Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).save_sectors_mapping_in_mongodb()
    print(Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING).
          get_stocks_summary_with_sector_and_eco_zone(start_date=date(2017, 1, 1), end_date=date(2017, 12, 31),
                                                      query_sector_mapping={'eco zone': 'USD', 'level': '2'},
                                                      to_display=None))
