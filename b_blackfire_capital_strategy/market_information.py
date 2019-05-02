from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.useful_class import CustomMultiprocessing, MiscellaneousFunctions

__author__ = 'pougomg'
import wrds
import numpy as np
import pandas as pd
from datetime import date, datetime
from a_blackfire_capital_class.sectors import Sectors
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import NAICS, SECTORS_MARKET_DATA_DB_NAME, \
    STOCKS_MARKET_DATA_DB_NAME
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING, PROD_CONNECTION_STRING


class MarketInformation:

    def __init__(self, data: pd.DataFrame, data_source: str, signal: str, consider_history: bool, **kwargs):
        """
        Definition:
        -----------
        This class is used to compute the strategy on the market information. We rank the information in 10 different
        clusters and define the clusters to buy and to short.

        Parameter:
        ----------

        :param data: dataFrame of stocks price.
        :param data_source: sector or stocks. Define if we want to apply the strategy by sector or by stocks.
        :param signal: variable to test
        :param consider_history: True/false. If True we apply the Z-score for each variable before ranking.
        :param kwargs:
        1) _percentile: default = [1,2,3,4,5,6,7,8,9,10] define the cluster to apply for the signal.
        2) _index_filter: default: S&P Global 1200 list of index gvkeyx defined by COMPUSTAT. filter
        the stocks to be part of an index.
        3) _stock_exchange_filter: default: None list of stock exchange we want the stocks to be part.

        Usage:
        -----

        """
        self._data_source = data_source

        if data_source == SECTORS_MARKET_DATA_DB_NAME:
            data = data.sort_values(['eco zone', 'sector', 'date'], ascending=[True, True, True])\
                .reset_index(drop=True)
            data['date'] = pd.DatetimeIndex(data['date'].dt.strftime('%Y-%m-%d')) + pd.DateOffset(0)
            data = data[['date', 'eco zone', 'sector', signal, 'ret', 'mc']]
            data.rename(columns={'ret': 'return'}, inplace=True)

        elif data_source == STOCKS_MARKET_DATA_DB_NAME:
            data = data.sort_values(['eco zone', 'sector', 'gvkey', 'date'], ascending=[True, True, True, True])\
                .reset_index(drop=True)
            data['date'] = pd.DatetimeIndex(data['date'].dt.strftime('%Y-%m-%d'))
            data = data[['date', 'eco zone', 'sector', 'gvkey', 'isin_or_cusip', signal, 'ret', 'mc']]
            data.rename(columns={'ret': 'return'}, inplace=True)

        self._data = data
        # self._benchmark = data[['date', 'mc',]]
        self._signal = signal
        self._consider_history = consider_history
        self._percentile = kwargs.get('percentile', [i for i in np.linspace(0, 1, 11)])

    @staticmethod
    def _apply_z_score(identification: dict, data: pd.DataFrame) -> pd.DataFrame:

        """
        Description:
        -----------

        This function is used to compute the 12 month z-score.

        Parameter:
        ----------

        :param identification: dict with key: ('eco zone', 'sector', 'gvkey')
        :param data: DataFrame

        Return:
        ------
        :return:
        """

        def z_score(group):
            """
                 This function is used to compute the z-score of an array input.

                 :param group: array of the data we want to compute the
            """

            return (group[-1] - group.mean()) / group.std()

        value = data.resample('1M').bfill(limit=1)
        value = value.rolling(12, min_periods=9).apply(z_score, raw=True)

        value['eco zone'] = identification.get('eco zone', None)
        value['sector'] = identification.get('sector', None)
        value['isin_or_cusip'] = identification.get('isin_or_cusip', None)

        return value

    @staticmethod
    def _apply_ranking(group: pd.DataFrame, by: str, percentile: list) -> pd.DataFrame:

        """"
        Description:
        ------------
        
        This function take a DataFrame as input and return a columns with a ranking from percentile range
        given the feature.

        :param
        group: DataFrame containing the values to rank
        by:  Name of the column to rank

        :return
        DataFrame containing one column ranking with the features ranks.

        """""

        labels = [str(i + 1) for i in range(len(percentile) - 1)]
        tab_group = group[[by]].quantile(np.array(percentile), numeric_only=False)
        group = group.fillna(np.nan)

        tab_group['labels'] = ['0'] + labels
        x = tab_group[[by, 'labels']].drop_duplicates([by])
        labels = list(x['labels'])
        labels.remove('0')
        group['ranking_' + by] = pd.cut(group[by], x[by], labels=labels).\
            values.add_categories('0').fillna('0')

        return group

    def _get_sector_strategy(self, strategy):

        if strategy == 1:
            group_by = ['eco zone', 'date']
        elif strategy == 2:
            group_by = ['sectors', 'date']
        elif strategy == 3:
            group_by = ['date']
        else:
            error_description = "The value of strategy entered is incorrect. It must be between 1 to 3. " \
                                "\n 1 is used to choose the best sector in each economics zones.\n 2 is " \
                                "used to choose the best eco zone for each sector.\n 3 is used to choose " \
                                "the best sectors in all the eco zone together."
            raise ValueError (error_description)

        if self._consider_history:

            result = self._data[['date', 'eco zone', 'sector', self._signal]].set_index('date')
            group = result.groupby(['eco zone', 'sector'])
            tab_parameter = [(name[0], name[1], data[[self._signal]],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_z_score)

            result.rename(columns={self._signal: 'signal'}, inplace=True)
            result.reset_index(inplace=True)
            result.dropna(subset=['signal'], inplace=True)

            data = pd.merge(self._data[['date', 'eco zone', 'sector', self._signal]],
                            result,
                            on=['date', 'eco zone', 'sector'])

        else:
            data = self._data[['date', 'eco zone', 'sector', self._signal]].copy()
            data['signal'] = data[self._signal]
            data.dropna(subset=['signal'], inplace=True)

        group = data.groupby(group_by)
        tab_parameter = [(data, 'signal', self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_ranking)

        result.drop(['signal', self._signal], axis=1, inplace=True)
        result.rename(columns={'ranking_signal': 'signal'}, inplace=True)

        return result

    def _get_stocks_strategy(self, strategy):

        d_strategy = {1: ['eco zone', 'sector', 'date'], 2: ['eco zone', 'date'], 3: ['sector', 'date'],
                      4: ['date']}
        if strategy not in d_strategy:
            error_description = "The value of strategy entered is incorrect. It must be between 1 to 4. " \
                                "\n 1 is used to choose the best sector in each economics zones.\n 2 is " \
                                "used to choose the best eco zone for each sector.\n 3 is used to choose " \
                                "the best sectors in all the eco zone together."
            raise ValueError(error_description)

        if self._consider_history:

            result = self._data[['date', 'eco zone', 'sector', 'isin_or_cusip', self._signal]].set_index('date')
            group = result.groupby(['eco zone', 'sector', 'isin_or_cusip'])
            tab_parameter = [({'eco zone': name[0], 'sector': name[1], 'isin_or_cusip': name[2]},
                              data[[self._signal]],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_z_score)

            result.rename(columns={self._signal: 'signal'}, inplace=True)
            result.reset_index(inplace=True)
            result.dropna(subset=['signal'], inplace=True)

            data = pd.merge(self._data[['date', 'eco zone', 'sector', 'isin_or_cusip', self._signal]],
                            result,
                            on=['date', 'eco zone', 'sector', 'isin_or_cusip'])

        else:
            data = self._data[['date', 'eco zone', 'sector', 'isin_or_cusip', self._signal]].copy()
            data['signal'] = data[self._signal]
            data.dropna(subset=['signal'], inplace=True)

        # group = data.groupby(d_strategy[strategy])
        group = data.groupby(['sector', 'date'])
        tab_parameter = [(data, 'signal', self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_ranking)

        result.drop(['signal', self._signal], axis=1, inplace=True)
        result.rename(columns={'ranking_signal': 'signal'}, inplace=True)

        return result

    def get_signal_for_strategy(self, strategy):

        if self._data_source == SECTORS_MARKET_DATA_DB_NAME:

            return self._get_sector_strategy(strategy)

        elif self._data_source == STOCKS_MARKET_DATA_DB_NAME:

            return self._get_stocks_strategy(strategy)

    def get_strategy_statistics(self, long_position, short_position, **kwargs):

        if self._data_source == SECTORS_MARKET_DATA_DB_NAME:

            eco_zone = kwargs.get('eco_zone', None)
            sector = kwargs.get('sector', None)

            if eco_zone is not None and sector is None:
                self._data = self._data[self._data['eco zone'] == eco_zone]
                signal = self.get_signal_for_strategy(1)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = signal['eco zone']
                signal['constituent'] = signal['sector']

                title = eco_zone
                description = "The result pres"

            elif eco_zone is None and sector is not None:
                self._data = self._data[self._data['sector'] == sector]
                signal = self.get_signal_for_strategy(2)

            elif eco_zone is None and sector is None:
                signal = self.get_signal_for_strategy(3)

            else:
                raise ValueError()

            portfolio = pd.merge(self._data, signal, on=['date', 'eco zone', 'sector'])

        elif self._data_source == STOCKS_MARKET_DATA_DB_NAME:

            eco_zone = kwargs.get('eco_zone', None)
            sector = kwargs.get('sector', None)

            if eco_zone is not None and sector is not None:
                self._data = self._data[(self._data['eco zone'] == eco_zone) &
                                        (self._data['sector'] == sector)]
                signal = self.get_signal_for_strategy(1)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = signal['sector']
                signal['constituent'] = signal['isin_or_cusip']

            elif eco_zone is not None and sector is None:
                self._data = self._data[self._data['eco zone'] == eco_zone]
                signal = self.get_signal_for_strategy(2)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = signal['eco zone']
                signal['constituent'] = signal['isin_or_cusip']

            elif eco_zone is None and sector is not None:
                self._data = self._data[self._data['sector'] == sector]
                signal = self.get_signal_for_strategy(3)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = signal['sector']
                signal['constituent'] = signal['isin_or_cusip']

            else:
                signal = self.get_signal_for_strategy(4)
                # signal['date'] = signal.set_index('date').shift(periods=1, freq='M').index
                signal['group'] = 'ALL'
                signal['constituent'] = signal['isin_or_cusip']

            portfolio = pd.merge(self._data, signal, on=['date', 'eco zone', 'sector', 'isin_or_cusip'])
            value = portfolio.set_index('date').groupby('isin_or_cusip')[['mc', 'signal']].shift(periods=1, freq='M').reset_index()
            # portfolio.to_excel('test.xlsx')

            portfolio.drop(['mc', 'signal'], axis=1, inplace=True)
            portfolio = pd.merge(portfolio,
                                 value,
                                 on=['date', 'isin_or_cusip'])
            # portfolio.to_excel('test_.xlsx')
            # print(a.shape)
        # return
        # print(portfolio['date'])
        portfolio.loc[:, 'position'] = None
        portfolio.loc[portfolio['signal'].astype(int).isin(long_position), 'position'] = 'l'
        portfolio.loc[portfolio['signal'].astype(int).isin(short_position), 'position'] = 's'

        portfolio.dropna(subset=['position'], inplace=True)
        portfolio.set_index('date', inplace=True)

        header = ['group', 'constituent', 'return', 'mc', 'position']
        stat = DisplaySheetStatistics(portfolio[header], 'USD', '')
        stat.plot_results()


if __name__ == '__main__':

    # sector = np.load('usa_summary_sectors.npy').item()
    # sector = pd.DataFrame(sector['data'], columns=sector['header'])
    # print(sector.columns)
    # path = 'C:/Users/Ghislain/Google Drive/BlackFire Capital/Data/'
    path = ''

    stocks = np.load(path + 'S&P Global ALL.npy').item()
    stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])

    # custom_sector = MiscellaneousFunctions().get_custom_group_for_io()

    # stocks = pd.merge(stocks, custom_sector[['sector', 'group']], on='sector')
    # print(stocks.columns)
    # MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, 'pt_ret', True,
    #                   index_filter=['031855'])._get_stocks_strategy(1)

    index_filter = ['031855', '150927', '151015', '000010', '153376', '151012', '150915', '150916']
    percentile = [i for i in np.linspace(0, 1, 21)]
    MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, 'pt_ret', True, percentile=percentile). \
        get_strategy_statistics(long_position=[20], short_position=[None], eco_zone=None,
                                sector=None)
