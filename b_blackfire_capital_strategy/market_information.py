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
            data.rename(columns={'ret': 'return'}, inplace=True)

        elif data_source == STOCKS_MARKET_DATA_DB_NAME:
            data = data.sort_values(['eco zone', 'sector', 'isin_or_cusip', 'date'],
                                    ascending=[True, True, True, True]).reset_index(drop=True)

        data['date'] = pd.DatetimeIndex(data['date'].dt.strftime('%Y-%m-%d'))
        self._data = data
        self._signal = signal
        self._consider_history = consider_history
        self._percentile = kwargs.get('percentile', [i for i in np.linspace(0, 1, 11)])

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

    def _get_stocks_strategy_signal(self, on: list):

        if self._consider_history:
            # if we want to use the acceleration of the signal instead of the signal itself.
            print("\n ***** Computing Z-score to get acceleration on {} *****".format(self._signal))
            result = self._data[['date', 'eco zone', 'sector', 'isin_or_cusip', self._signal]].set_index('date')
            group = result.groupby(['eco zone', 'sector', 'isin_or_cusip'])
            tab_parameter = [({'eco zone': name[0], 'sector': name[1], 'isin_or_cusip': name[2]},
                              data[[self._signal]],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_z_score)

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

        # rank the signal in percentiles.
        print("\n ***** Ranking the signal between the range [{}, {}] *****".format(
            self._percentile[1] * 10, self._percentile[-1] * 10))
        group = data.groupby(on)
        tab_parameter = [(data, 'signal', self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_ranking)
        result.drop(['signal', self._signal], axis=1, inplace=True)
        result.rename(columns={'ranking_signal': 'signal'}, inplace=True)

        result = pd.merge(self._data, result[['date', 'sector', 'eco zone', 'isin_or_cusip', 'signal']],
                          on=['date', 'sector', 'eco zone', 'isin_or_cusip'])
        # shift MC and signal to compute the return of the strategy.
        print("***** shifting MC and signal to compute the return of the strategy. *****")
        value = result.set_index('date').groupby(['eco zone', 'sector', 'isin_or_cusip'])[['mc', 'signal']].\
            shift(periods=1, freq='M').reset_index()
        result = pd.merge(self._data[['date', 'eco zone', 'sector', 'isin_or_cusip', 'ret']], value,
                          on=['date', 'sector', 'eco zone', 'isin_or_cusip'])

        return result

    def get_signal_for_strategy(self, on):

        if self._data_source == SECTORS_MARKET_DATA_DB_NAME:

            return self._get_sector_strategy(on)

        elif self._data_source == STOCKS_MARKET_DATA_DB_NAME:

            return self._get_stocks_strategy_signal(on)

    def display_sheet(self, group_signal_by: list, long_postion: list, short_position: list):

        portfolio = self.get_signal_for_strategy(group_signal_by)

        if self._data_source == STOCKS_MARKET_DATA_DB_NAME:
            portfolio.rename(columns={'isin_or_cusip': 'constituent', 'ret': 'return', 'sector': 'group'}, inplace=True)

        portfolio.loc[:, 'position'] = None
        portfolio.loc[:, 'group'] = 'ALL'
        portfolio.loc[portfolio['signal'].astype(int).isin(long_postion), 'position'] = 'l'
        portfolio.loc[portfolio['signal'].astype(int).isin(short_position), 'position'] = 's'

        portfolio.dropna(subset=['position'], inplace=True)
        print(portfolio.groupby('date')['constituent'].count())
        portfolio.set_index('date', inplace=True)

        # Compute S&P 500 return
        db = wrds.Connection()
        benchmark = db.raw_sql("SELECT datadate, prccm FROM compd.idx_mth WHERE gvkeyx = '000003'")
        benchmark['date'] = pd.DatetimeIndex(benchmark['datadate']) + pd.DateOffset(0)
        benchmark.set_index('date', inplace=True)
        benchmark['benchmark'] = benchmark['prccm'].pct_change(periods=1, freq='M')
        db.close()

        header = ['group', 'constituent', 'return', 'mc', 'position']
        stat = DisplaySheetStatistics(portfolio[header], 'USD 2', '', benchmark=benchmark[['benchmark']])
        stat.plot_results()

if __name__ == '__main__':

    # sector = np.load('usa_summary_sectors.npy').item()
    # sector = pd.DataFrame(sector['data'], columns=sector['header'])
    # print(sector.columns)
    path = 'C:/Users/Ghislain/Google Drive/BlackFire Capital/Data/'
    # path = ''

    stocks = np.load(path + 'S&P Global 1200.npy').item()
    stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])
    stocks = stocks[stocks['eco zone'] == 'USD']

    # Merge with custom group
    custom_sector = MiscellaneousFunctions().get_custom_group_for_io()
    custom_sector.drop_duplicates(subset=['sector'], inplace=True)
    stocks = pd.merge(stocks, custom_sector[['sector', 'group']], on='sector')
    stocks.drop(['sector'], axis=1, inplace=True)
    stocks.rename(columns={'group': 'sector'}, inplace=True)
    # print(stocks.columns)
    d = {1: ['date', 'eco zone', 'sector'], 2: ['date', 'sector'], 3: ['date']}
    MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, 'pt_ret', True).display_sheet(
        d[3], [10], [None])

