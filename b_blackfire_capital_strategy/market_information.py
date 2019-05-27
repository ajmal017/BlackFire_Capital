from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.useful_class import CustomMultiprocessing, MiscellaneousFunctions
from b_blackfire_capital_strategy.machine_learning import StockSelectionWithMLAlgorithm

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

    def __init__(self, data: pd.DataFrame, data_source: str, feature: list, consider_history: bool, **kwargs):
        """
        Definition:
        -----------
        This class is used to compute the strategy on the market information. We rank the information in 10 different
        clusters and define the clusters to buy and to short.

        Parameter:
        ----------

        :param data: dataFrame of stocks price.
        :param data_source: sector or stocks. Define if we want to apply the strategy by sector or by stocks.
        :param feature: variable to test
        :param consider_history: True/false. If True we apply the Z-score for each variable before ranking.
        :param kwargs:
        1) _percentile: default = [1,2,3,4,5,6,7,8,9,10] define the cluster to apply for the signal.

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
        self._feature = feature
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
            raise ValueError(error_description)

        if self._consider_history:

            result = self._data[['date', 'eco zone', 'sector', self._feature]].set_index('date')
            group = result.groupby(['eco zone', 'sector'])
            tab_parameter = [(name[0], name[1], data[[self._feature]],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_z_score)

            result.rename(columns={self._feature: 'signal'}, inplace=True)
            result.reset_index(inplace=True)
            result.dropna(subset=['signal'], inplace=True)

            data = pd.merge(self._data[['date', 'eco zone', 'sector', self._feature]],
                            result,
                            on=['date', 'eco zone', 'sector'])

        else:
            data = self._data[['date', 'eco zone', 'sector', self._feature]].copy()
            data['signal'] = data[self._feature]
            data.dropna(subset=['signal'], inplace=True)

        group = data.groupby(group_by)
        tab_parameter = [(data, 'signal', self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_ranking)

        result.drop(['signal', self._feature], axis=1, inplace=True)
        result.rename(columns={'ranking_signal': 'signal'}, inplace=True)

        return result

    def _get_stocks_strategy_signal(self, on: list) -> pd.DataFrame:

        """
        Description:
        ------------

        This function is used to perform the ranking of the features in one of the 3 groups:
        1- [date]: Rank all the stocks in the universes. Find for all the stocks the one with the best to the worst
                   features.
        2- [date, sector]: Rank all the stocks in the sectors. Rank all the stocks in the universes by each sectors.

        3- [date, eco zone, sector]: Find all the best stocks for each sectors in each coountry.

        Parameter:
        ----------

        :param on: list with the ranking method to use: [date], [date, sector], [date, eco zone, sector]

        Usage:
        ------

        rank_mki = MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, feature, True)
        ._get_stocks_strategy_signal(['date', 'sector'])
        rank_mki.head(15)

                     date eco zone  ... ranking_rec_acc_mki ranking_rcvar_acc_mki
        0      2002-09-30      USD  ...                   0                     0
        1      2002-10-31      USD  ...                   0                     0
        2      2002-11-30      USD  ...                   0                     0
        3      2002-12-31      USD  ...                   0                     0
        4      2003-01-31      USD  ...                   0                     0
        5      2003-02-28      USD  ...                   0                     0
        6      2003-03-31      USD  ...                   0                     0
        7      2003-04-30      USD  ...                   0                     0
        8      2003-05-31      USD  ...                   0                     0
        9      2003-06-30      USD  ...                   0                     0
        10     2003-07-31      USD  ...                   0                     0
        11     2003-08-31      USD  ...                   0                     0
        12     2003-09-30      USD  ...                   0                     0
        13     2003-10-31      USD  ...                   0                     0
        14     2003-11-30      USD  ...                   0                     0

        Return:
        -------

        :return: DataFrame with all the features rank according to the specified group
        """
        to_rank = self._feature
        data = self._data.copy()

        if self._consider_history:
            # if we want to use the acceleration of the signal instead of the signal itself.
            print("\n ***** Computing Z-score to get acceleration on {} *****".format(self._feature))
            result = data[['date', 'eco zone', 'sector', 'isin_or_cusip'] + to_rank].set_index('date')
            group = result.groupby(['eco zone', 'sector', 'isin_or_cusip'])
            tab_parameter = [({'eco zone': name[0], 'sector': name[1], 'isin_or_cusip': name[2]},
                              data[to_rank],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_z_score)
            rename = dict()
            for name in to_rank:
                rename[name] = name + '_acc'
            result.rename(columns=rename, inplace=True)
            to_rank = to_rank + (pd.Series(to_rank) + '_acc').tolist()

            result.reset_index(inplace=True)
            data = pd.merge(data, result, on=['date', 'eco zone', 'sector', 'isin_or_cusip'])

        # rank the signal in percentiles.
        print("\n ***** Ranking the signal between the range [{}, {}] *****".format(
            self._percentile[1] * 10, self._percentile[-1] * 10))

        group = data.groupby(on)
        tab_parameter = [(data, to_rank, self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_ranking)

        print("***** shifting MC and signal to compute the return of the strategy. *****")
        to_rank = ('ranking_' + pd.Series(to_rank)).tolist()
        value = result.set_index('date').groupby(['eco zone', 'sector', 'isin_or_cusip'])[['mc'] + to_rank].\
            shift(periods=1, freq='M').reset_index()
        result = pd.merge(self._data[['date', 'eco zone', 'sector', 'isin_or_cusip', 'ret']], value,
                          on=['date', 'sector', 'eco zone', 'isin_or_cusip'])
        rename = dict()
        for name in to_rank:
            rename[name] = name + '_mki'

        result.rename(columns=rename, inplace=True)

        return result

    def get_signal_for_strategy(self, on):

        if self._data_source == SECTORS_MARKET_DATA_DB_NAME:

            return self._get_sector_strategy(on)

        elif self._data_source == STOCKS_MARKET_DATA_DB_NAME:

            return self._get_stocks_strategy_signal(on)

    def display_sheet(self, group_signal_by: list, signal_to_display: str, long_postion: list, short_position: list):
        """
        Description:
        ------------

        This function is used to display the result of one strategy in a tear sheet.

        Parameter:
        ----------

        :param group_signal_by:
        :param signal_to_display:
        :param long_postion:
        :param short_position:

        Usage:
        ------

        :return:
        """
        result = self.get_signal_for_strategy(group_signal_by)

        group = result.groupby(['date'])
        tab_parameter = [(data, ['ret'], [i for i in np.linspace(0, 1, 6)]) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_ranking)
        result.to_excel('t.xlsx')

        if self._data_source == STOCKS_MARKET_DATA_DB_NAME:
            result.rename(columns={'isin_or_cusip': 'constituent', 'ret': 'return', 'sector': 'group'}, inplace=True)

        portfolio = result.copy()
        portfolio.loc[:, 'position'] = None
        portfolio.loc[:, 'group'] = 'ALL'
        portfolio['signal'] = portfolio[signal_to_display]
        portfolio.loc[portfolio['signal'].astype(int).isin(long_postion), 'position'] = 'l'
        portfolio.loc[portfolio['signal'].astype(int).isin(short_position), 'position'] = 's'

        portfolio.dropna(subset=['position'], inplace=True)
        # print(portfolio.groupby('date')['constituent'].count())
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

        feature = ('ranking_' + pd.Series(self._feature) + '_mki').tolist()
        if self._consider_history:
            feature += ('ranking_' + pd.Series(self._feature) + '_acc_mki').tolist()

        result = pd.merge(result, benchmark, left_on='date', right_index=True)
        result['ranking_ret'] = result['ranking_ret'].astype(float)
        # result.loc[result['return'] - result['benchmark'] > 0, 'ranking_ret'] = 1
        result[feature] = result[feature].astype(int)
        # print(result)
        result = StockSelectionWithMLAlgorithm(result, feature).get_signal()
        portfolio = result.copy()
        portfolio.loc[:, 'position'] = None
        portfolio.loc[:, 'group'] = 'ALL'
        portfolio.loc[portfolio['signal'].astype(int).isin([4,5]), 'position'] = 'l'
        # print(portfolio)
        # portfolio.loc[portfolio['signal'].astype(int).isin(short_position), 'position'] = 's'
        portfolio.set_index('date', inplace=True)
        portfolio.dropna(subset=['position'], inplace=True)

        stat = DisplaySheetStatistics(portfolio[header], 'USD ML', '', benchmark=benchmark[['benchmark']])
        # print(portfolio.groupby('date')['constituent'].count())
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
    print(stocks.groupby(['date'])[['isin_or_cusip']].count())

    # Merge with custom group
    custom_sector = MiscellaneousFunctions().get_custom_group_for_io()
    custom_sector.drop_duplicates(subset=['sector'], inplace=True)
    stocks = pd.merge(stocks, custom_sector[['sector', 'group']], on='sector')
    stocks.drop(['sector'], axis=1, inplace=True)
    stocks.rename(columns={'group': 'sector'}, inplace=True)
    # print(stocks.columns)
    d = {1: ['date', 'eco zone', 'sector'], 2: ['date', 'sector'], 3: ['date']}
    feature = ['pt_ret', 'ptvar', 'rec', 'rcvar']

    MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, feature, True)._get_stocks_strategy_signal(['date', 'sector'])
    # MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, feature, True).display_sheet(
    #     d[2], 'ranking_pt_ret_acc', [10], [None])

