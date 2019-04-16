from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.useful_class import CustomMultiprocessing

__author__ = 'pougomg'
import wrds
import numpy as np
import pandas as pd
from datetime import date
from a_blackfire_capital_class.sectors import Sectors
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import NAICS, SECTORS_MARKET_DATA_DB_NAME, \
    STOCKS_MARKET_DATA_DB_NAME
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING, PROD_CONNECTION_STRING


class MarketInformation:

    def __init__(self, data: pd.DataFrame, data_source: str, signal: str, consider_history: bool, **kwargs):

        self._data_source = data_source

        if data_source == SECTORS_MARKET_DATA_DB_NAME:
            data = data.sort_values(['eco zone', 'sector', 'date'], ascending=[True, True, True])\
                .reset_index(drop=True)
            data['date'] = pd.DatetimeIndex(data['date'].dt.strftime('%Y-%m-%d'))
            data = data[['date', 'eco zone', 'sector', signal, 'ret', 'mc']]
            data.rename(columns={'ret': 'return'}, inplace=True)

        elif data_source == STOCKS_MARKET_DATA_DB_NAME:
            data = data.sort_values(['eco zone', 'sector', 'gvkey', 'date'],
                                    ascending=[True, True, True, True])\
                .reset_index(drop=True)
            data['date'] = pd.DatetimeIndex(data['date'].dt.strftime('%Y-%m-%d'))
            data = data[['date', 'eco zone', 'sector','gvkey', 'isin_or_cusip', signal, 'ret', 'mc']]
            data.rename(columns={'ret': 'return'}, inplace=True)

        self._data = data
        self._signal = signal
        self._consider_history = consider_history
        self._percentile = kwargs.get('percentile', [i for i in np.linspace(0,1,11)])
        self._index_filter = kwargs.get('index_filter', None)
        self._stock_exchange_filter = kwargs.get('stock_exchange_filter', None)

    @staticmethod
    def _apply_z_score(identification, group):

        def z_score(group):
            """
                 This fucntion is used to compute the z-score of an array input.

                 :param group: array of the data we want to compute the
            """

            return (group[-1] - group.mean()) / group.std()

        value = group.resample('1M').bfill(limit=1)
        value = value.rolling(12, min_periods=9).apply(z_score, raw=True)

        value['eco zone'] = identification.get('eco zone', None)
        value['sector'] = identification.get('sector', None)
        value['gvkey'] = identification.get('gvkey', None)


        return value

    @staticmethod
    def _apply_ranking(group, by, percentile):

        """"
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

        if strategy == 1:
            group_by = ['eco zone', 'sector', 'date']
        elif strategy == 2:
            group_by = ['eco zone', 'date']
        elif strategy == 3:
            group_by = ['sector', 'date']
        elif strategy == 4:
            group_by = ['date']
        else:
            error_description = "The value of strategy entered is incorrect. It must be between 1 to 4. " \
                                "\n 1 is used to choose the best sector in each economics zones.\n 2 is " \
                                "used to choose the best eco zone for each sector.\n 3 is used to choose " \
                                "the best sectors in all the eco zone together."
            raise ValueError (error_description)

        if self._index_filter:

            db = wrds.Connection()
            index_constituent = db.raw_sql("SELECT a.*, b.cusip, b.isin from compd.idxcst_his as a "
                                           "INNER JOIN comp.security b ON (a.gvkey = b.gvkey AND a.iid = b.iid) "
                                           "WHERE a.gvkeyx IN ('031855')" )
            db.close()

            index_constituent.loc[index_constituent['thru'].isna(), 'thru'] = date.today()
            index_constituent.loc[index_constituent['cusip'].isna(), 'cusip'] = index_constituent.loc[:, 'isin']
            index_constituent['to_merge'] = index_constituent['gvkey'] + '_' + index_constituent['cusip']

            self._data['merge'] = self._data['gvkey'] + '_' + self._data['isin_or_cusip']
            self._data = pd.merge(self._data, index_constituent[['from', 'thru', 'to_merge']], on=['to_merge'])

            group = self._data.groupby(['isin_or_cusip'])


        if self._consider_history:

            result = self._data[['date', 'eco zone', 'sector', 'gvkey', self._signal]].set_index('date')
            group = result.groupby(['eco zone', 'sector', 'gvkey'])
            tab_parameter = [({'eco zone':name[0], 'sector': name[1], 'gvkey': name[2]},
                              data[[self._signal]],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_z_score)

            result.rename(columns={self._signal: 'signal'}, inplace=True)
            result.reset_index(inplace=True)
            result.dropna(subset=['signal'], inplace=True)

            data = pd.merge(self._data[['date', 'eco zone', 'sector', 'gvkey', self._signal]],
                            result,
                            on=['date', 'eco zone', 'sector', 'gvkey'])

        else:
            data = self._data[['date', 'eco zone', 'sector', 'gvkey', self._signal]].copy()
            data['signal'] = data[self._signal]
            data.dropna(subset=['signal'], inplace=True)

        group = data.groupby(group_by)
        tab_parameter = [(data, 'signal', self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_ranking)

        result.drop(['signal', self._signal], axis=1, inplace=True)
        result.rename(columns={'ranking_signal': 'signal'}, inplace=True)
        result.to_excel('test.xlsx')

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
                signal['constituent'] = signal['gvkey']

            elif eco_zone is not None and sector is None:
                self._data = self._data[self._data['eco zone'] == eco_zone]
                signal = self.get_signal_for_strategy(2)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = signal['eco zone']
                signal['constituent'] = signal['gvkey']

            elif eco_zone is None and sector is not None:
                self._data = self._data[self._data['sector'] == sector]
                signal = self.get_signal_for_strategy(3)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = signal['sector']
                signal['constituent'] = signal['gvkey']

            else:
                signal = self.get_signal_for_strategy(4)
                signal['date'] = signal['date'] + pd.DateOffset(months=1)
                signal['group'] = 'ALL'
                signal['constituent'] = signal['gvkey']

            portfolio = pd.merge(self._data, signal, on=['date', 'eco zone', 'sector', 'gvkey'])


        portfolio.loc[:, 'position'] = None
        portfolio.loc[portfolio['signal'].astype(int).isin(long_position), 'position'] = 'l'
        portfolio.loc[portfolio['signal'].astype(int).isin(short_position), 'position'] = 's'

        portfolio.dropna(subset=['position'], inplace=True)
        portfolio.set_index('date', inplace=True)
        portfolio.to_excel('test.xlsx')

        header = ['group', 'constituent', 'return', 'mc', 'position']
        stat = DisplaySheetStatistics(portfolio[header], 'USD', '')
        stat.plot_results()










if __name__ == '__main__':

    # sector = np.load('usa_summary_sectors.npy').item()
    # sector = pd.DataFrame(sector['data'], columns=sector['header'])
    # print(sector.columns)

    stocks = np.load('usa_summary_stocks.npy').item()
    stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])
    # print(stocks.columns)

    MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, 'pt_ret', True).\
        get_strategy_statistics(long_position=[10, 9, 8], short_position=[None], eco_zone='USD',
                                sector='444')