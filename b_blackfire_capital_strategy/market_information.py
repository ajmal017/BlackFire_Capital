from a_blackfire_capital_class.useful_class import CustomMultiprocessing

__author__ = 'pougomg'
import numpy as np
import pandas as pd
from datetime import date
from a_blackfire_capital_class.sectors import Sectors
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import NAICS, SECTORS_MARKET_DATA_DB_NAME
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING, PROD_CONNECTION_STRING


class MarketInformation:

    def __init__(self, data: pd.DataFrame, data_source: str, signal: str, consider_history: bool, **kwargs):

        self._data_source = data_source

        if data_source == SECTORS_MARKET_DATA_DB_NAME:
            data = data.sort_values(['eco zone', 'sector', 'date'],
                                    ascending=[True, True, True])\
                .reset_index(drop=True)
            data.loc[:, 'date'] = data.loc[:, 'date'].dt.strftime('%Y-%m-%d')
            data['date'] = pd.DatetimeIndex(data['date'])
            data = data[['date', 'eco zone', 'sector', signal]]

        self._data = data
        self._signal = signal
        self._consider_history = consider_history
        self._percentile = kwargs.get('percentile', [i for i in np.linspace(0,1,11)])

    @staticmethod
    def _apply_z_score(eco_zone, sector, group):

        def z_score(group):
            """
                 This fucntion is used to compute the z-score of an array input.

                 :param group: array of the data we want to compute the
            """

            return (group[-1] - group.mean()) / group.std()

        value = group.resample('1M').bfill(limit=1)
        value = value.rolling(12, min_periods=9).apply(z_score, raw=True)
        value['eco zone'] = eco_zone
        value['sector'] = sector


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

            result = self._data.set_index('date')
            group = result.groupby(['eco zone', 'sector'])
            tab_parameter = [(name[0], name[1], data[[self._signal]],) for name, data in group]
            result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_z_score)

            result.rename(columns={self._signal: 'signal'}, inplace=True)
            result.reset_index(inplace=True)
            result.dropna(subset=['signal'], inplace=True)

            data = pd.merge(self._data, result, on=['date', 'eco zone', 'sector'])

        else:
            self._data['signal'] = self._data[self._signal]
            data = self._data
            data.dropna(subset=['signal'], inplace=True)

        group = data.groupby(group_by)
        tab_parameter = [(data, 'signal', self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._apply_ranking)

        result.drop(['signal'], axis=1, inplace=True)
        result.rename(columns={'ranking_signal': 'signal'}, inplace=True)


        return result



    def get_signal_for_strategy(self, strategy):

        if self._data_source == SECTORS_MARKET_DATA_DB_NAME:

            return self._get_sector_strategy(strategy)





if __name__ == '__main__':

    sector = np.load('usa_summary_sectors.npy').item()
    sector = pd.DataFrame(sector['data'], columns=sector['header'])
    print(sector.columns)

    # stocks = np.load('usa_summary_stocks.npy').item()
    # stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])
    # print(stocks.columns)

    MarketInformation(sector, SECTORS_MARKET_DATA_DB_NAME, 'pt_ret', False).get_signal_for_strategy(1)