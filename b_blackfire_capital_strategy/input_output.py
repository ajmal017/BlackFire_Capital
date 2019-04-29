from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.sectors import Sectors
from a_blackfire_capital_class.useful_class import CustomMultiprocessing, MiscellaneousFunctions
import pandas as pd
import numpy as np
from datetime import datetime

from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import IO_SUPPLY, IO_DEMAND


class IOStrategy:

    def __init__(self, data: pd.DataFrame, by: str, signal: str, **kwargs):

        data['date'] = pd.DatetimeIndex(data['date'].dt.strftime('%Y-%m-%d')) + pd.DateOffset(0)
        self._data = data
        self._by = by
        self._signal = signal
        self._is_sector_data = kwargs.get('sector_data', False)
        self._percentile = kwargs.get('percentile', [i for i in np.linspace(0, 1, 6)])
        self._sector_data = None

    @staticmethod
    def _get_signal(group, year, signal, by):

        if by == IO_SUPPLY:
            leontief = MiscellaneousFunctions().get_leontief_matrix(int(year))
        elif by == IO_DEMAND:
            leontief = MiscellaneousFunctions().get_leontief_matrix(int(year))
            leontief = pd.DataFrame(np.linalg.pinv(leontief.values), leontief.columns, leontief.index)
            leontief.index.name = 'Code'
        else:
            raise ValueError("Incorrect Input value. By must be {} or {}".format(IO_DEMAND, IO_SUPPLY))

        s_summary = pd.merge(leontief.reset_index()[['Code']], group,
                             left_on='Code', right_on='sector', how='left')
        s_summary[signal] = s_summary[signal].fillna(0)
        result = leontief.dot(s_summary.set_index('Code')[signal]).to_frame('value')

        s_summary = pd.merge(s_summary.dropna(subset=['sector']), result, left_on='sector', right_on=result.index)
        s_summary = s_summary[['date', 'sector', 'value']]
        s_summary.rename(columns={'value': signal}, inplace=True)

        return s_summary

    def group_stocks_by_sectors(self):

        """
        Description:
        ------------
        This function is used to group the stocks using the sectors custom group of IO table

        Return:
        ------

        :return None.
        """

        if not self._is_sector_data:

            custom_group = MiscellaneousFunctions().get_custom_group_for_io()
            custom_group.drop_duplicates('sector', inplace=True)
            m_summary = pd.merge(self._data, custom_group, on='sector')
            m_summary.drop('sector', inplace=True, axis=1)
            m_summary.rename(columns={'group': 'sector'}, inplace=True)
            s_summary = Sectors().compute_monthly_sectors_summary(m_summary)
            self._sector_data = s_summary
        else:

            self._sector_data = self._data

    def get_strategy_signal(self):

        self.group_stocks_by_sectors()
        s_summary = self._sector_data[['date', 'sector', self._signal]]

        # Get result for the Leontief matrix.
        group = s_summary.groupby(['date'])
        tab_parameter = [(data, name.year, self._signal, self._by) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, self._get_signal)

        # Rank the signal by percentile
        group = result.groupby(['date'])
        tab_parameter = [(data, self._signal, self._percentile) for name, data in group]
        result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_ranking)
        result.rename(columns={'ranking_' + self._signal: 'signal'}, inplace=True)

        result = pd.merge(self._sector_data, result[['date', 'sector', 'signal']], on=['date', 'sector'])

        value = result.set_index('date').groupby('sector')[['mc', 'signal']].shift(periods=1, freq='M').reset_index()
        result = pd.merge(self._sector_data[['date', 'sector', 'ret']], value, on=['date', 'sector'])

        return result

    def display_sheet(self):

        portfolio = self.get_strategy_signal()
        portfolio.rename(columns={'sector': 'constituent', 'ret': 'return'}, inplace=True)
        portfolio.loc[:, 'position'] = None
        portfolio.loc[:, 'group'] = 'ALL'
        portfolio.loc[portfolio['signal'].astype(int).isin([5]), 'position'] = 'l'
        # portfolio.loc[portfolio['signal'].astype(int).isin([1]), 'position'] = 's'

        portfolio.dropna(subset=['position'], inplace=True)
        portfolio.set_index('date', inplace=True)

        header = ['group', 'constituent', 'return', 'mc', 'position']
        stat = DisplaySheetStatistics(portfolio[header], 'USD', '')
        stat.plot_results()


if __name__ == '__main__':

    path = 'C:/Users/Ghislain/Google Drive/BlackFire Capital/Data/'
    # path = ''

    stocks = np.load(path + 'S&P Global 1200.npy').item()
    stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])
    stocks = stocks[stocks['eco zone'] == 'USD']

    IOStrategy(data=stocks, by=IO_SUPPLY, signal='ret').display_sheet()