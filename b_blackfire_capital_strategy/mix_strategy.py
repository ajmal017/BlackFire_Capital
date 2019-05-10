import numpy as np
import pandas as pd
import wrds

from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.useful_class import MiscellaneousFunctions
from b_blackfire_capital_strategy.input_output import IOStrategy
from b_blackfire_capital_strategy.market_information import MarketInformation
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import IO_SUPPLY, IO_DEMAND, STOCKS_MARKET_DATA_DB_NAME

if __name__ == '__main__':
    # path = 'C:/Users/Ghislain/Google Drive/BlackFire Capital/Data/'
    path = ''
    stocks = np.load(path + 'S&P Global ALL.npy').item()
    stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])
    # stocks.loc[:, 'eco zone'] = 'ALL'
    # stocks = stocks[stocks['eco zone'].isin(['USD'])]

    io_portfolio = IOStrategy(data=stocks, by=IO_SUPPLY, signal='eq_ret', consider_history=False,
                              percentile=[i for i in np.linspace(0, 1, 11)]).get_wiod_strategy_signal()

    # Merge with custom group
    custom_sector = MiscellaneousFunctions().get_custom_group_for_io()
    custom_sector.drop_duplicates(subset=['sector'], inplace=True)
    stocks = pd.merge(stocks, custom_sector[['sector', 'group']], on='sector')
    stocks.drop(['sector'], axis=1, inplace=True)
    stocks.rename(columns={'group': 'sector'}, inplace=True)

    d = {1: ['date', 'eco zone', 'sector'], 2: ['date', 'sector'], 3: ['date']}
    mrkt_info_portfolio = MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, 'pt_ret', True).get_signal_for_strategy(
        d[2])
    mrkt_info_portfolio.rename(columns={'signal': 'signal 2'}, inplace=True)
    print(io_portfolio.columns)
    print(io_portfolio.head(10))
    print(mrkt_info_portfolio.columns)
    print(mrkt_info_portfolio.head(10))
    portfolio = pd.merge(io_portfolio[['date', 'eco zone', 'sector', 'signal']],
                         mrkt_info_portfolio[['date', 'eco zone', 'sector', 'isin_or_cusip', 'mc', 'ret', 'signal 2']],
                         on=['date', 'eco zone', 'sector'])

    # portfolio.to_excel('portfolio.xlsx')
    # io_portfolio.to_excel('io.xlsx')
    # mrkt_info_portfolio.to_excel('mrkt.xlsx')
    portfolio.rename(columns={'isin_or_cusip': 'constituent', 'ret': 'return'}, inplace=True)
    portfolio.loc[:, 'position'] = None
    portfolio.loc[:, 'group'] = 'ALL'
    portfolio.loc[(portfolio['signal'].astype(int).isin([10])) & (portfolio['signal 2'].astype(int).isin([10])),
                  'position'] = 'l'
    portfolio.loc[(portfolio['signal'].astype(int).isin([1])) & (portfolio['signal 2'].astype(int).isin([1])),
                  'position'] = 's'

    portfolio.dropna(subset=['position'], inplace=True)

    print(portfolio.groupby('date')[['constituent']].count().mean())
    portfolio.set_index('date', inplace=True)

    # Compute S&P 500 return
    db = wrds.Connection()
    benchmark = db.raw_sql("SELECT datadate, prccm FROM compd.idx_mth WHERE gvkeyx = '000003'")
    benchmark['date'] = pd.DatetimeIndex(benchmark['datadate']) + pd.DateOffset(0)
    benchmark.set_index('date', inplace=True)
    benchmark['benchmark'] = benchmark['prccm'].pct_change(periods=1, freq='M')
    db.close()

    header = ['group', 'constituent', 'return', 'mc', 'position']
    stat = DisplaySheetStatistics(portfolio[header], 'GLobal IO_SUPPLY WLD S&P ALL', '', benchmark=benchmark[['benchmark']])
    stat.plot_results()
