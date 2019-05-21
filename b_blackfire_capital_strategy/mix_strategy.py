import numpy as np
import pandas as pd
import wrds

from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.useful_class import MiscellaneousFunctions, CustomMultiprocessing
from b_blackfire_capital_strategy.input_output import IOStrategy
from b_blackfire_capital_strategy.machine_learning import StockSelectionWithMLAlgorithm
from b_blackfire_capital_strategy.market_information import MarketInformation
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import IO_SUPPLY, IO_DEMAND, \
    STOCKS_MARKET_DATA_DB_NAME

if __name__ == '__main__':
    # path = 'C:/Users/Ghislain/Google Drive/BlackFire Capital/Data/'
    path = ''


    ######################################################################################################
    #
    # Ouverture des fichiers stocks
    #
    ######################################################################################################

    # Global Signal.
    wld = np.load(path + 'Global Stocks.npy').item()
    # wld = np.load(path + 'S&P Global ALL.npy').item()
    wld = pd.DataFrame(wld['data'], columns=wld['header'])
    # wld = wld[wld['eco zone'].isin(['USD'])]
    wld = wld[(wld['adj_pc'] >= 5) & (wld['mc'] >= 200000000)]

    # Stocks signal
    stocks = np.load(path + 'S&P Global ALL.npy').item()
    stocks = pd.DataFrame(stocks['data'], columns=stocks['header'])
    # stocks = stocks[stocks['eco zone'].isin(['USD'])]

    ######################################################################################################
    #
    # Supply and demand signal
    #
    ######################################################################################################

    io_sup_portfolio = IOStrategy(data=wld, by=IO_SUPPLY, signal='ret', consider_history=False,
                              percentile=[i for i in np.linspace(0, 1, 6)]).get_wiod_strategy_signal()


    io_dem_portfolio = IOStrategy(data=wld, by=IO_DEMAND, signal='ret', consider_history=False,
                              percentile=[i for i in np.linspace(0, 1, 6)]).get_wiod_strategy_signal()

    io_sup_portfolio.rename(columns={'signal': 'io supply signal'}, inplace=True)
    io_dem_portfolio.rename(columns={'signal': 'io demand signal'}, inplace=True)

    #####################################################################################################
    #
    # Stocks signal
    #
    #####################################################################################################

    # Merge with custom group
    custom_sector = MiscellaneousFunctions().get_custom_group_for_io()
    custom_sector.drop_duplicates(subset=['sector'], inplace=True)
    stocks = pd.merge(stocks, custom_sector[['sector', 'group']], on='sector')
    stocks.drop(['sector'], axis=1, inplace=True)
    stocks.rename(columns={'group': 'sector'}, inplace=True)

    d = {1: ['date', 'eco zone', 'sector'], 2: ['date', 'sector'], 3: ['date']}
    mrkt_info_portfolio = MarketInformation(stocks, STOCKS_MARKET_DATA_DB_NAME, 'pt_ret',
                                            True).get_signal_for_strategy(d[2])
    mrkt_info_portfolio.rename(columns={'signal': 'pt return signal'}, inplace=True)


    #######################################################################################################
    #
    # Merge all signal
    #
    #######################################################################################################

    result = pd.merge(io_sup_portfolio[['date', 'eco zone', 'sector', 'io supply signal']],
                         mrkt_info_portfolio[['date', 'eco zone', 'sector', 'isin_or_cusip',
                                              'mc', 'ret', 'pt return signal']],
                         on=['date', 'eco zone', 'sector'])

    result = pd.merge(io_dem_portfolio[['date', 'eco zone', 'sector', 'io demand signal']],
                         result,
                         on=['date', 'eco zone', 'sector'])


    #######################################################################################################
    #
    # Implement ML Signal
    #
    #######################################################################################################

     # Rank the signal by percentile
    # print("\n########### Rank signal by percentile ###########")
    # group = result.groupby(['date'])
    # tab_parameter = [(data, 'ret', [i for i in np.linspace(0, 1, 11)]) for name, data in group]
    # result = CustomMultiprocessing().exec_in_parallel(tab_parameter, MiscellaneousFunctions().apply_ranking)
    # result.sort_values(by=['date'], inplace=True)

    # result = StockSelectionWithMLAlgorithm(result).get_signal()





    #########################################################################################################
    #
    # Display results
    #
    #########################################################################################################

    portfolio = result.copy()
    portfolio.rename(columns={'isin_or_cusip': 'constituent', 'ret': 'return'}, inplace=True)
    portfolio.loc[:, 'position'] = None
    portfolio.loc[:, 'group'] = 'ALL'
    print(portfolio.columns)

    portfolio.rename(columns={'pt return signal': 'signal'}, inplace=True)
    portfolio.loc[(portfolio['signal'].astype(int).isin([10])) &
                  (portfolio['io supply signal'].astype(int).isin([5])) &
                  (portfolio['io supply signal'].astype(int).isin([5])),
                  'position'] = 'l'
    # portfolio.loc[(portfolio['signal'].astype(int).isin([10])), 'position'] = 'l'
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
    stat = DisplaySheetStatistics(portfolio[header], 'IO S&P ALL Hyb', '', benchmark=benchmark[['benchmark']])
    stat.plot_results()


    portfolio = result.copy()
    portfolio.rename(columns={'isin_or_cusip': 'constituent', 'ret': 'return'}, inplace=True)
    portfolio.loc[:, 'position'] = None
    portfolio.loc[:, 'group'] = 'ALL'

    portfolio.rename(columns={'pt return signal': 'signal'}, inplace=True)
    portfolio.loc[(portfolio['signal'].astype(int).isin([10])) & (portfolio['io demand signal'].astype(int).isin([5])),
                  'position'] = 'l'
    # portfolio.loc[(portfolio['signal'].astype(int).isin([10])), 'position'] = 'l'
    portfolio.dropna(subset=['position'], inplace=True)

    print(portfolio.groupby('date')[['constituent']].count().mean())
    portfolio.set_index('date', inplace=True)

    header = ['group', 'constituent', 'return', 'mc', 'position']
    stat = DisplaySheetStatistics(portfolio[header], 'IO S&P ALL demand', '', benchmark=benchmark[['benchmark']])
    stat.plot_results()


