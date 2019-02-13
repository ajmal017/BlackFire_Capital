# from bBlackFireCapitalData.SectorsMarketData.GetSectorsMarketInfos import getsectorinlevel
from aBlackFireCapitalClass.ClassStrategyStatistics.displaysheet import DisplaysheetStatistics
from bBlackFireCapitalData.SectorsMarketData.GetSectorsMarketInfos import getSectorForLevel
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import profile

""" This script will build some strategies base ont the sentiment in the market. As input, we """ + \
"""have the StocksPriceData price, price target and consensus group by NAICS. The following strategy """ + \
"""will be implemented.\n 1. Does WLD Naics can predict the NAICS that are the bestin position?""" + \
"""\n 2. Can the analyst predict the country where a NAICS will outprform?"""

import motor
import tornado
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn import tree, metrics, svm, ensemble
from pathlib import Path
from sklearn.cross_validation import cross_val_score, KFold, train_test_split
from scipy.stats import sem

import matplotlib.pyplot as plt

from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataPrice import SectorsMarketDataPrice

__ENTETE__ = ['date', 'eco zone', 'naics', 'csho', 'vol', 'pc', 'ph', 'pl', 'ptnvar', 'ptmvar',
              'pptnvar', 'pptmvar', 'csnrec', 'csmrec', 'csnvar', 'csmvar']

__INDICE_FOR_VAR__ = ['eco zone', 'naics', 'pc']
__ACTUAL__ = ''
__PREV__ = '_prev'

########################################################################################################################
#                                                                                                                      #
# Section 1: Machine learning Evaluation                                                                               #
#                                                                                                                      #
########################################################################################################################

def evaluate_cross_validation(clf, x, y, k):
    cv = KFold(len(y), k, shuffle=True, random_state=0)
    scores = cross_val_score(clf, x, y, cv=cv)
    print(scores)
    print('Mean score: {0:.3f}) (+/- {1:.3f})'.format(np.mean(scores), sem(scores)))


def train_and_evaluate(clf, x_tr, x_te, y_tr, y_te):
    clf.fit(x_tr, y_tr)
    print('Accuracy on training test {0:.3f}'.format(clf.score(x_tr, y_tr)))
    print('Accuracy on testing test {0:.3f}'.format(clf.score(x_te, y_te)))

    y_pr = clf.predict(x_te)
    print("Classification report")
    print(metrics.classification_report(y_te, y_pr))

########################################################################################################################
#                                                                                                                      #
# Section 2: Important Function for the computation                                                                    #
#                                                                                                                      #
########################################################################################################################


def calculate_benchmark(group):

    monthly_return = (group['mc'] * group['return']).sum()/group['mc'].sum()
    return monthly_return


def z_score(group):
    """
         This fucntion is used to compute the z-score of an array input.

         :param group: array of the data we want to compute the
    """

    return (group[-1] - group.mean()) / group.std()


def return_in_quantile(group) -> pd.DataFrame:
    """"
    This fuction take a Dataframe as input and return a columns with a ranking from 1 to 10 given the feature

    :param
    group: Dataframe containing the values to rank
    feature:  Name of the column to rank.

    :return
    Dataframe containing one column ranking with the features ranks.

    """""
    print(group.name)
    print(group)
    Labels = ['1', '2', '3']
    tab_group = group[['ret']].quantile(np.array([0, 0.2, 0.8, 1]), numeric_only=False)
    group = group.fillna(np.nan)

    tab_group['labels'] = ['0'] + Labels
    x = tab_group[['ret', 'labels']].drop_duplicates(['ret'])
    labels = list(x['labels'])
    labels.remove('0')
    group['ranking_return'] = pd.cut(group['ret'], x['ret'], labels=labels).values.add_categories('0').fillna('0')

    return group


def calculate_return_by_quantile(group, column_name):

    tab = [[], []]
    for i in range(1, 11):
        t = group[group.loc[:, column_name] == i]
        if t["return"].mean() == np.nan:
            tab[0].append(0)
        else:
            tab[0].append(t["return"].mean())

        try:
            tab[1].append((t['mc'] * t['return']).sum() / t['mc'].sum())
        except ZeroDivisionError:
            tab[1].append(0)
    tab = pd.DataFrame(tab, columns=['Q' + str(i) for i in range(1, 11)])
    tab['l/s (130/30)'] = 1.3 * tab['Q10']  - 0.3 * tab['Q1']
    tab['l/s (150/50)'] = 1.5 * tab['Q10']  - 0.5 * tab['Q1']
    tab.fillna(0, inplace=True)
    if group.name[0].year == 2017:
        print(group.name)
        print(group[group['historical_ranking_pt_return'] == 10][['mc', 'return']])
        print(tab[["Q1", 'Q10']])

    return tab


def cum_prod_return(group):


    group = group + 1
    group.fillna(1, inplace=True)
    group.iloc[0:1, :] = 100
    group = group.cumprod()

    return group.reset_index()


def group_in_quantile(group, feature, quantiles) -> pd.DataFrame:
    """"
    This fuction take a Dataframe as input and return a columns with a ranking from 1 to 10 given the feature

    :param
    group: Dataframe containing the values to rank
    feature:  Name of the column to rank

    :return
    Dataframe containing one column ranking with the features ranks.

    """""
    labels = [str(i + 1) for i in range(len(quantiles) - 1)]
    tab_group = group[[feature]].quantile(np.array(quantiles), numeric_only=False)
    group = group.fillna(np.nan)

    tab_group['labels'] = ['0'] + labels
    x = tab_group[[feature, 'labels']].drop_duplicates([feature])
    labels = list(x['labels'])
    labels.remove('0')
    group['ranking_' + feature] = pd.cut(group[feature], x[feature], labels=labels).values.add_categories('0').fillna(
        '0')
    return group


def get_sector_return(eco, naics, pc, ecop, naicsp, pcp):
    if eco != ecop:
        return None
    if naics != naicsp:
        return None
    try:
        return pc / pcp - 1
    except ZeroDivisionError:
        return None
    except TypeError:
        return None


def shift_stocks_return(group, periode):

    group['index'] = group.index
    group = group.set_index('date')
    t = group[['return']].shift(periods=periode, freq='M')
    group.loc[:, 'return'] = t['return']

    return group.set_index('index')[['return']]


def get_next_monthly_return(eco, naics, ecop, naicsp, ret):
    if eco != ecop:
        return None
    if naics != naicsp:
        return None

    return ret


def get_sectors_price(params):
    ClientDB = motor.motor_tornado.MotorClient(params.ConnectionString)
    # loop = tornado.ioloop.IOLoop
    # loop.current().run_sync(SectorsMarketDataPrice(ClientDB, None).create_index)

    loop = tornado.ioloop.IOLoop
    tabSectorInfos = loop.current().run_sync(SectorsMarketDataPrice(ClientDB, {}, None).GetStocksPriceFromDB)

    tab_to_save = []

    for value in tabSectorInfos:
        eco = value['eco zone']
        naics = value['naics']
        date = value['date']

        csho = value['csho']
        vol = value['vol']
        pc = value['price_close']
        ph = value['price_high']
        pl = value['price_low']

        pt = value['price_target']
        cs = value['consensus']

        t = [date, eco, naics, csho, vol, pc, ph, pl, pt['num_var'], pt['mean_var'], pt['pnum_var'], pt['pmean_var'],
             cs['num_recom'], cs['mean_recom'], cs['num_var'], cs['mean_var']]

        tab_to_save.append(t)

    np.save('tabSectorPrice.npy', tab_to_save)

    ClientDB.close()

########################################################################################################################
#                                                                                                                      #
# Section 3: Strategy Implementation                                                                                   #
#                                                                                                                      #
########################################################################################################################


def strategy_by_sector_for_eco_zone(eco_zone):

    my_path = Path(__file__).parent.parent.parent.parent.resolve()
    monthly_prices = np.load(str(my_path) + '/bBlackFireCapitalData/SectorsMarketData/monthly_sectors_prices_us.npy')
    entete = ['eco zone', 'naics', 'date', 'level_3', 'mc', 'vol', 'npt', 'npptvar', 'nptvar', 'nrc', 'nrcvar',
              'return', 'pt_return', 'mpt_return', 'pptvar', 'ptvar', 'rc', 'rcvar']

    monthly_prices = pd.DataFrame(monthly_prices, columns=entete)
    monthly_prices = monthly_prices[(monthly_prices['eco zone'] == eco_zone)]

    monthly_prices.loc[monthly_prices['pt_return'] == 0, 'pt_return'] = None
    print(monthly_prices.info())

    # Filter all NAICS of level 2
    monthly_prices = monthly_prices[monthly_prices['naics'].isin(getSectorForLevel(2))]
    monthly_prices = monthly_prices.sort_values(["eco zone", "naics", "date"], ascending=[True, True, True]).reset_index(drop=True)
    monthly_prices['date'] = pd.DatetimeIndex(monthly_prices['date'])

    #Benchmark
    Benchmark = monthly_prices[['mc', 'return']].groupby(monthly_prices['date']).apply(calculate_benchmark)
    Benchmark = pd.DataFrame(Benchmark, columns=['benchmark'])

    # Shift Stocks returns
    result = monthly_prices[['date', 'eco zone', 'naics', 'return']].groupby(['eco zone', 'naics']).apply(shift_stocks_return, -1)
    monthly_prices['return'] = result['return']

    # Remove all None return
    monthly_prices.dropna(subset=['return'], inplace=True)

    # Ranking Stocks Returns
    quantiles = [0, 0.2, 0.8, 1]
    result = monthly_prices[['date', 'eco zone', 'naics', 'return']].groupby(['eco zone', 'date']).apply(group_in_quantile,
                                                                                                  'return', quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'naics', 'ranking_return']])

    ###################################################################################################################
    #
    # Ranking of sector by month
    #
    ###################################################################################################################


    # --> Price Target
    quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    result = monthly_prices[['date','naics', 'eco zone', 'pt_return']].groupby(['eco zone', 'date']).apply(group_in_quantile,
                                                                                                     'pt_return',
                                                                                                     quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'naics', 'ranking_pt_return']])

    # --> Mean Price Target Return
    quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    result = monthly_prices[['date','naics', 'eco zone', 'mpt_return']].groupby(['eco zone', 'date']).apply(group_in_quantile,
                                                                                                     'mpt_return',
                                                                                                     quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'naics', 'ranking_mpt_return']])

    # --> Mean Price Target Return Variation
    quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    result = monthly_prices[['date', 'naics', 'eco zone', 'ptvar']].groupby(['eco zone', 'date']).apply(group_in_quantile,
                                                                                                     'ptvar',
                                                                                                     quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'naics', 'ranking_ptvar']])

    # # --> Mean recommendation
    # quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    # result = monthly_prices[['date', 'naics', 'eco zone', 'rc']].groupby(['eco zone', 'date']).apply(group_in_quantile,
    #                                                                                                  'rc',
    #                                                                                                  quantiles)
    # monthly_prices = pd.merge(monthly_prices, result[['date', 'naics', 'ranking_rc']])
	#
    # # --> Mean recommendation var
    # quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    # result = monthly_prices[['date', 'naics', 'eco zone', 'rcvar']].groupby(['eco zone', 'date']).apply(group_in_quantile,
    #                                                                                                  'rcvar',
    #                                                                                                  quantiles)
    # monthly_prices = pd.merge(monthly_prices, result[['date', 'naics', 'ranking_rcvar']])

    ###################################################################################################################
    #
    # calculation of Z score
    #
    ###################################################################################################################

    # Calculation of 12 month z score for features ptvar, pt et rcvar
    result = monthly_prices.set_index('date')

    result = result[['naics', 'eco zone', 'ptvar', 'pt_return', 'mpt_return', 'rc', 'rcvar']].groupby(['eco zone', 'naics']).rolling(12,
                                                                                                min_periods=9).apply(
        z_score)

    result = result.reset_index()
    result = result[result['date'] > datetime(2000, 12, 1)]

    ###################################################################################################################
    #
    # Ranking of Z score by month
    #
    ###################################################################################################################

    # --> Price Target Variation
    _ = result[['date','eco zone', 'naics', 'ptvar']].groupby(['eco zone', 'date']).apply(group_in_quantile, 'ptvar',
                                                                                    quantiles)
    _.rename(columns={'ranking_ptvar': 'historical_ranking_ptvar'}, inplace=True)
    monthly_prices = pd.merge(monthly_prices, _[['date', 'naics', 'historical_ranking_ptvar']], on=['naics', 'date'])

    # --> Price Target return
    _ = result[['date','eco zone', 'naics', 'pt_return']].groupby(['eco zone', 'date']).apply(group_in_quantile, 'pt_return',
                                                                                    quantiles)
    _.rename(columns={'ranking_pt_return': 'historical_ranking_pt_return'}, inplace=True)
    monthly_prices = pd.merge(monthly_prices, _[['date', 'naics', 'historical_ranking_pt_return']], on=['naics', 'date'])

    # --> Mean Price Target return
    _ = result[['date','eco zone', 'naics', 'mpt_return']].groupby(['eco zone', 'date']).apply(group_in_quantile, 'mpt_return',
                                                                                    quantiles)
    _.rename(columns={'ranking_mpt_return': 'historical_ranking_mpt_return'}, inplace=True)
    monthly_prices = pd.merge(monthly_prices, _[['date', 'naics', 'historical_ranking_mpt_return']], on=['naics', 'date'])


    # # --> Mean recommendation
    # _ = result[['date','eco zone', 'naics', 'rc']].groupby(['eco zone', 'date']).apply(group_in_quantile, 'rc',
    #                                                                                 quantiles)
    # _.rename(columns={'ranking_rc': 'historical_ranking_rc'}, inplace=True)
    # monthly_prices = pd.merge(monthly_prices, _[['date', 'naics', 'historical_ranking_rc']], on=['naics', 'date'])
	#
    # # --> Mean recommendation variation
    # _ = result[['date','eco zone', 'naics', 'rcvar']].groupby(['eco zone', 'date']).apply(group_in_quantile, 'rcvar',
    #                                                                                 quantiles)
    # _.rename(columns={'ranking_rcvar': 'historical_ranking_rcvar'}, inplace=True)
    # monthly_prices = pd.merge(monthly_prices, _[['date', 'naics', 'historical_ranking_rcvar']], on=['naics', 'date'])

    ###################################################################################################################
    #
    # Return of Quantile by feature
    #
    ###################################################################################################################

    portfolio = monthly_prices[['date', 'eco zone', 'naics', 'return', 'mc', 'ranking_ptvar', 'historical_ranking_ptvar']]
    portfolio = portfolio[(portfolio['ranking_ptvar'].astype(int) > 9) & (portfolio['historical_ranking_ptvar'].astype(int) > 8)]

    sector = np.load('isin_pf.npy')
    sector = pd.DataFrame(sector, columns=['date', 'naics', 'isin', 'return', 'mc', 'historical_ranking_pt_return'])
    sector['date'] = pd.DatetimeIndex(sector['date'])
    portfolio = pd.merge(portfolio[['date', 'naics']], sector, on=['date', 'naics'])
    portfolio = portfolio[['date', 'naics', 'isin', 'return', 'mc', 'historical_ranking_pt_return']]
    portfolio.columns = ['date', 'group', 'constituent', 'return', 'mc', 'signal']
    portfolio = portfolio[portfolio['signal'] == '10']
    portfolio.loc[:, 'signal'] = 'buy'
    stat = DisplaysheetStatistics(portfolio, 'Sector selection', Benchmark)
    stat.plot_results()
    return
    portfolio.columns = ['date', 'group', 'constituent', 'return', 'mc', 'signal']

    portfolio.loc[:, 'signal'] = 'buy'
    stat = DisplaysheetStatistics(portfolio,'Sector selection', Benchmark)
    stat.plot_results()

    return
    monthly_prices['historical_ranking_pt_return'] = monthly_prices['historical_ranking_ptvar'].astype(int)
    print('done')
    result = monthly_prices[['date', 'naics', 'mc', 'return', 'historical_ranking_pt_return', 'eco zone']].groupby(['date', 'eco zone']).apply(
        calculate_return_by_quantile, 'historical_ranking_pt_return').reset_index()
    # return
    result.rename(columns={'level_2': 'Type'}, inplace=True)
    result.loc[ :, 'Type'] = result['Type'].map({0: 'EW', 1: 'MW'})

    # benchmark['date'] = pd.DatetimeIndex(benchmark['date'])
    # result = pd.merge(result, benchmark, on=['date', 'naics'], )

    result.set_index('date', inplace=True)
    result = result.groupby(['eco zone', 'Type']).apply(cum_prod_return).reset_index()
    result.set_index('date', inplace=True)
    result.to_excel("output.xlsx")
    print(result)
    result[result['Type'] == 'MW'][['Q10', 'Q1']].plot()
    plt.show()
    return


def strategy_by_stocks_in_eco_zone(eco_zone):

    my_path = Path(__file__).parent.parent.parent.parent.resolve()
    monthly_prices = np.load(str(my_path) + '/bBlackFireCapitalData/SectorsMarketData/monthly_prices_adj_us.npy')
    entete = ['eco zone', 'naics', 'gvkey', 'isin', 'exchg', 'USDtocurr', 'adj_factor', 'date',
                                       'pc', 'ph', 'pl', 'vol', 'curr', 'pt', 'npt', 'pptvar', 'npptvar', 'ptvar',
                                       'nptvar', 'rc', 'nrc', 'rcvar', 'nrcvar', 'csho', 'adj_pc', 'return',
                                       'pt_return']

    monthly_prices = pd.DataFrame(monthly_prices, columns=entete)
    monthly_prices = monthly_prices[(monthly_prices['eco zone'] == eco_zone)]
    monthly_prices.loc[monthly_prices['pt_return'] == 0, 'pt_return'] = None
    print(monthly_prices.info())

    # Filter all NAICS of level 2
    monthly_prices = monthly_prices[monthly_prices['naics'].isin(getSectorForLevel(2))]
    monthly_prices = monthly_prices.sort_values(["naics", "isin", "date"], ascending=[True, True, True]).reset_index(drop=True)
    monthly_prices['date'] = pd.DatetimeIndex(monthly_prices['date'])

    # Calcul of Market Cap.
    monthly_prices.loc[:, 'mc'] = monthly_prices.loc[:, 'csho'] * monthly_prices.loc[:, 'pc'] / monthly_prices.loc[:, 'USDtocurr']
    # monthly_prices = monthly_prices[monthly_prices['mc'] > 100000000]

    # Shift Stocks returns
    result = monthly_prices[['date', 'isin', 'pc', 'return']].groupby(['isin']).apply(shift_stocks_return, -1)
    monthly_prices['return'] = result['return']

    # Calcul of BenchMark
    # result = monthly_prices[['date', 'naics','pc', 'return', 'csho']].groupby(['naics', 'date']).apply(calculate_benchmark)
    # result = result.reset_index().set_index('date').shift(periods=1, freq='M').reset_index()
    # result.rename(columns={0: 'Benchmark'}, inplace=True)
    # benchmark = result

    # Remove all None return
    monthly_prices.dropna(subset=['return'], inplace=True)

    # Ranking Stocks Returns
    quantiles = [0, 0.2, 0.8, 1]
    result = monthly_prices[['date', 'naics', 'isin', 'return']].groupby(['naics', 'date']).apply(group_in_quantile,
                                                                                                  'return', quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'isin', 'ranking_return']])

    # Ranking of Feature by month
    # --> Price Target
    quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    result = monthly_prices[['date', 'naics', 'isin', 'pt_return']].groupby(['naics', 'date']).apply(group_in_quantile,
                                                                                                     'pt_return',
                                                                                                     quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'isin', 'ranking_pt_return']])

    # --> Mean variation of Price Target
    quantiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    result = monthly_prices[['date', 'naics', 'isin', 'ptvar']].groupby(['naics', 'date']).apply(group_in_quantile,
                                                                                                 'ptvar', quantiles)
    monthly_prices = pd.merge(monthly_prices, result[['date', 'isin', 'ranking_ptvar']])

    # Calculation of 12 month z score for features ptvar, pt et rcvar
    result = monthly_prices.set_index('date')

    result = result[['naics', 'isin', 'ptvar', 'pt_return']].groupby(['naics', 'isin']).rolling(12,
                                                                                                min_periods=9).apply(
        z_score)

    result = result.reset_index()
    result = result[result['date'] > datetime(2000, 12, 1)]

    # Ranking of Naics z score by month

    # --> Price Target Variation
    _ = result[['date', 'naics', 'isin', 'ptvar']].groupby(['naics', 'date']).apply(group_in_quantile, 'ptvar',
                                                                                    quantiles)
    _.rename(columns={'ranking_ptvar': 'historical_ranking_ptvar'}, inplace=True)
    monthly_prices = pd.merge(monthly_prices, _[['date', 'isin', 'historical_ranking_ptvar']], on=['isin', 'date'])

    # --> Price Target returm
    _ = result[['date', 'naics', 'isin', 'pt_return']].groupby(['naics', 'date']).apply(group_in_quantile, 'pt_return',
                                                                                        quantiles)
    _.rename(columns={'ranking_pt_return': 'historical_ranking_pt_return'}, inplace=True)
    monthly_prices = pd.merge(monthly_prices, _[['date', 'isin', 'historical_ranking_pt_return']], on=['isin', 'date'])

    portfolio = monthly_prices[['date', 'naics', 'isin', 'return', 'mc', 'historical_ranking_pt_return']]
    np.save('isin_pf.npy', portfolio)
    portfolio.columns = ['date', 'group', 'constituent', 'return', 'mc', 'signal']

    return
    portfolio = portfolio[portfolio['signal'] == '10']
    portfolio.loc[:, 'signal'] = 'buy'
    stat = DisplaysheetStatistics(portfolio, 'Sector selection')
    stat.plot_results()


if __name__ == "__main__":
    # PlotData()
    # strategy_by_sector_for_eco_zone("USD")
    # strategy_by_stocks_in_eco_zone("USD")
    strategy_by_sector_for_eco_zone('USD')
    # strategy_wld()
