# from bBlackFireCapitalData.SectorsMarketData.GetSectorsMarketInfos import getsectorinlevel
from bBlackFireCapitalData.SectorsMarketData.GetSectorsMarketInfos import getSectorForLevel
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import profile

""" This script will build some strategies base ont the sentiment in the market. As input, we """ + \
    """have the StocksPriceData price, price target and consensus group by NAICS. The following strategy """+\
    """will be implemented.\n 1. Does WLD Naics can predict the NAICS that are the bestin position?"""+\
    """\n 2. Can the analyst predict the country where a NAICS will outprform?"""

import motor
import tornado
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn import tree, metrics, svm
from sklearn.cross_validation import cross_val_score,  KFold, train_test_split
from scipy.stats import sem

import matplotlib.pyplot as plt

from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataPrice import SectorsMarketDataPrice


__ENTETE__ = ['date', 'eco zone', 'naics', 'csho', 'vol', 'pc', 'ph', 'pl', 'ptnvar', 'ptmvar',
              'pptnvar', 'pptmvar', 'csnrec', 'csmrec', 'csnvar', 'csmvar']

__INDICE_FOR_VAR__ = ['eco zone', 'naics', 'pc']
__ACTUAL__ = ''
__PREV__ = '_prev'


def z_score(group):

    return (group[-1] - group.mean())/group.std()


def reversal_signal(val):

    if val == 10:
        return -1
    elif val == 1:
        return 1
    else:
        return 0


def evaluate_cross_validation(clf, x, y, k):

    cv = KFold(len(y), k, shuffle=True, random_state=0)
    scores = cross_val_score(clf, x, y, cv=cv)
    print(scores)
    print('Mean score: {0:.3f) (+/- {1:.3f})'.format(np.mean(scores), sem(scores)))


def train_and_evaluate(clf, x_tr, x_te, y_tr, y_te):

    clf.fit(x_tr, y_tr)
    print('Accuracy on training test {0:.3f}'.format(clf.score(x_tr, y_tr)))
    print('Accuracy on testing test {0:.3f}'.format(clf.score(x_te, y_te)))

    y_pr = clf.predict(x_te)
    print("Classification report")
    print(metrics.classification_report(y_te, y_pr))


def return_by_quantile(group):

    tab = [['EW'],['MW']]
    for i in range(1, 11):
        t = group[group['Rankingptmvar'] == i]
        t['mc'] = t['csho'] * t['ret']
        tab[0].append(t["ret"].mean() + 1)
        tab[1].append((t['csho'] * t['ret']).sum()/t['csho'].sum() + 1)
    tab = pd.DataFrame(tab, columns=['Type'] + ['Q'+str(i) for i in range(1, 11)])
    tab['l/s'] = 1 + (1.3*(tab['Q9'] - 1) - 0.3*(tab['Q2'] - 1))
    tab['date'] = group.name
    return tab


def group_in_quantile(group):

    tab_group = group[['ptmvar']].quantile(np.array([0, 0.1, 0.2, 0.3,0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]), numeric_only=False)
    group = group.fillna(np.nan)
    labels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    group['Rankingptmvar'] = pd.cut(group['ptmvar'],  tab_group['ptmvar'], labels=labels).values.add_categories('0').fillna('0')

    return group[['date', 'naics', 'Rankingptmvar']]


def get_sector_return(eco, naics, pc, ecop, naicsp, pcp):

    if eco != ecop:
        return None
    if naics != naicsp:
        return None
    try:
        return pc/pcp - 1
    except ZeroDivisionError:
        return None
    except TypeError:
        return None


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


def plot_data():

    tabSectorPrice = np.load('tabSectorPrice.npy')
    tabSectorPrice = pd.DataFrame(tabSectorPrice,columns=__ENTETE__)

    tabSectorPrice = tabSectorPrice.sort_values(by=['eco zone', 'naics', 'date'], ascending=
    [True, True, False]).reset_index(drop=True)

    tabSectorPrice[['eco zone', 'naics']] = tabSectorPrice[['eco zone', 'naics']].astype(str)
    tabSectorPrice[['pc']] = tabSectorPrice[['pc']].astype(float)

    tabSectorPricep = tabSectorPrice.loc[1:, __INDICE_FOR_VAR__].reset_index(drop=True)
    tabSectorPrice = tabSectorPrice.iloc[:-1]
    tabSectorPrice = tabSectorPrice.join(tabSectorPricep, lsuffix=__ACTUAL__, rsuffix=__PREV__)

    v = np.vectorize(GetSectorReturn)

    tabSectorPrice['ret'] = v(tabSectorPrice['eco zone' + __ACTUAL__],
                              tabSectorPrice['naics' + __ACTUAL__],
                              tabSectorPrice['pc' + __ACTUAL__],
                              tabSectorPrice['eco zone' + __PREV__],
                              tabSectorPrice['naics' + __PREV__],
                              tabSectorPrice['pc' + __PREV__]
                              )

    __ENTETE__.append('ret')
    tabSectorPrice = tabSectorPrice[__ENTETE__]

    tabSectorPricep = tabSectorPrice.loc[:tabSectorPrice.shape[0] - 2, ['eco zone', 'naics', 'ret']].reset_index(drop=True)

    tabSectorPrice = tabSectorPrice.iloc[1:].reset_index(drop=True)
    tabSectorPrice = tabSectorPrice.join(tabSectorPricep, lsuffix=__ACTUAL__, rsuffix=__PREV__)

    v = np.vectorize(GetReturnnextmonth)

    tabSectorPrice['ret_n'] = v(tabSectorPrice['eco zone' + __ACTUAL__],
                              tabSectorPrice['naics' + __ACTUAL__],
                              tabSectorPrice['eco zone' + __PREV__],
                              tabSectorPrice['naics' + __PREV__],
                              tabSectorPrice['ret' + __PREV__]
                              )
    __ENTETE__.remove('ret')
    __ENTETE__.append('ret_n')

    tabSectorPrice = tabSectorPrice[__ENTETE__]
    tabSectorPrice = tabSectorPrice.dropna(subset=['ret_n'])
    tabSectorPrice = tabSectorPrice[(tabSectorPrice['ret_n'] < 0.5) & (tabSectorPrice['ret_n'] > -0.5) &
                                    (tabSectorPrice['date'] > datetime(2000, 1, 1))]

    np.save('DataModelSectorPrice', tabSectorPrice)


def strategy_wld():

    tab_data = np.load('DataModelSectorPrice.npy')
    __ENTETE__.append('ret')
    tab_data = pd.DataFrame(tab_data, columns=__ENTETE__)
    tab_data = tab_data.reset_index(drop=True)
    tab_data = tab_data[(tab_data['naics'] == 'ALL')][['date', 'eco zone', 'ptmvar', 'csmvar', 'pc', 'ret', 'csho']].reset_index(drop=True)

    tab_data = tab_data[tab_data['eco zone'] != "WLD"]

    tab_data = tab_data.sort_values(["eco zone", "date"], ascending=[True, True]).reset_index(drop=True)

    print(__ENTETE__)
    tab_data['date'] = pd.to_datetime(tab_data['date'])
    print(tab_data.info())

    #Calcul du Zscore 12 mo pour chaque naics
    result = tab_data.set_index('date')
    result = result[['ptmvar', 'csmvar']].groupby(result['eco zone']).rolling(12, min_periods=9).apply(z_score)
    result = pd.DataFrame(result, columns=['ptmvar']).reset_index()
    result = result[result['date'] > datetime(2000,12,1)]

    #Ranking of Naics Zscore by month
    result = result[['date', 'eco zone', 'ptmvar']].groupby(result['date']).apply(group_in_quantile)
    tab_data = pd.merge(tab_data, result[['eco zone', 'date', 'Rankingptmvar']], on=['eco zone', 'date'])
    tab_data['Rankingptmvar'] = tab_data['Rankingptmvar'].astype(int)
    print(tab_data.head())
    v = np.vectorize(reversal_signal)
    tab_data['test'] = v(tab_data['Rankingptmvar'])
    # plt.figure(1)
    # plt.subplot(2,1,1)
    # tab_data[tab_data['naics'] == '517'].set_index('date')['pc'].plot()
    # plt.subplot(2,1,2)
    # tab_data[tab_data['naics'] == '517'].set_index('date')['test'].plot()


    #Calcul return by Quantile
    result = tab_data[['csho', 'ret', 'Rankingptmvar', 'date']].groupby(tab_data['date']).apply(return_by_quantile).set_index('date')
    result.iloc[[0,1], 1:12] = 1
    print(result.head())
    result = result[result['Type'] == 'EW']
    result = result.groupby(result['Type']).cumprod()
    print(result)

@profile
def strategy_by_sector_for_eco_zone(eco_zone):

    tab_data = np.load('DataModelSectorPrice.npy')
    __ENTETE__.append('ret')
    tab_data = pd.DataFrame(tab_data, columns=__ENTETE__)
    tab_data = tab_data.reset_index(drop=True)
    tab_data = tab_data[(tab_data['eco zone'] == eco_zone)][['date', 'naics', 'ptmvar', 'csmvar', 'pc', 'ret', 'csho']].reset_index(drop=True)

    tab_data = tab_data[tab_data['naics'].isin(getSectorForLevel(2))]

    tab_data = tab_data.sort_values(["naics", "date"], ascending=[True, True]).reset_index(drop=True)

    print(__ENTETE__)
    tab_data['date'] = pd.to_datetime(tab_data['date'])
    print(tab_data.info())

    #Calcul du Zscore 12 mo pour chaque naics
    result = tab_data.set_index('date')
    result = result[['ptmvar', 'csmvar']].groupby(result['naics']).rolling(12, min_periods=9).apply(z_score)
    result = pd.DataFrame(result, columns=['ptmvar']).reset_index()
    result = result[result['date'] > datetime(2000,12,1)]

    #Ranking of Naics Zscore by month
    result = result[['date', 'naics', 'ptmvar']].groupby(result['date']).apply(group_in_quantile)
    tab_data = pd.merge(tab_data, result[['naics', 'date', 'Rankingptmvar']], on=['naics', 'date'])
    tab_data['Rankingptmvar'] = tab_data['Rankingptmvar'].astype(int)
    print(tab_data.head())
    v = np.vectorize(reversal_signal)
    tab_data['test'] = v(tab_data['Rankingptmvar'])
    plt.figure(1)
    plt.subplot(2,1,1)
    tab_data[tab_data['naics'] == '517'].set_index('date')['pc'].plot()
    plt.subplot(2,1,2)
    tab_data[tab_data['naics'] == '517'].set_index('date')['test'].plot()


    #Calcul return by Quantile
    result = tab_data[['csho', 'ret', 'Rankingptmvar', 'date']].groupby(tab_data['date']).apply(return_by_quantile).set_index('date')
    result.iloc[[0,1], 1:12] = 1
    print(result.head())
    result = result[result['Type'] == 'EW']
    result = result.groupby(result['Type']).cumprod()
    print(result)

    # result['Q1'].plot()
    # result['Q2'].plot()
    # result['Q3'].plot()
    # result['Q10'].plot()

    # result['l/s'].plot()
    # result['Q5'].plot()
    # tab_data = tab_data.set_index('date')
    # (tab_data[(tab_data['naics'] == '721')]['strategy']*18).plot(ax=plt.gca())
    #
    # plt.subplot(2,1,2)
    # tab_data[(tab_data['naics'] == '721')]['sptmvar'].plot(ax=plt.gca())
    # #
    plt.show()


if __name__ == "__main__":

    # PlotData()
    strategy_by_sector_for_eco_zone("WLD")
    # strategy_wld()
