from bBlackFireCapitalData.SectorsMarketData.GetSectorsMarketInfos import getsectorinlevel

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

def GroupBySectorWLD(group):
    date = group.name
    tabGroup = group[['ptmvar', 'pptmvar', 'csmrec', 'csmvar', 'ret']].quantile(np.array([0, 0.1, 0.3, 0.7,0.9, 1]), numeric_only=False)
    group = group.fillna(np.nan)

    labels = ['1', '2', '3', '4', '5']
    group['bkptmvar'] = pd.cut(group['ptmvar'], tabGroup['ptmvar'], labels=labels).values.add_categories('0').fillna('0')
    group['bkpptmvar'] = pd.cut(group['pptmvar'], tabGroup['pptmvar'], labels=labels).values.add_categories('0').fillna('0')
    group['bkcsmrec'] = pd.cut(group['csmrec'], tabGroup['csmrec'], labels=labels).values.add_categories('0').fillna('0')
    group['bkcsmvar'] = pd.cut(group['csmvar'], tabGroup['csmvar'], labels=labels).values.add_categories('0').fillna('0')
    group['bkret'] = pd.cut(group['ret'], tabGroup['ret'], labels=labels).values.add_categories('0').fillna('0')
    group['date'] = date
    group['wret'] = group['csho'] * group['ret']

    to_sell = group[(group['bkcsmvar'] == '1') & (group['bkptmvar'] == '5')]
    to_buy = group[(group['bkcsmvar'] == '5')& (group['bkptmvar'] == '1')]

    to_buy = (to_buy['csho'] * to_buy['ret']).sum()/to_buy['csho'].sum()
    to_sell = (to_sell['csho'] * to_sell['ret']).sum() / to_sell['csho'].sum()

    # print(to_buy, to_sell)
    return 1.3 * to_sell - 0.3 * to_buy + 1

    # return group[['date', 'naics', 'bkptmvar', 'bkpptmvar', 'bkcsmrec', 'bkcsmvar', 'bkret']]


def GetSectorReturn(eco, naics, pc, ecop, naicsp, pcp):

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


def GetReturnnextmonth(eco, naics, ecop, naicsp, ret):

    if eco != ecop:
        return None
    if naics != naicsp:
        return None

    return ret


def getSectorsPrice(params):

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


def PlotData():

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


def StrategybySectorWLD():

    tabData = np.load('DataModelSectorPrice.npy')
    __ENTETE__.append('ret')
    tabData = pd.DataFrame(tabData, columns=__ENTETE__)
    print(__ENTETE__)

    t = tabData[(tabData['eco zone'] == 'CAD') & (tabData['naics'] == '211')].set_index('date')[['ret', 'pc', 'ptmvar', 'csmvar']]
    print(t['pc'].iloc[0])
    t['price close'] = t['pc']/t['pc'].iloc[0]
    t['price target var'] = t['ptmvar']*3
    t['consensus var'] = -t['csmvar'] / t['csmvar'].iloc[0]

    t[['price close', 'price target var']].plot()
    print(t[['pc', 'ptmvar', 'csmvar']])
    print(t[['ret', 'ptmvar', 'csmvar']].corr(method='kendall'))
    # tabData = tabData[['date', 'eco zone', 'naics', 'csho', 'ptmvar', 'pptmvar', 'csmrec', 'csmvar', 'ret']]
    #
    # tabData = tabData[tabData['naics'].isin(getsectorinlevel(2))]
    #
    # tabData = tabData[(tabData['eco zone'] == 'USD') & (tabData['naics'] != 'ALL')]
    # print(tabData.shape)
    # tabData['ret'] = tabData['ret'].dropna()
    # tabData['ptmvar'] = tabData['ptmvar'].dropna()
    # print(tabData.shape)
    # tabData = tabData.groupby(['date']).apply(GroupBySectorWLD)
    # print(tabData.index)
    # tabData = pd.DataFrame(np.array(tabData), columns=['return'], index=tabData.index)
    # tabData['ret_cum'] = tabData['return'].cumprod()
    # print(tabData)

    # tabData[['ret_cum']].plot()

    plt.show()

    # x_tr, x_te, y_tr, y_te = train_test_split(tabData[['ptmvar']], tabData['ret'], test_size=0.25, random_state=0)
    # svc = tree.DecisionTreeClassifier(criterion='entropy')
    # evaluate_cross_validation(svc, x_tr, y_tr, 5)
    # print(x_tr,y_tr)
    # train_and_evaluate(svc, x_tr, x_te, y_tr, y_te)


if __name__ == "__main__":

    # PlotData()
    StrategybySectorWLD()
