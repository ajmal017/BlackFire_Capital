from timeit import timeit

import motor
import tornado
import wrds

from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import profile, TestNoneValue
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import exc
import wrds
from pymongo import InsertOne

_ACTUAL_ = '_act'
_PREVIOUS_ = '_prev'
def applyChecking(isin_or_cusip, gvkey, date, curr, csho, vol, adj_factor, price_close,
                  price_high, price_low, iid, exrate):
    return InsertOne({'isin_or_cusip': isin_or_cusip,
                      'gvkey': gvkey,
                      'date': datetime.datetime(date.year,date.month,date.day,16,0,0),
                      'curr': curr,
                      'csho': csho,
                      'vol': vol,
                      'adj_factor': adj_factor,
                      'price_close': price_close,
                      'price_high': price_high,
                      'price_low': price_low,
                      'iid': iid,
                      'USD_to_curr': exrate,
                      'global': False})


def convertDateToString(date):
    return date.strftime('%Y-%m')

def applyGroupBy(x):
    return (x['date'].unique()[0]), list(x['data'].values)

def OrOperation(x, y):

    return True if (x ==True or y==True) else False


def CalculateConsensusVar(gvkey_act, gvkey_prev, recom_act, recom_prev, date_act,
                            date_prev, mask_code_act, mask_code_prev,):

    if gvkey_act != gvkey_prev:
        return None
    if mask_code_act != mask_code_prev:
        return None
    if (date_act - date_prev).days > 6*30:
        return None
    return int(recom_act) - int(recom_prev)


# @profile
# @timeit(number= int(3))
def find_duplicate_movies(params):

    if params == "consensus":
        ['gvkey', 'ticker', 'cusip', 'estimid', 'cname', 'ireccd', 'anndats', 'amaskcd']
        indice_for_var = [0,5,6,7]
    entete = ['ticker', 'cusip', 'estimid','cname', 'ireccd', 'anndats', 'amaskcd']

    try:

        # order = entete[-2]
        entete_table = ','.join(entete)

        sqlstmt = 'select ' + ','.join(entete) + ' From {schema}.{table} ORDER BY cusip LIMIT {limit} OFFSET {offset}'.format(
            schema='ibes',
            table='recddet',
            limit=1000000,
            offset=0
        )
        # sqlstmt = 'select pt.*, B.exrat FROM(select ' + entete_table + ' FROM {schema}.{table}  ' \
        #             'ORDER BY '.format(schema='comp', table='g_secd',) + order + ' LIMIT {limit} OFFSET {offset}) As pt ' \
        #             'LEFT JOIN ibes.hdxrati B ON (pt.datadate = B.anndats AND pt.curcdd = B.curr) '.format(
        #     limit=1000000,
        #     offset=0
        # )
        # db = wrds.Connection()
        # res = db.raw_sql(sqlstmt)
        # db.close()
        res = np.load('consensusdftest.npy')

        res = pd.DataFrame(res, columns=entete)
        res['gvkey'] = res['cusip']
        entete = ['gvkey', 'ticker', 'cusip', 'estimid', 'cname', 'ireccd', 'anndats', 'amaskcd']
        res = res[entete]
        v = np.vectorize(convertDateToString)
        res['date'] = v(res['anndats'])
        res = res.sort_values(["gvkey","amaskcd","date"], ascending=[True, False,False])
        res = res.iloc[:].reset_index(drop=True)
        res_p = res.iloc[1:, indice_for_var].reset_index(drop=True)
        res = res.iloc[:-1]

        res = res.join(res_p, lsuffix=_ACTUAL_, rsuffix=_PREVIOUS_)
        if params == 'consensus':
            v = np.vectorize(CalculateConsensusVar)
            res['variation'] = v(res[entete[indice_for_var[0]] + _ACTUAL_], res[entete[indice_for_var[0]] + _PREVIOUS_],
                                 res[entete[indice_for_var[1]] + _ACTUAL_], res[entete[indice_for_var[1]] + _PREVIOUS_],
                                 res[entete[indice_for_var[2]] + _ACTUAL_], res[entete[indice_for_var[2]] + _PREVIOUS_],
                                 res[entete[indice_for_var[3]] + _ACTUAL_], res[entete[indice_for_var[3]] + _PREVIOUS_])
        # print(res[['gvkey_act', 'gvkey_prev', 'amaskcd_act', 'amaskcd_prev','anndats_act', 'anndats_prev','variation']])

        res = res.sort_values("date", ascending=True)
        tab_unique_date = list(res['date'].unique())
        print(type(tab_unique_date))

        def SortingForPatching(date):

            pos_end = tab_unique_date.index(date)
            pos_begin = pos_end - 6
            if pos_begin < 0:
                pos_begin = 0
            return tab_unique_date[pos_begin]+' to '+tab_unique_date[pos_end]

        v = np.vectorize(SortingForPatching)
        res['intervall'] = v(res['date'])
        print(res.loc[1:3])
        # print(res[['date','intervall']])



    except exc.SQLAlchemyError as e:
        print(e)
        return "lot : Not downloaded"
    # finally:
    #     db.close()


find_duplicate_movies('consensus')
# def funct(x):
#     return x['c_id'].unique()[0], x['c1'].values
# t = [[0,10,100], [0,15,110], [0,15,112], [2,96,120],[56,43,42]]
# df = pd.DataFrame(t, columns = ['c_id', 'c1', 'c2'])
# tab=df.groupby('c_id').apply(lambda x: funct(x))
# print(tab)

# date = datetime.datetime(2018,10,20,16,0,0,0)
# print(date.strftime('%Y-%d-%m'))

