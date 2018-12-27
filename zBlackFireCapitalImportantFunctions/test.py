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


@profile
# @timeit(number= int(3))
def find_duplicate_movies():

    entete = ['gvkey', 'datadate', 'ajexdi', 'cshoc',
              'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
              'isin', 'iid']
    try:

        order = entete[-2]
        entete_table = ','.join(entete)

        sqlstmt = 'select pt.*, B.exrat FROM(select ' + entete_table + ' FROM {schema}.{table}  ' \
                    'ORDER BY '.format(schema='comp', table='g_secd',) + order + ' LIMIT {limit} OFFSET {offset}) As pt ' \
                    'LEFT JOIN ibes.hdxrati B ON (pt.datadate = B.anndats AND pt.curcdd = B.curr) '.format(
            limit=1000000,
            offset=0
        )
        db = wrds.Connection()
        res = db.raw_sql(sqlstmt)
        db.close()
        res.set_index('datadate')
        res = res[res['curcdd'].notnull()]
        v = np.vectorize(applyChecking)

        res['data'] = v(res[entete[9]], res[entete[0]], res[entete[1]], res[entete[8]],
                        res[entete[3]], res[entete[4]], res[entete[2]], res[entete[5]],
                        res[entete[6]], res[entete[7]], res[entete[10]], res['exrat'])

        res = res[['datadate', 'data']]
        v = np.vectorize(convertDateToString)
        res['date'] = v(res['datadate'])
        tab = res.groupby('date').apply(lambda x: applyGroupBy(x))

        ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(StocksMarketDataPrice(ClientDB, "ALL", tab).SetManyStocksPriceInDB)
        ClientDB.close()

    except exc.SQLAlchemyError as e:
        print(e)
        return "lot : Not downloaded"
    finally:
        db.close()


find_duplicate_movies()

# def funct(x):
#     return x['c_id'].unique()[0], x['c1'].values
# t = [[0,10,100], [0,15,110], [0,15,112], [2,96,120],[56,43,42]]
# df = pd.DataFrame(t, columns = ['c_id', 'c1', 'c2'])
# tab=df.groupby('c_id').apply(lambda x: funct(x))
# print(tab)

# date = datetime.datetime(2018,10,20,16,0,0,0)
# print(date.strftime('%Y-%d-%m'))

