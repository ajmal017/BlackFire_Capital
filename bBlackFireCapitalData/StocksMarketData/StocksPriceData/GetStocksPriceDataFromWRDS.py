# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:07:09 2018
@author: Utilisateur
"""
import datetime
import multiprocessing
import collections

import motor
import tornado
import wrds
from sqlalchemy import exc
from pymongo import InsertOne, UpdateOne, UpdateMany
import numpy as np
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString

table = collections.namedtuple('table', [
    'value', "position", "connectionstring","Global",
])

__OBSERVATION__ = 200000

def applyChecking(isin_or_cusip, gvkey, date, curr, csho, vol, adj_factor, price_close,
                  price_high, price_low, iid, exrate, global_):
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
                      'global': global_})


def convertDateToString(date):
    return date.strftime('%Y-%m')

def applyGroupBy(x):
    return (x['date'].unique()[0]), list(x['data'].values)


def GetStocksPriceData(params):

    if params.globalWRDS:

        entete = ['gvkey', 'datadate', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'isin', 'iid']
    else:
        entete = ['gvkey', 'datadate', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'cusip', 'iid']


    try:
        order = entete[-2]
        entete_table = ','.join(entete)

        sqlstmt = 'select pt.*, B.exrat FROM(select ' + entete_table + ' FROM {schema}.{table}  ' \
                    'ORDER BY '.format(schema=params.library, table=params.table,) + order + ' LIMIT {limit} OFFSET {offset}) As pt ' \
                    'LEFT JOIN ibes.hdxrati B ON (pt.datadate = B.anndats AND pt.curcdd = B.curr) '.format(
            limit= params.observation,
            offset=params.offset
        )
        db = wrds.Connection()
        res = db.raw_sql(sqlstmt)
        db.close()
        res.set_index('datadate')
        res = res[res['curcdd'].notnull()]
        res['global'] = params.globalWRDS
        v = np.vectorize(applyChecking)

        res['data'] = v(res[entete[9]], res[entete[0]], res[entete[1]], res[entete[8]],
                        res[entete[3]], res[entete[4]], res[entete[2]], res[entete[5]],
                        res[entete[6]], res[entete[7]], res[entete[10]], res['exrat'],
                        res['global'])

        res = res[['datadate', 'data']]
        v = np.vectorize(convertDateToString)
        res['date'] = v(res['datadate'])
        tab = res.groupby('date').apply(lambda x: applyGroupBy(x))

        ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(StocksMarketDataPrice(ClientDB, "ALL", tab).SetManyStocksPriceInDB)
        ClientDB.close()
        print('lot : [', params.offset, ", ", params.observation + params.offset, "] Completed")
        return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Completed"


    except exc.SQLAlchemyError as e:
        print('lot : [', params.offset, ", ", params.observation + params.offset, "] Not Completed")
        return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Not Completed"
    finally:
        db.close()



