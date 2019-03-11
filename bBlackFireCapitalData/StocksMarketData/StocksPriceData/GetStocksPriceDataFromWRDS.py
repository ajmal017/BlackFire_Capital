# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:07:09 2018
@author: Ghislain N. P
"""
import datetime
import multiprocessing
import collections

import motor
import tornado
import wrds
from sqlalchemy import exc
from pymongo import InsertOne
import numpy as np
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataPrice import StocksMarketDataPrice
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString


table = collections.namedtuple('table', [
    'value', "position", "connectionstring","Global",
])

__OBSERVATION__ = 200000


def apply_checking(isin_or_cusip, gvkey, date, curr, csho, vol, adj_factor, price_close,
                  price_high, price_low, iid, exrate, global_):

    """
    This function is used to construct all the infos to insert in the DB using MongoDB

    :param isin_or_cusip: isin or cusip of the stocks
    :param gvkey: gvkey of the stocks
    :param date: data of the price
    :param curr: currency of the stocks
    :param csho: common shares
    :param vol: volume trade of the stocks
    :param adj_factor: adjusted factor to compute the split of the stocks price
    :param price_close: price close at the end of the day
    :param price_high: high price for the daya
    :param price_low: low price for the day
    :param iid:
    :param exrate: exchange rate USD/curr for the day
    :param global_: Global/Norht America stocks

    :return: Data frame of pymongo.Insertone()
    """
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


def convert_date_to_string(date):

    """
    This function is used to convert a datetime do string data in format Year-Month.
    :param date: stock price date
    :return: Data frame of date
    """
    return date.strftime('%Y-%m')


def apply_group_by(x):
    return (x['date'].unique()[0]), list(x['data'].values)


def get_stocks_price_data(params):

    """
    This function is used to download the stocks price for the stocks in WRDS.

    :param params collections containing data to download all the useful information. These informations are :

    1. globalWRDS = True/False Indicates if we use the Global or the US database for the stocks.
    2. library: Library to get the stocks from comp
    3. table: Table name gsecd or secd
    4. Observation (int): Number of observation to get
    5. Offset: Offset position to start

    :return: Set the data in the MongoDB
    """

    # entete to download
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

        # SQL Statement to get Data
        sqlstmt = 'select pt.*, B.exrat FROM(select ' + entete_table + ' FROM {schema}.{table}  ' \
                    'ORDER BY '.format(schema=params.library, table=params.table,) + order + ' LIMIT {limit} OFFSET {offset}) As pt ' \
                    'LEFT JOIN ibes.hdxrati B ON (pt.datadate = B.anndats AND pt.curcdd = B.curr) '.format(
            limit=params.observation,
            offset=params.offset
        )
        db = wrds.Connection()
        res = db.raw_sql(sqlstmt)
        db.close()
        res.set_index('datadate')
        res = res[res['curcdd'].notnull()]
        res['global'] = params.globalWRDS

        # Create InsertOne for all the params
        v = np.vectorize(apply_checking)

        res['data'] = v(res[entete[9]], res[entete[0]], res[entete[1]], res[entete[8]],
                        res[entete[3]], res[entete[4]], res[entete[2]], res[entete[5]],
                        res[entete[6]], res[entete[7]], res[entete[10]], res['exrat'],
                        res['global'])

        res = res[['datadate', 'data']]
        v = np.vectorize(convert_date_to_string)
        res['date'] = v(res['datadate'])
        tab = res.groupby('date').apply(lambda x: apply_group_by(x))

        # Add the data to the MpngoDB
        client_db = motor.motor_tornado.MotorClient(ProdConnectionString)
        loop = tornado.ioloop.IOLoop
        loop.current().run_sync(StocksMarketDataPrice(client_db, "ALL", tab).SetManyStocksPriceInDB)
        client_db.close()
        print('lot : [', params.offset, ", ", params.observation + params.offset, "] Completed")
        return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Completed"

    except exc.SQLAlchemyError as e:
        print('lot : [', params.offset, ", ", params.observation + params.offset, "] Not Completed")
        return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Not Completed"
    finally:
        db.close()

# if __name__ == 'main':
