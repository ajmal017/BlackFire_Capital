import motor
import tornado
import collections
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from pymongo import InsertOne, UpdateOne

__author__ = 'pougomg'
import wrds
import numpy as np


def get_stocks_info_data_dict(parameter: collections) -> dict:
    """
    
    :param parameter:
    :return:
    """
    db = wrds.Connection()
    res = db.get_table(library=parameter.library,
                       table=parameter.table[0])
    db.close()

    d_info = dict()
    try:
        d_info = np.load('dict_info.npy').item()
    except FileNotFoundError:
        print("File doesnt exists")

    for pos in range(res.shape[0]):

        ticker = res['tic'][pos]
        gvkey = res['gvkey'][pos]
        iid = res['iid'][pos]
        cusip = res['cusip'][pos]
        exchg = res['exchg'][pos]

        if not np.isnan(exchg):
            exchg = str(int(exchg))

        excntry = res['excntry'][pos]
        ibtic = res['ibtic'][pos]
        isin = res['isin'][pos]
        secstat = res['secstat'][pos]
        sedol = res['sedol'][pos]
        tpci = res['tpci'][pos]

        if cusip is None and isin is not None:
            cusip = isin[2:11]
        if cusip is not None:
            cusip_8 = cusip[0:8]
        else:
            cusip_8 = None

        stock_id = [{'ticker': ticker, 'ibtic': ibtic, 'iid': iid, 'cusip': cusip,
                     'exhg': exchg, 'excntry': excntry, 'isin': isin,
                     'secstat': secstat, 'sedol': sedol, 'tpci': tpci,
                     'cusip_8': cusip_8}]

        data = {'_id': gvkey, 'company name': None, 'incorporation location': None, 'naics': None,
                'sic': None, 'gic sector': None, 'gic ind': None, 'eco zone': None,
                'stock identification': stock_id}

        if gvkey in d_info:
            d_info[gvkey]['stock identification'].append(stock_id[0])
        else:
            d_info[gvkey] = data

    db = wrds.Connection()

    res = db.get_table(library=parameter.library,
                       table=parameter.table[1])
    db.close()

    for pos in range(res.shape[0]):

        gvkey = res['gvkey'][pos]
        company = res['conm'][pos]

        if parameter.globalWRDS:
            fic = res['fic'][pos]
        else:
            fic = None
        sic = res['sic'][pos]
        naics = res['naics'][pos]

        data = {'_id': gvkey, 'company name': company, 'incorporation location': fic, 'naics': naics,
                'sic': sic, 'gic sector': None, 'gic ind': None, 'eco zone': None,
                'stock identification': None}

        if gvkey in d_info:
            d_info[gvkey]['company name'] = company if company is not None else d_info[gvkey]['company name']
            d_info[gvkey]['incorporation location'] = fic if fic is not None else d_info[gvkey][
                'incorporation location']
            d_info[gvkey]['naics'] = naics if naics is not None else d_info[gvkey]['naics']
            d_info[gvkey]['sic'] = sic if sic is not None else d_info[gvkey]['sic']

    if not parameter.globalWRDS:

        db = wrds.Connection()

        res = db.raw_sql("select a.gvkey, a.fic  from comp.secd a group by a.gvkey, a.fic")

        for pos in range(res.shape[0]):
            gvkey = res['gvkey'][pos]
            fic = res['fic'][pos]

            if gvkey in d_info:
                d_info[gvkey]['incorporation location'] = fic if fic is not None else d_info[gvkey][
                    'incorporation location']

        db.close()
    data = []
    for key in d_info:
        data.append(d_info[key])

    np.save('dict_info.npy', d_info)
    return parameter.table, 'Completed'


def set_stocks_infos_data_in_db(connection_string):

    client_db = motor.motor_tornado.MotorClient(connection_string)
    d_info = np.load('dict_info.npy').item()
    data = []

    for key in d_info:
        data.append(d_info[key])
    tornado.ioloop.IOLoop.current().run_sync(StocksMarketDataInfos(client_db, data).SetDataInDB)
    client_db.close()


def get_stocks_infos_from_db(connection_string):

    client_db = motor.motor_tornado.MotorClient(connection_string)
    tab = tornado.ioloop.IOLoop.current().run_sync(StocksMarketDataInfos(client_db, {}, None).GetDataFromDB)
    result = []
    client_db.close()

    for value in tab:

        gvkey = value['_id']
        eco = value['eco zone']
        naics = value['naics']

        ident = value['stock identification']
        for v in ident:
            result.append([gvkey, eco, naics, v['isin'], v['ibtic'], v['cusip_8'], v['exhg']])
            result.append([gvkey, eco, naics, v['cusip'], v['ibtic'], v['cusip_8'], v['exhg']])

    np.save('StocksPricesInfos', result)


if __name__ == 'main':


