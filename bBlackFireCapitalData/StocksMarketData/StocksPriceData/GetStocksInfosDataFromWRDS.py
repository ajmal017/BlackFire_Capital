import motor
import tornado
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from pymongo import InsertOne, UpdateOne

__author__ = 'pougomg'
import wrds
import numpy as np


def GetStocksInfosDataDict(parameter):

    """parameter: library = comp, table= [security, names], observation = int, offset = int, globalWrds =true/false."""
    db = wrds.Connection()
    res = db.get_table(library=parameter.library,
                       table=parameter.table[0])
    db.close()


    d_infos = dict()
    try:
        d_infos = np.load('dict_infos.npy').item()
    except FileNotFoundError:
        print("File doesnt exists")

    for pos in range(res.shape[0]):

        ticker = res['tic'][pos]
        gvkey = res['gvkey'][pos]
        iid = res['iid'][pos]
        cusip = res['cusip'][pos]
        exchg = res['exchg'][pos]

        if np.isnan(exchg) == False:
            exchg = str(int(exchg))

        excntry = res['excntry'][pos]
        ibtic = res['ibtic'][pos]
        isin = res['isin'][pos]
        secstat = res['secstat'][pos]
        sedol = res['sedol'][pos]
        tpci = res['tpci'][pos]

        if cusip == None and isin != None:
            cusip = isin[2:11]
        if cusip != None:
            cusip_8 = cusip[0:8]
        else:
            cusip_8 = None

        stock_id = [{'ticker': ticker, 'ibtic': ibtic, 'iid': iid, 'cusip': cusip,
                     'exhg': exchg, 'excntry': excntry, 'isin': isin,
                     'secstat': secstat, 'sedol': sedol, 'tpci': tpci,
                     'cusip_8': cusip_8}]

        data = {'_id': gvkey, 'company name': None, 'incorporation location': None, 'naics': None,
                'sic': None, 'gic sector': None,'gic ind': None, 'eco zone': None,
                'stock identification': stock_id}

        if gvkey in d_infos:
            d_infos[gvkey]['stock identification'].append(stock_id[0])
        else:
            d_infos[gvkey] = data


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

        if gvkey in d_infos:
            d_infos[gvkey]['company name'] = company if company is not None else d_infos[gvkey]['company name']
            d_infos[gvkey]['incorporation location'] = fic if fic is not None else d_infos[gvkey]['incorporation location']
            d_infos[gvkey]['naics'] = naics if naics is not None else d_infos[gvkey]['naics']
            d_infos[gvkey]['sic'] = sic if sic is not None else d_infos[gvkey]['sic']

    if not parameter.globalWRDS:

        db = wrds.Connection()

        res = db.raw_sql("select a.gvkey, a.fic  from comp.secd a group by a.gvkey, a.fic")

        for pos in range(res.shape[0]):
            gvkey = res['gvkey'][pos]
            fic = res['fic'][pos]

            if gvkey in d_infos:
                d_infos[gvkey]['incorporation location'] = fic if fic is not None else d_infos[gvkey][
                    'incorporation location']

        db.close()
    data = []
    for key in d_infos:
        data.append(d_infos[key])


    np.save('dict_infos.npy', d_infos)
    return parameter.table, 'Completed'



def SetStocksInfosDataInDB(connectionString):

    ClientDB = motor.motor_tornado.MotorClient(connectionString)
    d_infos = np.load('dict_infos.npy').item()
    data = []

    for key in d_infos:
        data.append(d_infos[key])
    tornado.ioloop.IOLoop.current().run_sync(StocksMarketDataInfos(ClientDB, data).SetDataInDB)
    ClientDB.close()
