from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import ClientDB

__author__ = 'pougomg'
import wrds
import numpy as np


def SetStocksInfosDataInDB(parameter):

    """parameter: library = comp, table= [security, names], observation = int, offset = int, globalWrds =true/false."""
    db = wrds.Connection()

    res = db.get_table(library=parameter.library,
                       table=parameter.table[0])
    db.close()

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

        StocksMarketDataInfos(ClientDB, data).SetDataInDB()

    db = wrds.Connection()

    res = db.get_table(library=parameter.library,
                       table=parameter.table[1])
    db.close()

    for pos in range(res.shape[0]):

        gvkey = res['gvkey'][pos]
        company = res['conm'][pos]

        if parameter.globalWrds:
            fic = res['fic'][pos]
        else:
            fic = None
        sic = res['sic'][pos]
        naics = res['naics'][pos]

        data = {'_id': gvkey, 'company name': company, 'incorporation location': fic, 'naics': naics,
                'sic': sic, 'gic sector': None, 'gic ind': None, 'eco zone': None,
                'stock identification': None}

        StocksMarketDataInfos(ClientDB, data).SetDataInDB()

    return (parameter.table, 'Completed')





