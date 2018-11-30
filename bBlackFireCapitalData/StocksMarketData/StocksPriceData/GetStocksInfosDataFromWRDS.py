__author__ = 'pougomg'
import wrds
import pymongo
import numpy as np
import collections

table = collections.namedtuple('table', [
    'value', "position",
])

def GetStocksInfosData(parameter):

    """parameter: library = comp, table, observation, offset."""
    db = wrds.Connection()
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["stocks_infos"]
    res = db.get_table(library=parameter.library,
                       table=parameter.table,
                       obs=parameter.observation,
                       offset=parameter.offset)

    res = res.values
    count = res.shape[0]

    def SetStocksInfosInDB(param):

        table = param.table


    db.close()

def set_table_basic_info(x):

    if x.global_:
        g = "g_"
        global_ = "Global Infos"
    else:
        g = ""
        global_ = 'North America'



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
        a = sif(gvkey, None, None, None, None, None, None, None, stock_id)

        info_to_add = st_sif(mydb, a.get_info())
        info_to_add.add_stock_infos()

    res = db.get_table(library="comp", table=g + "names")

    for pos in range(res.shape[0]):

        gvkey = res['gvkey'][pos]
        company = res['conm'][pos]

        if x.global_:
            fic = res['fic'][pos]
        else:
            fic = None
        sic = res['sic'][pos]
        naics = res['naics'][pos]

        stock_id = [{}]
        a = sif(gvkey, company, fic, naics, sic, None, None, None, stock_id)

        info_to_add = st_sif(mydb, a.get_info())
        info_to_add.add_stock_infos()

    myclient.close()


    return global_ + " Infos completed"


