# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:07:09 2018

@author: Utilisateur
"""
import multiprocessing
import time
import collections

import numpy as np
import pandas as pd
import pymongo
import psycopg2
import wrds

from aBlackFireCapitalClass.ClassStocksMarketData.informations import stocks_infos as sif
from aBlackFireCapitalClass.ClassStocksMarketData.informations import stocks_data as sdt
from bBlackFireCapitalData.StocksMarketData import infos as st_sif
from bBlackFireCapitalData.StocksMarketData import data as st_sdt

columns = ['gvkey', 'iid', 'curcdd', 'ajexdi', 'cshoc', 'cshtrd',
           'prccd', 'prchd', 'prcld', 'prcod', 'datadate']

table = collections.namedtuple('table', [
    'value', "position",
])


def test_value(value, v):
    if value is not None:
        if np.isnan(value) == True:
            value = v
    else:
        value = v

    return value



def set_data_parrallel_mode(x):

    db = wrds.Connection()

    global set_price

    def set_price(x):

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        tab = x.value
        d = dict()
        d_vol = dict()
        d_prcld = dict()
        d_prchd = dict()

        entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                      'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                      'fic', 'cusip']

        for res in tab:
            #print(res[2])
            gvkey = res[0]
            date = res[1]
            date = str(date.year) + 'M' + str(date.month)
            conm = res[2]
            ajex = test_value(res[3], 1)
            csho = test_value(res[4], 0)
            vol = test_value(res[5], 0)
            prccd = test_value(res[6], 0)
            prchd = test_value(res[7], 0)
            prcld = test_value(res[8], 100000000)
            curcdd = res[9]
            fic = res[10]
            isin = res[11]
            #ret = res[entete[12]]

            if d_vol.get((date, isin), False):
                d_vol[(date, isin)] += vol
            else:
                d_vol[(date, isin)] = vol

            if d_prchd.get((date, isin), False):
                d_prchd[(date, isin)] = max(d_prchd[(date, isin)], prchd)
            else:
                d_prchd[(date, isin)] = prchd

            if d_prcld.get((date, isin), False):
                d_prcld[(date, isin)] = min(d_prcld[(date, isin)], prcld)
            else:
                d_prcld[(date, isin)] = prcld

            d[(date, isin)] = [gvkey, curcdd, csho, vol, ajex, prccd, prchd,
                                   prcld, conm, fic]

        for key in d:

            date = key[0]
            isin = key[1]

            gvkey = d[key][0]
            curcdd = d[key][1]
            csho = d[key][2]
            vol = d[key][3]
            ajex = d[key][4]
            prccd = d[key][5]
            prchd = d[key][6]
            prcld = d[key][7]

            if False == False:
                info = sif(gvkey, d[key][8], d[key][9], None, None, None, None, None, [{}])
                mydb = myclient["stocks_infos"]
                info_to_add = st_sif(mydb, info.get_info())
                info_to_add.add_stock_infos()

            data = sdt(gvkey, date, isin, curcdd, csho, vol, ajex,
                           prccd, prchd, prcld, 0, 0, 0, {}, [])

            mydb = myclient["stocks_"+date]
            data_to_add = st_sdt(mydb, data.get_info())
            data_to_add.add_stock_data()

        myclient.close()
        return 'lot : [', x.position, "] Completed"

    entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'fic', 'cusip']
    res = db.get_table(library=x.library,
                           table=x.table,
                           columns=entete,
                           obs=x.observation,
                           offset= x.offset)

    db.close()
    res = res.values
    count = res.shape[0]
    observ = 200000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1

    pt = ()
    for v in range(iter):

        start = v*observ
        end = (v+1)*observ
        if end > count:
            end = count
        pt += table(value=res[start:end,:], position=x.offset),

    pool = multiprocessing.Pool(processes=5)
    result = pool.map(set_price, pt)
    print(result)
    pool.close()
    pool.join()

    return 'lot : [', x.offset, ", ", x.observation + x.offset,"] Completed"


def set_price(x):

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    res = x.tab
    print(res.shape)
    d = dict()
    d_vol = dict()
    d_prcld = dict()
    d_prchd = dict()

    entete = ['gvkey', 'datadate', 'conm', 'ajexdi', 'cshoc',
                  'cshtrd', 'prccd', 'prchd', 'prcld', 'curcdd',
                  'fic', 'cusip']

    for pos in range(res.shape[0]):

        gvkey = res[entete[0]][pos]
        date = res[entete[1]][pos]
        date = str(date.year) + 'M' + str(date.month)
        conm = res[entete[2]][pos]
        ajex = test_value(res[entete[3]][pos], 1)
        csho = test_value(res[entete[4]][pos], 0)
        vol = test_value(res[entete[5]][pos], 0)
        prccd = test_value(res[entete[6]][pos], 0)
        prchd = test_value(res[entete[7]][pos], 0)
        prcld = test_value(res[entete[8]][pos], 100000000)
        curcdd = res[entete[9]][pos]
        fic = res[entete[10]][pos]
        isin = res[entete[11]][pos]
        #ret = res[entete[12]][pos]

        if d_vol.get((date, isin), False):
            d_vol[(date, isin)] += vol
        else:
            d_vol[(date, isin)] = vol

        if d_prchd.get((date, isin), False):
            d_prchd[(date, isin)] = max(d_prchd[(date, isin)], prchd)
        else:
            d_prchd[(date, isin)] = prchd

        if d_prcld.get((date, isin), False):
            d_prcld[(date, isin)] = min(d_prcld[(date, isin)], prcld)
        else:
            d_prcld[(date, isin)] = prcld

        d[(date, isin)] = [gvkey, curcdd, csho, vol, ajex, prccd, prchd,
                               prcld, conm, fic]
    return

    for key in d:

        date = key[0]
        isin = key[1]

        gvkey = d[key][0]
        curcdd = d[key][1]
        csho = d[key][2]
        vol = d[key][3]
        ajex = d[key][4]
        prccd = d[key][5]
        prchd = d[key][6]
        prcld = d[key][7]


        if x.global_ == False:
            info = sif(gvkey, d[key][8], d[key][9], None, None, None, None, None, [{}])
            mydb = myclient["stocks_infos"]
            info_to_add = st_sif(mydb, info.get_info())
            info_to_add.add_stock_infos()

        data = sdt(gvkey, date, isin, curcdd, csho, vol, ajex,
                       prccd, prchd, prcld, 0, 0, 0, {}, [])

        mydb = myclient["stocks_"+date]
        data_to_add = st_sdt(mydb, data.get_info())
        data_to_add.add_stock_data()

    myclient.close()

    ##set_table_basic_info(global_ = True)


##set_table_basic_info(global_ = False)
# set_price(library = 'comp', table = 'g_secd', global_ = True)
# set_price(library = 'comp', table = 'secd', global_ = False)


# =============================================================================
# Start of the class    
# =============================================================================
class store_global_price():
    columns = ['gvkey', 'iid', 'curcdd', 'ajexdi', 'cshoc', 'cshtrd',
               'prccd', 'prchd', 'prcld', 'prcod', 'datadate']

    def __init__(self, end_position):

        self.end_position = end_position

    def add_global_price_to_db(value):

        #        len_table = db.get_row_count(library='comp', table='g_sec_dprc')
        len_table = 20
        actual_position = value.end_position
        step = 5

        try:
            db = wrds.Connection()

            while actual_position < len_table:
                start_point = actual_position

                res = db.get_table(library='comp',
                                   table='g_sec_dprc',
                                   columns=columns,
                                   obs=step,
                                   offset=start_point)
                start_point = start_point + step

        except psycopg2.OperationalError:
            time.sleep(60)
        #                res = db.get_table(library='comp',
        #                                   table='g_sec_dprc',
        #                                   columns=columns,
        #                                   obs=step,
        #                                   offset= start_point)
        print(res)
        print('')


def get_world_stocks_price(file_name):
    fichier = open(file_name, 'r')

    fichier.readline()
    fichier.readline()
    fichier.readline()
    entete = fichier.readline().rstrip('\n\r').split(' ')
    entete = [i for i in entete if i != '']
    print(entete)
    len_entete = len(entete)
    print(len_entete)
    gvkey_pos = entete.index('gvkey')
    iid_pos = entete.index('iid')
    date_pos = entete.index('datadate')
    ggroup_pos = entete.index('ggroup')
    gind_pos = entete.index('gind')
    gsector_pos = entete.index('gsector')
    loc_pos = entete.index('loc')
    naics_pos = entete.index('naics')
    sic_pos = entete.index('sic')
    isin_pos = entete.index('isin')
    sedol_pos = entete.index('sedol')
    exg_pos = entete.index('exchg')
    secstat_pos = entete.index('secstat')
    ajexdi_pos = entete.index('ajexdi')
    cho_pos = entete.index('cshoc')
    cshtrd_pos = entete.index('cshtrd')
    cur_pos = entete.index('curcdd')
    fic_pos = entete.index('fic')
    prccd_pos = entete.index('prccd')
    prchd_pos = entete.index('prchd')
    prcld_pos = entete.index('prcld')

    df = pd.read_csv(file_name)
    print(df.shape)
    d = dict()
    d_vol = dict()
    d_ph = dict()
    d_pl = dict()

    for line in fichier:

        entete = line.rstrip('\n\r').split(' ')
        entete = [i for i in entete if i != '']
        print("")
        if len(entete) == len_entete and entete[0] != 'gvkey':
            cusip = entete[isin_pos][2:11]
            date = entete[date_pos]
            date = date[0:4] + 'M' + str(int(date[4:6]))
            gvkey = entete[gvkey_pos]
            iid = entete[iid_pos]
            ggroup = entete[ggroup_pos]
            gind = entete[gind_pos]
            gsector = entete[gsector_pos]
            loc = entete[loc_pos]
            naics = entete[naics_pos]
            sic = entete[sic_pos]
            sedol = entete[sedol_pos]
            exg = entete[exg_pos]
            secstat = entete[secstat_pos]
            ajexdi = entete[ajexdi_pos]
            csho = entete[cho_pos]
            vol = entete[cshtrd_pos]
            curr = entete[cur_pos]
            fic = entete[fic_pos]
            prccd = entete[prccd_pos]
            prchd = entete[prchd_pos]
            prcld = entete[prcld_pos]

            if d_vol.get((date, cusip), False):
                d_vol[(date, cusip)] += vol
            else:
                d_vol[(date, cusip)] = vol

            if d_ph.get((date, cusip), False):
                d_ph[(date, cusip)] = max(prchd, d_ph[(date, cusip)])
            else:
                d_ph[(date, cusip)] = prchd

            if d_pl.get((date, cusip), False):
                d_pl[(date, cusip)] = min(prcld, d_pl[(date, cusip)])
            else:
                d_pl[(date, cusip)] = prcld

            if secstat == 0:
                tab = [gvkey,  # 1
                       iid,  # 2
                       ggroup,  # 3
                       gind,  # 4
                       gsector,  # 5
                       loc,  # 6
                       naics,  # 7
                       sic,  # 8
                       sedol,  # 9
                       exg,  # 10
                       secstat,  # 11
                       ajexdi,  # 12
                       csho,  # 13
                       d_vol[(date, cusip)],  # 14
                       curr,  # 15
                       fic,  # 16
                       prccd,  # 17
                       d_ph[(date, cusip)],  # 18
                       d_pl[(date, cusip)]]  # 19

                d[(date, cusip)] = tab[:]
    print(d)
    for key in d:
        stocks_dt = stocks_data(key[0], key[1], tab[14],
                                tab[12], tab[13], tab[11],
                                tab[16], 0, tab[17], tab[18],
                                0, 0, 1, {}, {}, tab[6],
                                tab[7], tab[4], tab[3], tab[5],
                                tab[0], tab[1], tab[2], tab[9],
                                tab[8], tab[10])

        print(stocks_dt.get_info())
        print('')

# get_world_stocks_price('test.txt')
