# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 20:05:00 2018

@author: Utilisateur
"""
import pymongo
import wrds
import numpy as np
import collections
from b_blackfire_data.consensus_and_price_target.data_from_db import price_target as pt_db
from b_blackfire_data.consensus_and_price_target.data_from_db import consensus as cs_db
from a_blackfire_class.stocks.informations import price_target as spt
from a_blackfire_class.stocks.informations import stocks_consensus as scs
import os


def test_value(value, v):
    if value is not None:
        if np.isnan(value) == True:
            value = v
    else:
        value = v

    return value


def set_price_target(x):

    entete = ['ticker', 'cusip', 'cname', 'estimid', 'horizon', 'value',
              'estcur', 'anndats','amaskcd']





    print('lot : [', x.offset, ", ", x.offset + x.observation, "]")

    db = wrds.Connection()

    #res = db.get_table(library= x.library,
    #                       table= x.table,
    #                       columns=entete,
    #                       obs=x.observation,
    #                       offset= x.offset)
    res = db.raw_sql("select a.ticker, a.cusip, a.cname, a.estimid, a.horizon, a.value, " +
                     "a.estcur, a.anndats, a.amaskcd from ibes.ptgdet a where a.ticker = '" + x.ticker+"'")

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")



    for pos in range(res.shape[0]):

        tic = res[entete[0]][pos]
        cusip = res[entete[1]][pos]
        cname = res[entete[2]][pos]
        estim = res[entete[3]][pos]
        hor = res[entete[4]][pos]
        value = res[entete[5]][pos]
        cur = res[entete[6]][pos]
        date = res[entete[7]][pos]
        mask_code = res[entete[8]][pos]
        if cusip == None:
            cusip = tic
        pt = spt(cusip, tic, cname, estim, value, hor, cur, date, mask_code)
        t = pt_db(myclient, pt.get_info())
        t.add_price_target()

    myclient.close()

    db.close()

    return 'lot : [', x.offset, ", ", x.observation + x.offset,"] Price target Completed"


def set_consensus(x):

    entete = ['ticker', 'cusip', 'cname', 'estimid', 'ireccd',
              'anndats','amaskcd']

    query = ''

    for word in entete:
        query += 'a.'+word+','

    query = query[:-1]
    print('lot : [', x.offset, ", ", x.offset + x.observation, "]")

    db = wrds.Connection()

    #res = db.get_table(library= x.library,
    #                       table= x.table,
    #                       columns=entete,
    #                       obs=x.observation,
    #                       offset= x.offset)

    res = db.raw_sql("select " + query + " from ibes.recddet a where a.ticker = '" + x.ticker+"'")

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    for pos in range(res.shape[0]):

        tic = res[entete[0]][pos]
        cusip = res[entete[1]][pos]
        cname = res[entete[2]][pos]
        estim = res[entete[3]][pos]
        value = res[entete[4]][pos]
        date = res[entete[5]][pos]
        mask_code = res[entete[6]][pos]
        if cusip == None:
            cusip = tic
        cs = scs(cusip,tic,cname,estim,value,date,mask_code)
        c = cs_db(myclient, cs.get_info())
        c.add_consensus()

    myclient.close()

    db.close()

    return 'lot : [', x.offset, ", ", x.observation + x.offset,"] Consensus Completed"

