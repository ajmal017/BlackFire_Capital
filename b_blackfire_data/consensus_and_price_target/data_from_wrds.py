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
              'estcur', 'anndats']





    print('lot : [', x.offset, ", ", x.offset + x.observation, "]")

    db = wrds.Connection()

    res = db.get_table(library= x.library,
                           table= x.table,
                           columns=entete,
                           obs=x.observation,
                           offset= x.offset)

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
        if cusip == None:
            cusip = tic
        pt = spt(cusip, tic, cname, estim, value, hor, cur, date)
        t = pt_db(myclient, pt.get_info())
        t.add_price_target()

    myclient.close()

    db.close()

    return 'lot : [', x.offset, ", ", x.observation + x.offset,"] Price target Completed"


def set_consensus():
    db = wrds.Connection()

    entete = ['ticker', 'cusip', 'cname', 'estimid', 'ireccd',
              'anndats']

    count = db.get_row_count(library="ibes",
                             table="recddet")

    db.close()

    obs_ = 100000
    count = 100000
    iter_ = int(np.round(count / obs_))

    if iter_ * obs_ < count:
        iter_ += 1

    for i in range(iter_):
        print('lot : [', i * obs_, ", ", (i + 1) * obs_, "]")

        db = wrds.Connection()

        res = db.get_table(library="ibes",
                           table="ptgdet",
                           columns=entete,
                           obs=obs_,
                           offset=i * obs_)

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")

        for pos in range(res.shape[0]):

            tic = res[entete[0]][pos]
            cusip = res[entete[1]][pos]
            cname = res[entete[2]][pos]
            estim = res[entete[3]][pos]
            value = res[entete[4]][pos]
            date = res[entete[5]][pos]

            if cusip == None:
                cusip = tic
            pt = scs(cusip, tic, cname, estim, value, date)
            c = cs_db(myclient, pt.get_info())
            c.add_consensus()

        myclient.close()

        db.close()
