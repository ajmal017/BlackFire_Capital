import pymongo
from csv import reader
import numpy as np
import pandas as pd

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

def get_ibes(tab):
    pt_price = 0
    pt_var = 0
    pt_num = 0
    sum_prd = 0

    cs_recom = 0
    cs_var = 0
    cs_num = 0
    cs_sum_prd = 0

    for value in tab:
        #price target

        pt = value[1]
        pt_price += pt['price']
        pt_var += pt['variation']
        pt_num += pt['number']
        sum_prd += pt['number']*pt['variation']

        #consensus

        cs = value[0]
        cs_recom += cs['recom']
        cs_var += cs['variation']
        cs_num += cs['number']
        cs_sum_prd += cs['number']*cs['variation']

    pt = {'price': pt_price / pt_num, 'mean variation': pt_var / pt_num, 'number': pt_num,
          'weighted variation': sum_prd / pt_num}

    cs = {'recom': cs_recom / cs_num, 'mean variation': cs_var / cs_num, 'number': cs_num,
          'weighted variation': cs_sum_prd / pt_num}

    return [cs, pt]




def classify_stocks(table_stocks):
    if len(table_stocks) == 1:
        return [table_stocks[0]['isin']]
    else:
        for value in table_stocks:
            return


def add_info():
    n = 0
    file = open('naics_.csv', 'r')
    file.readline()
    for entete in file:

        value = list(reader([entete]))[0]
        level = value[0]
        naics = value[2]
        class_title = value[3]
        scri = value[4]
        class_definition = value[5]
        if int(level) < 4 and scri != 'CAN':
            mydb = myclient['sector_infos'].value
            mydb.insert_one({'_id': naics, 'title': class_title,
                             'description': class_definition})
            print(level, naics, class_title)


def get_stocks_data():

    naics = '486'
    zone = 'EUR'
    date = '2014M1'

    stocks_infos_db = myclient['stocks_infos'].value
    zone_eco_infos_db = myclient['zone_eco_infos'].value

    country_in_zone_eco = zone_eco_infos_db.find({'zone_eco': zone})

    q = {'naics': {"$regex": "^"+naics}, 'eco zone': zone}
    stocks_in_zone = stocks_infos_db.find(q)
    tab_price_sector = []
    tab_ibes = []

    for stocks_infos in stocks_in_zone:
        stocks_preference = classify_stocks(stocks_infos['stock identification'])
        tab = []
        for isin in stocks_preference:
            ibes = False
            st_db = myclient['stocks_'+date].value
            v = st_db.find_one({'_id':isin, 'gvkey':stocks_infos['_id']})

            if v.count()>0:
                tab.append([v['csho'], v['price_close']*v['csho']/v['curr_to_usd'], v['vol']])
                if ibes == False:
                    tab_ibes.append([v['consensus'], v['price_target']])
                    ibes = True
        tab = np.array(tab)
        if tab.shape[0] > 1:
            mask = (tab[:, 1] == max(tab[:, 1]))
            max_mc = tab[mask]
            tab_price_sector.append(max_mc)

    tab_price_sector = np.array(tab_price_sector)
    csho = sum(tab_price_sector[:,0])
    price = sum(tab_price_sector[:,1])/csho
    vol = sum(tab_price_sector[:,2])
    ibes = get_ibes(tab_ibes)
    pt = ibes[1]
    cs = ibes[0]



mydb = myclient['stocks_infos'].value
x = mydb.find()
print(x.count())
