import pymongo
import numpy as np
import collections
import json

merge_ibes_data = collections.namedtuple('merge_ibes_data', [
    'stocks_infos_query',
    'type',
    'query_ibes',
])

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
stocks_infos_db = myclient['stocks_infos'].value


def generate_month(start_date, end_date):
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    tab = []
    b = False
    for yr in range(start_year, end_year + 1):

        for month in range(1, 13):
            date = str(yr) + 'M' + str(month)
            if date == start_date:
                b = True
            if b:
                tab.append(date)
            if date == end_date:
                break
    return tab


def get_list_of_all_cusip(tab_cusip):
    t = []
    for infos in tab_cusip:

        if infos['isin'] != None:
            if infos['isin'][0:2] != 'US' and infos['isin'][0:2] != 'CA':
                t.append(infos['isin'])
            else:
                t.append(infos['cusip'])
        elif infos['cusip'] != None:
            t.append(infos['cusip'])

    return t


def get_mean_value(date, cursor, type):
    number = 0
    n_ = 0
    value = 0
    recom = 0
    var = 0

    if type == 'price_target':
        for v in cursor:
            number += 1
            value += v['price_usd']
            act_date = v['date_activate'][:7]
            act_date = act_date[:6] if act_date[-1] == 'J' else act_date
            var += v['variation'] if act_date != date else var
            n_ += 1 if act_date != date else var
        if number != 0:
            var = 0 if n_ == 0 else var / n_
            return {'price_usd': value / number, 'variation': var, 'number': number}
        else:
            return None
    elif type == 'consensus':
        for v in cursor:
            number += 1
            recom += v['recom']
            act_date = v['date_activate'][:7]
            act_date = act_date[:6] if act_date[-1] == 'J' else act_date
            var += v['variation'] if act_date != date else var
            n_ += 1 if act_date != date else var
        if number != 0:
            var = 0 if n_ == 0 else var / n_
            return {'recom': recom / number, 'variation': var, 'number': number}
        else:
            return None


def add_value_to_stocks_db(date, gvkey, query_ibes, type):
    ibes_db = myclient[type + "_" + date].value
    stocks_db = myclient['stocks_' + date].value

    _ = stocks_db.find({'gvkey': gvkey})
    __ = ibes_db.find(query_ibes)
    _ = [1]
    if len(_) > 0 and __.count() > 0:
        value = get_mean_value(date, __, type)  # get the mean value of all the Forecast
        for stocks in _:
            if (len(stocks[type])) == 0:
                q = {'_id': stocks['_id']}
                newv = {'$set': {type, value}}
                _.update(q, newv)


def merge_stocks_with_ibes_cusip(x):
    """stocks infos query is cusip_8 or ibes_tic"""
    """x.stocks_infos_query: cusip_8 or ibtic"""
    """x.type: consensus or price_target"""
    """x.query_ibes: ticker or cusip"""

    stocks_infos_query = x.stocks_infos_query
    type = x.type
    query_ibes = x.query_ibes

    stocks_infos = stocks_infos_db.find({'stock identification':
                                             {'$elemMatch': stocks_infos_query}})
    # Find all the stocks infos corresponding to the ibes CUSIP/TIC

    tab_month = generate_month('1984M1', '2018M12')
    list_cusip = []

    for infos in stocks_infos:
        list_cusip = list_cusip + get_list_of_all_cusip(infos['stock identification'])

    for date in tab_month:
        stocks_price_db = myclient['stocks_' + date].value
        ibes_db = myclient[type + '_' + date].value

        value_to_add = get_mean_value(date, ibes_db.find(query_ibes), type)
        if value_to_add is not None:

            for cusip in list_cusip:

                cur = stocks_price_db.find({'_id': cusip})
                if cur.count() > 0:
                    newvalues = {"$set": {type: value_to_add}}
                    stocks_price_db.update_many({'_id': cusip}, newvalues)


def merge_stocks_with_curr():
    tab_month = generate_month('2014M1', '2014M1')
    currency_us_db = myclient['currency'].USD

    for date in tab_month:
        stocks_db = myclient['stocks_' + date].value

        stocks = stocks_db.find()

        for s in stocks:
            curr = s['curr']
            d = currency_us_db[date]

            value = d.find_one({'_id': curr})
            q = {'_id': s['_id']}
            newv = {'$set': {'curr_to_usd': value['rate']}}
            stocks_db.update(q, newv)


# stocks_infos_db = myclient['stocks_2014M1'].value
# print(json.dumps(stocks_infos_db.find_one(), indent=1))
# merge_stocks_with_curr()
# stocks_infos_db = myclient['stocks_2010M4'].value
# q = {'naics': { "$regex": "^72"}, 'incorporation location': 'BEL'}
# for x in stocks_infos_db.find():
# print(json.dumps(x, indent=1))
#    print(x)
#    print('')

#stocks_infos_query = {'ibtic': '@BNP'}
#type = 'price_target'
#query_ibes = {'ticker': '@BNP'}

#x = merge_ibes_data(stocks_infos_query, type, query_ibes)
#merge_stocks_with_ibes_cusip(x)

#tab_month = generate_month('1984M1', '2018M12')

# for date in tab_month:
#    stocks_price_db = myclient['stocks_' + date].value
#    print(date, stocks_price_db.find_one({'_id':'FR0000140063'}))

#print(json.dumps(stocks_infos_db.find_one({'_id': '015532'}), indent=1))
