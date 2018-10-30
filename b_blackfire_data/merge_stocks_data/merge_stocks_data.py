import pymongo
import numpy as np
import json

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
stocks_infos_db = myclient['stocks_inofs'].value

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


def get_mean_value(date, cursor, type):
    number = 0
    value = 0
    recom = 0
    var = 0

    if type == 'price_target':
        t = []
        for v in cursor:
            print(v)
            number += 1
            t.append([v['price'], v['curr']])
            act_date = v['date_activate'][:7]
            act_date = act_date[:6] if act_date[-1] == 'J' else act_date
            var += v['variation'] if act_date != date else var
        t = np.array(t)
        curr = np.unique(t[:, 1])
        if len(curr) == 1:
            return {'price': value / number, 'variation': var / number, 'number': number,
                    'curr': curr[0]}
        # TODO implement method for different currency
    elif type == 'consensus':
        for v in cursor:
            print(v)
            number += 1
            recom += v['recom']
            act_date = v['date_activate'][:7]
            act_date = act_date[:6] if act_date[-1] == 'J' else act_date
            var += v['variation'] if act_date != date else var
        return {'recom': recom / number, 'variation': var / number, 'number': number}


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
    stocks_infos = stocks_infos_db.find({'stock identification':
                                             {'$elemMatch': x.stocks_infos_query}})
    # Find all the stocks infos corresponding to the ibes CUSIP/TIC

    tab_month = generate_month('1984M1', '2018M12')

    for infos in stocks_infos:
        gvkey = infos['gvkey']  # get the gvkey

        for date in tab_month:
            add_value_to_stocks_db(date, gvkey, x.query_ibes, x.type)


def merge_stocks_with_curr():
    tab_month = generate_month('2014M1', '2014M1')
    currency_us_db = myclient['currency'].USD

    for date in tab_month:
        stocks_db = myclient['stocks_'+ date].value

        stocks = stocks_db.find()

        for s in stocks:

            curr = s['curr']
            d = currency_us_db[date]

            value = d.find_one({'_id':curr})
            q = {'_id': s['_id']}
            newv = {'$set': {'curr_to_usd': value['rate']}}
            stocks_db.update(q, newv)

#stocks_infos_db = myclient['stocks_2014M1'].value
#print(json.dumps(stocks_infos_db.find_one(), indent=1))
#merge_stocks_with_curr()
stocks_infos_db = myclient['stocks_2010M4'].value
#q = {'naics': { "$regex": "^72"}, 'incorporation location': 'BEL'}
for x in stocks_infos_db.find():
    #print(json.dumps(x, indent=1))
    print(x)
    print('')
