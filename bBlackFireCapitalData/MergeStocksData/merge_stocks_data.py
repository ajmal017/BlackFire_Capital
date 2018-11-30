import pymongo
import numpy as np
import collections
import json
import multiprocessing

merge_ibes_data = collections.namedtuple('merge_ibes_data', [
    'stocks_infos_query',
    'type',
    'query_ibes',
])

mean_ibes_data = collections.namedtuple('mean_ibes_data', [
    'date',
    'type',
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

            if act_date != date:
                n_ += 1
                var += float(v['variation'])

        if number != 0:
            var = 0 if n_ == 0 else var / n_
            return {'price_usd': value / number, 'variation': var, 'number': number, 'num_var': n_}
        else:
            return None
    elif type == 'consensus':
        for v in cursor:

            act_date = v['date_activate'][:7]
            act_date = act_date[:6] if act_date[-1] == 'J' else act_date

            number += 1
            recom += float(v['recom'])
            if act_date != date:
                n_ += 1
                var += float(v['variation'])
        try:
            r = var/n_
        except ZeroDivisionError:
            r = None

        if number != 0:
            return {'recom': recom / number, 'variation': r, 'number': number, 'num_var': n_}
        else:
            return None


def set_new_value(actual, new, type):
    #return {'recom': recom / number, 'variation': r, 'number': number, 'num_var': n_}
    #return {'price_usd': value / number, 'variation': var, 'number': number, 'num_var': n_}

    var = actual.get('variation', 0)
    number = actual.get('number', 0)
    num_var = actual.get('num_var', 0)

    if type == "consensus":
        recom = actual.get('recom', 0)
        new['recom'] = (new['recom']*new["number"] + recom*number)/(new["number"] + number)
        new["number"] = number + new["number"]
        try:
            new['var'] = (new['var']*new['num_var'] + var*num_var)/(num_var + new['num_var'])
            new['num_var'] = num_var + new['num_var']
        except ZeroDivisionError, TypeError:
            new['var'] = 0
            new['num_var'] = 0
        return new

    #if type == "price_target":

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

    print(x.date, x.type)
    date = x.date
    stocks_price_db = myclient['stocks_' + date].value
    ibes_db = myclient[x.type + '_' + date].value
    ibes_infos = myclient[x.type + "_infos"].value

    for stocks in stocks_price_db.find():

        gvkey = stocks["gvkey"]
        cusip = stocks["_id"]
        gvkey_ibes = ibes_infos.find({"gvkey": gvkey})
        tab_value = []

        for infos in gvkey_ibes:
            cusip_ibes = infos["_id"]

            for value in ibes_db.find({"cusip": cusip_ibes}):
                tab_value.append(value)
        return_value = get_mean_value(date, tab_value, x.type)
        newvalues = {"$set": {x.type: return_value}}
        stocks_price_db.update_one({'_id': cusip}, newvalues)

    return "done with " + x.type + "for period" + date




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


curr_db = myclient['currency'].USD

def patch_currency(x):

    date = x.date
    curr_ = curr_db[date]
    st_db = myclient["stocks_"+date].value

    for stocks in st_db.find():
        curr_rate = curr_.find_one(stocks['curr'])
        if curr_rate is not None:
            curr_rate = curr_rate['rate']
        else:
            curr_rate = None
        myquery = {"_id": stocks["_id"]}
        stocks["curr_to_usd"] =  curr_rate
        st_db.update_one(myquery, {"$set": stocks})

def set_gvkey_in_ibes(x):

    stocks_infos_query_ibtic = x.stocks_infos_query[1]
    stocks_infos_query_cusip_8 = x.stocks_infos_query[0]
    query_ibes_cusip = x.query_ibes[0]

    stocks_infos = stocks_infos_db.find_one({'stock identification':
                                             {'$elemMatch': stocks_infos_query_ibtic}})

    if stocks_infos is not None:
        pt_infos = myclient[x.type + "_infos"].value

        pt_infos.update({"_id": query_ibes_cusip["cusip"]}, {"$set": { "gvkey": stocks_infos["_id"]}})
        return

    stocks_infos = stocks_infos_db.find_one({'stock identification':
                                             {'$elemMatch': stocks_infos_query_cusip_8}})

    if stocks_infos is not None:
        pt_infos = myclient[x.type + "_infos"].value
        pt_infos.update({"_id": query_ibes_cusip["cusip"]}, {"$set": { "gvkey": stocks_infos["_id"]}})
        return



cusip_data = collections.namedtuple('cusip_data', [
    'date',
])


total = 0
ins = 0

#pt_infos = myclient["consensus_infos"].value
#print("Consensus")
#for value in pt_infos.find():
#    if "gvkey" in value:
#        ins += 1
#    total += 1
#    stocks_infos_query = [{'cusip_8': value["_id"]},{'ibtic': value["ticker"]}]
#    type = 'consensus'
#    query_ibes = [{'cusip': value["_id"]},{'ticker': value["ticker"]}]
#    t += merge_ibes_data(stocks_infos_query, type, query_ibes),

#print(ins, total)
#total = 0
#ins = 0
#pt_infos = myclient["price_target_infos"].value
#print("price_target")
#for value in pt_infos.find():
#    if "gvkey" in value:
#        ins += 1
#    total += 1
#    stocks_infos_query = [{'cusip_8': value["_id"]},{'ibtic': value["ticker"]}]
#    type = 'price_target'
#    query_ibes = [{'cusip': value["_id"]},{'ticker': value["ticker"]}]
#    t += merge_ibes_data(stocks_infos_query, type, query_ibes),

#print(ins, total)

#print(t)
#
#merge_stocks_with_ibes_cusip(t)
#set_gvkey_in_ibes(t[0])
#set_gvkey_in_ibes(t[1])

if __name__ == '__main__':
    tab_month = generate_month("1984M1", "2018M10")
    t = ()
    print("consensus ---------------------------------------------")

    for date in tab_month:
        t += mean_ibes_data(date= date,type = 'consensus'),

    pool = multiprocessing.Pool(processes=16)
    r = pool.map(merge_stocks_with_ibes_cusip, t)
    pool.close()
    pool.join()

    print("price target ---------------------------------------------")
    t = ()
    for date in tab_month:
        t += mean_ibes_data(date= date,type = 'price_target'),

    pool = multiprocessing.Pool(processes=16)
    r = pool.map(merge_stocks_with_ibes_cusip, t)
    pool.close()
    pool.join()
#print(r)
#for date in tab_month:
#    ibes_db = myclient['consensus_' + date].value
#    print(date)
#    for value in ibes_db.find({'ticker': "YHOO"}):
#        print(value)
#    print('')
#    stocks_price_db = myclient['stocks_' + date].value
#    print(date, stocks_price_db.find_one({'gvkey':'062634'}))
#    print('')

#print(json.dumps(stocks_infos_db.find_one({'_id': '015532'}), indent=1))
