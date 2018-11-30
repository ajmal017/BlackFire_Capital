import pymongo
import collections
import multiprocessing



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


def patch_price_target(input):

    tab_date = generate_month('1984M1', '2018M12')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myquery = input.query
    print(input)

    for per in range(1, len(tab_date)):

        pt_db_p = myclient["price_target_" + tab_date[per - 1]].value
        pt_db_a = myclient["price_target_" + tab_date[per]].value

        query = pt_db_p.find(myquery[0])

        if query.count() == 0:
            query = pt_db_p.find(myquery[1])

        for value in query:

            cusip = value['cusip']
            tic = value["ticker"]
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = int(value['horizon'])
            previous_price = value['price_usd']

            newquery = {'cusip': cusip, 'mask_code': mask_code}
            new_query = pt_db_a.find(newquery)

            if new_query.count() == 0:
                newquery = {'ticker': tic, 'mask_code': mask_code}
                new_query = pt_db_a.find(newquery)

            if new_query.count() == 0:

                act_date = act_date[:7]
                act_date = act_date[:6] if act_date[-1] == 'J' else act_date
                pos = tab_date.index(act_date)
                if per < pos + hor:
                    pt_db_a.insert_one(value)
            else:

                for newvalue in new_query:

                    new_act_date = newvalue['date_activate']
                    if new_act_date != act_date:
                        try:
                            variation = (newvalue['price_usd'] - previous_price) / previous_price
                        except (TypeError, ZeroDivisionError):
                            variation = None
                        pt_db_a.update_one({'_id': newvalue['_id']}, {"$set": {"variation": variation}})

    myclient.close()
    return


def patch_consensus(input):

    print(input.query)
    tab_date = generate_month('1984M1', '2018M12')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myquery = input.query

    for per in range(1, len(tab_date)):

        pt_db_p = myclient["consensus_" + tab_date[per - 1]].value
        pt_db_a = myclient["consensus_" + tab_date[per]].value
        myquery = input.query

        query = pt_db_p.find(myquery[0], {"_id": 0})

        if query.count() == 0:
            query = pt_db_p.find(myquery[1], {"_id": 0})

        for value in query:
            cusip = value['cusip']
            tic = value['ticker']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = 6
            previous_recom = value['recom']

            myquery = {"cusip": cusip, "mask_code": mask_code}
            x = pt_db_a.find(myquery)

            if x.count() == 0:
                myquery = {"ticker": tic, "mask_code": mask_code}
                x = pt_db_a.find(myquery)

            if x.count() == 0:
                act_date = act_date[:7]
                act_date = act_date[:6] if act_date[-1] == 'J' else act_date
                pos = tab_date.index(act_date)
                if per < pos + hor:
                    pt_db_a.insert_one(value)

            else:
                for actual_value in x:
                    if actual_value['date_activate'] == value['date_activate']:
                        actual_value['variation'] = value['variation']
                    else:
                        actual_recom = actual_value['recom']

                        var = (actual_recom - previous_recom)
                        actual_value['variation'] = var

                    newactualvalue = {"$set": actual_value}
                    pt_db_a.update_one({'_id': actual_value['_id']}, newactualvalue)



    myclient.close()

cusip_data = collections.namedtuple('cusip_data', [
    'query',
])
t = ()
#t += cusip_data(query=[{"cusip": "@BNP"}, {"ticker": "@BNP"}]),
#t += cusip_data(query=[{"cusip": "AAPL"}, {"ticker": "AAPL"}]),
#pool = multiprocessing.Pool(processes=2)
#pool.map(patch_price_target, t)

#tab_date = generate_month('1984M1', '2018M12')
#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#pt_infos = myclient["price_target_infos"].value

#print("Patch")

#i = 0
#xx = 0
#for value in pt_infos.find():
#    stocks_infos_query = [{'cusip_8': value["_id"]},{'ibtic': value["ticker"]}]
#    if value["_id"] == "@8W3"and value["ticker"] == "@8W3":
#        xx = i
#    type = 'price_target'
#    query_ibes = [{'cusip': value["_id"]},{'ticker': value["ticker"]}]
#    t += cusip_data(query= query_ibes),
#    i += 1

#print(len(t))
#print(xx)
#pool = multiprocessing.Pool()
#pool.map(patch_consensus, t)



#for date in tab_date:
#    pt_db_p = myclient["price_target_" + date].value
#    print(date)
#    for value in pt_db_p.find({"ticker": 'AAPL'}):
#        print(value)
#    print("")