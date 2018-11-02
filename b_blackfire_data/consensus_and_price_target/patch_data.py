import pymongo
import json
from b_blackfire_data.consensus_and_price_target.data_from_db import price_target


def generate_month(start_date, end_date):

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    tab = []
    b = False
    for yr in range(start_year , end_year + 1):

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
    #myquery =
    myquery = input.query

    for per in range(1, len(tab_date)):
        pt_db_p = myclient["price_target_" + tab_date[per - 1]].value
        pt_db_a = myclient["price_target_" + tab_date[per]].value

        for value in pt_db_p.find(myquery):
            print(value)

            cusip = value['cusip']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = int(value['horizon'])
            previous_price = value['price_usd']

            newquery = {'cusip': cusip, 'mask_code': mask_code}
            new_query = pt_db_a.find(newquery)

            if new_query.count() == 0:

                act_date = act_date[:7]
                act_date = act_date[:6] if act_date[-1] == 'J' else act_date
                print('no find ', mask_code, act_date)
                pos = tab_date.index(act_date)
                if per < pos + hor:
                    print('inside')
                    pt_db_a.insert_one(value)
            else:

                for newvalue in new_query:
                    print('find ', newvalue)

                    new_act_date = newvalue['date_activate']
                    if new_act_date != act_date:
                        variation = (newvalue['price_usd'] - previous_price)/ previous_price
                        pt_db_a.update_one({'_id': newvalue['_id']},{ "$set": {"variation": variation } })



    return

    for per in range(1, len(tab_date)):


        to_display = {"_id": 0}

        for value in pt_db_p.find(myquery, to_display):
            print(tab_date[per], value)
            cusip = value['cusip']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = int(value['horizon'])
            previous_price = value['price_usd']

            myquery = {"cusip": cusip, "mask_code": mask_code}
            print(myquery)
            to_display = None
            by = 'price'
            x = pt_db_a.find(myquery, to_display).sort(by, 1)

            if x.count() == 0:
                print(value)
                act_date = act_date[:7]
                act_date = act_date[:6] if act_date[-1] == 'J' else act_date
                pos = tab_date.index(act_date)
                if per < pos + hor:
                    # price_target(pt_db_a,value).patch_target_price()
                    pt_db_a.insert_one(value)

            else:
                print(cusip, ' exists')
                for actual_value in x:
                    print(actual_value)
                    if actual_value['date_activate'] == value['date_activate']:
                        actual_value['variation'] = value['variation']
                    else:
                        actual_price = actual_value['price_usd']
                        actual_curr = actual_value['curr']

                        if previous_price != 0:
                            var = (actual_price - previous_price) / previous_price
                            actual_value['variation'] = var

                    newactualvalue = {"$set": actual_value}
                    pt_db_a.update_one({'_id': actual_value['_id']}, newactualvalue)


def patch_consensus(input):
    print(input)
    tab_date = generate_month('1984M1', '2018M12')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myquery = input.query

    for per in range(1, len(tab_date)):
        pt_db_p = myclient["consensus_" + tab_date[per - 1]].value
        pt_db_a = myclient["consensus_" + tab_date[per]].value
        to_display = {"_id": 0}
        cusip = '45920010'
        myquery = input.query

        for value in pt_db_p.find(myquery, to_display):
            cusip = value['cusip']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = 12
            previous_recom = value['recom']

            myquery = {"cusip": cusip, "mask_code": mask_code}
            to_display = None
            by = 'price'
            x = pt_db_a.find(myquery, to_display).sort(by, 1)

            if x.count() == 0:
                act_date = act_date[:7]
                act_date = act_date[:6] if act_date[-1] == 'J' else act_date
                pos = tab_date.index(act_date)
                if per < pos + hor:
                    pt_db_a.insert_one(value)

            else:
                # print(cusip, ' exists')
                for actual_value in x:
                    # print(actual_value)
                    if actual_value['date_activate'] == value['date_activate']:
                        actual_value['variation'] = value['variation']
                    else:
                        actual_recom = actual_value['recom']
                        var = (actual_recom - previous_recom)
                        actual_value['variation'] = var

                    newactualvalue = {"$set": actual_value}
                    pt_db_a.update_one({'_id': actual_value['_id']}, newactualvalue)

#patch_price_target('')
#patch_consensus()
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#d = ['admin', 'config', 'local']
#for x in myclient.list_database_names():
#    if x not in d:
#        if x[0:13] == 'price_target_' and x != 'price_target_infos':
#            myclient.drop_database(x)

#    print(x)

tab_date = generate_month('2015M1','2018M12')
print('start-----------------------')
for per in tab_date:

    pt_db = myclient["price_target_" + per].value
    cs_db = myclient["consensus_" + per].value
    ticker = '@SQJ'
    myquery = {"ticker": ticker}

#    to_display = {'price':1,'date_activate':1, 'variation':1,'_id':0, 'mask_code':1, 'analyst':1}
#    by = 'price'
#    x = pt_db.find(myquery).sort(by, 1)
    print(per)
#    moy = 0
    n = 0
#    for v in x:

#        if v['variation'] != 0:
    #
    # #            moy += v['variation']
    # #            n+=1
#    print(moy/n)
    #to_display = {'recom':1,'date_activate':1, 'variation':1,'_id':0, 'mask_code':1, 'analyst':1}
    by = 'recom'
    x = cs_db.find(myquery).sort(by, 1)
    moy_rec = 0
    n = 0
    n_ = 0
    moy = 0
    for v in x:
        moy_rec += v['recom']
        if v['variation'] != 0:
            moy += v['variation']
            n_+=1
        n+=1
        #print(v)
    print(moy_rec/n, moy/n_)
    print('')

p = myclient['stocks_infos'].value
print(json.dumps(p.find_one({'_id':'014447'}), indent=1))
