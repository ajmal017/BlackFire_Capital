import pymongo
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


def patch_price_target():
    tab_date = generate_month('1984M1', '2018M12')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    for per in range(1, len(tab_date)):
        pt_db_p = myclient["price_target_" + tab_date[per - 1]].value
        pt_db_a = myclient["price_target_" + tab_date[per]].value
        to_display = {"_id": 0}
        cusip = '45920010'
        mask_code = '128849'
        myquery = {"cusip": cusip}

        for value in pt_db_p.find(myquery, to_display):
            cusip = value['cusip']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = int(value['horizon'])
            previous_price = value['price']
            previous_curr = value['curr']

            myquery = {"cusip": cusip, "mask_code": mask_code}
            to_display = None
            by = 'price'
            x = pt_db_a.find(myquery, to_display).sort(by, 1)

            if x.count() == 0:
                act_date = act_date[:7]
                act_date = act_date[:6] if act_date[-1] == 'J' else act_date
                pos = tab_date.index(act_date)
                if per < pos + hor:
                    # price_target(pt_db_a,value).patch_target_price()
                    pt_db_a.insert_one(value)

            else:
                # print(cusip, ' exists')
                for actual_value in x:
                    # print(actual_value)
                    if actual_value['date_activate'] == value['date_activate']:
                        actual_value['variation'] = value['variation']
                    else:
                        actual_price = actual_value['price']
                        actual_curr = actual_value['curr']

                        if actual_curr == previous_curr:
                            if previous_price != 0:
                                var = (actual_price - previous_price) / previous_price
                                actual_value['variation'] = var

                    newactualvalue = {"$set": actual_value}
                    pt_db_a.update_one({'_id': actual_value['_id']}, newactualvalue)


def patch_consensus():

    tab_date = generate_month('1984M1', '2018M12')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    for per in range(1, len(tab_date)):
        pt_db_p = myclient["consensus_" + tab_date[per - 1]].value
        pt_db_a = myclient["consensus_" + tab_date[per]].value
        to_display = {"_id": 0}
        cusip = '45920010'
        myquery = {"cusip": cusip}

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

#patch_price_target()
#patch_consensus()
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
d = ['admin', 'config', 'local']
#for x in myclient.list_database_names():
#    if x not in d:
#        if x[0:10] == 'consensus_' and x != 'consensus_infos':
#            myclient.drop_database(x)

#    print(x)

tab_date = generate_month('2007M1','2010M12')
print('start-----------------------')
for per in tab_date:

    pt_db = myclient["price_target_" + per].value
    cs_db = myclient["consensus_" + per].value
    cusip = '45920010'
    mask_code = '128849'
    myquery = {"cusip": cusip, 'analyst': 'MORGAN'}

    #to_display = {'price':1,'date_activate':1, 'variation':1,'_id':0, 'mask_code':1, 'analyst':1}
    by = 'price'
    x = pt_db.find(myquery).sort(by, 1)
    print(per)
    for v in x:
        print(v)
    #to_display = {'recom':1,'date_activate':1, 'variation':1,'_id':0, 'mask_code':1, 'analyst':1}
    by = 'recom'
    x = cs_db.find(myquery).sort(by, 1)

    for v in x:
        print(v)
    print('')
