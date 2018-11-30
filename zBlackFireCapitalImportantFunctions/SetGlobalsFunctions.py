import pymongo

ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")
processor = 2


def GenerateMonthlyTab(start_date, end_date):

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


def GetMeanValueOfSectorAgregation(date, cursor, type):

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