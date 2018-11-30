from csv import reader
import pymongo
import sys
import numpy as np
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['country_economics_data']

country = [['canada','CAN','CAD','156'],
           ['united states','USA','USD','111'],
           ['austria', 'AUT', 'EUR', '122'],
           ['belgium','BEL', 'EUR', '124'],
           ['denmark', 'DNK', 'DKK', '128'],
           ['finland', 'FIN', 'EUR', '172'],
           ['france', 'FRA', 'EUR', '132'],
           ['germany', 'DEU', 'EUR', '134'],
           ['ireland', 'IRL', 'EUR', '178'],
           ['israel', 'ISR', 'ILS', '436'],
           ['italy', 'ITA', 'EUR', '136'],
           ['netherlands', 'NLD', 'EUR', '138'],
           ['norway', 'NOR', 'NOK', '142'],
           ['portugal', 'PRT', 'EUR', '182'],
           ['spain', 'ESP', 'EUR', '184'],
           ['sweden', 'SWE', 'SEK', '144'],
           ['switzerland', 'CHE', 'CHF', '146'],
           ['united kingdom', 'GBR', 'GBP', '112'],
           ['australia', 'AUS', 'AUD', '193'],
           ['hong kong', 'HKG', 'HKD', '532'],
           ['japan', 'JPN', 'JPY', '158'],
           ['new zealand', 'NZL', 'NZD', '196'],
           ['singapore', 'SGP', 'SGD', '576'],
           ['brazil', 'BRA', 'BRL', '223'],
           ['chile', 'CHL', 'CLP', '228'],
           ['colombia', 'COL', 'COP', '233'],
           ['mexico', 'MEX', 'MXN', '273'],
           ['peru', 'PER', 'PEN', '293'],
           ['czech republic', 'CZE', 'CZK', '935'],
           ['egypt', 'EGY', 'EGP', '469'],
           ['greece', 'GRC', 'EUR', '174'],
           ['hungary', 'HUN', 'HUF', '944'],
           ['poland', 'POL', 'PLN', '964'],
           ['qatar', 'QAT', 'QAR', '453'],
           ['russia', 'RUS', 'RUB', '922'],
           ['south africa', 'ZAF', 'ZAR', '199'],
           ['turkey', 'TUR', 'TRY', '186'],
           ['united arab emirates', 'ARE', 'AED', '466'],
           ['china', 'CHN', 'CNY', '924'],
           ['india', 'IND', 'INR', '534'],
           ['indonesia', 'IDN', 'IDR', '536'],
           ['korea', 'KOR', 'KRW', '542'],
           ['malaysia', 'MYS', 'MYR', '548'],
           ['pakistan', 'PAK', 'PKR', '564'],
           ['philippines', 'PHL', 'PHP', '566'],
           ['taiwan', 'TWN', 'TWD', '528'],
           ['thailand', 'THA', 'THB', '578'],
           ['Euro Area','EUR', 'EUR','163']]

d = dict()
d_curr = dict()
zone_eco = dict()
for c in country:

    d_curr[c[1]] = c[2]
    zone_eco[c[2]] = c[2]
    if c[2] == 'EUR':
        d[c[3]] = c[1]

    else:
        d[c[3]] = c[1]


def add_io_dots_data():

    file = open('io.csv', 'r')
    all_line = reader(file)
    db = mydb['io_eco_zone']
    d_io = dict()
    i = 0
    for c in all_line:
        if i ==0:
            #print(c)
            country_pos = c.index('Country Code')
            indicator_pos = c.index('Indicator Code')
            counterpart_pos = c.index('Counterpart Country Code')
            value_pos = c.index('Attribute')
            start_year = c.index('1990')
            end_year = c.index('2017')

        else:
            country = c[country_pos]
            counter = c[counterpart_pos]
            val = True

            if d_curr.get(d.get(country),False) == 'EUR':
                if country != '163':
                    val = False

            if d_curr.get(d.get(counter),False) == 'EUR':
                if counter != '163':
                    val = False
            if country in d and counter in d and c[value_pos] =='Value' and val==True:
                indic = c[indicator_pos]

                yr = 1990
                t = []
                for x in range(start_year, end_year + 1):
                    t.append(c[x])

                x_ = list(set(t))
                if len(x_) > 0:

                    for x in range(start_year, end_year + 1):
                        value = c[x]
                        if value != '':
                            try:

                                q = {'_id': d_curr[d[counter]], 'value': float(value)}
                                if d_io.get((d[country],indic, str(yr)), False):
                                    d_io[(d[country],indic, str(yr))].append(q)
                                else:
                                    d_io[(d[country],indic, str(yr))] = [q]
                            except ValueError:
                                print(d[country], d[counter], indic, value)
                                sys.exit()


                        yr += 1

        i+=1

    for k in d_io:
        if k[0] == 'EUR':
            print(k, d_io[k])
        country = k[0]
        indic = k[1]
        yr = k[2]
        db_ = db[d_curr[country]]
        indic_db = db_[indic]
        yr_db = indic_db[yr]
        yr_db.insert_many(d_io[k])


def weight_currency_index():
    export_ = 'TXG_FOB_USD'
    new_db = mydb['currency_weight_index']

    for yr in range(1990, 2018):
        d = dict()
        for country in zone_eco:
            t = mydb['io_eco_zone']
            db_ = t[country]
            indic_db = db_['TMG_CIF_USD']
            yr_db = indic_db[yr]

            X = yr_db.aggregate([{'$match': {
                '_id': {'$not': {'$eq': country}}},
            }, {'$group': {'_id': None, 'total': {'$sum': "$value"}, 'count': {'$sum': 1}}}])

            total = 0
            for x in X:
                total = x['total']

            query = yr_db.find({'_id': {'$not': {'$eq': country}}})

            if total != 0:
                for x in query:
                    d[country + '_' + x['_id']+'_i'] = x['value'] / total


        for country in zone_eco:
            t = mydb['io_eco_zone']
            db_ = t[country]
            indic_db = db_[export_]
            yr_db = indic_db[yr]

            X = yr_db.aggregate([{'$match': {
                '_id': {'$not': {'$eq': country}}},
            }, {'$group': {'_id': None, 'total': {'$sum': "$value"}, 'count': {'$sum': 1}}}])

            total = 0
            for x in X:
                total = x['total']

            query = yr_db.find({'_id': {'$not': {'$eq': country}}})

            if total != 0:
                for x in query:
                    d[country + '_' + x['_id']+'_e'] = x['value'] / total


        for country in zone_eco:
            for counter in zone_eco:
                if country != counter:
                    value = 0
                    for thir_part in zone_eco:
                        if thir_part != country and thir_part != counter:
                            exp = d.get((country + '_' + thir_part +'_e'),0)
                            imp_n = d.get((thir_part + '_' + counter +'_i'),0)
                            imp_d = d.get((thir_part + '_' + country +'_i'),0)
                            value += exp*imp_n/(1-imp_d)
                    d[country + '_' + counter + '_t'] = value


        d_ = dict()
        for country in zone_eco:
            for counter in zone_eco:
                if country != counter:
                    d_[(country , counter)] = 0.5*d.get((country+'_' + counter +'_i'),0) +\
                                                0.25*d.get((country+'_' + counter +'_e'),0) +\
                                                0.25*d.get((country+'_' + counter + '_t'), 0)
        s = 0
        for k in d_:
            query = {'country':k[0], 'counter':k[1], 'weight': 100*d_[k]}
            new_db_yr = new_db[yr]
            new_db_yr.insert_one(query)

            #print(k, "{0:.2f}".format(100*d_[k]))


#myclient.drop_database('country_economics_data')
#add_io_dots_data()
#weight_currency_index()
#myclient.close()
new_db = mydb['currency_weight_index']
curr_db = myclient['currency'].USD
d = dict()
for yr in range(2000, 2018):

    db_weight = new_db[yr-1]

    for country in zone_eco:
        weight = db_weight.find({'country': country})
        tab = []
        t = ['date',]
        counter_tab = []
        for x in weight:
            t.append(x['weight'])
            counter_tab.append(x['counter'])
        tab.append(t)
        for m in range(1,13):
            date = str(yr) + 'M' + str(m)
            t = [date]
            db_curr = curr_db[date]
            rate_country = db_curr.find_one({'_id': country})
            if rate_country is not None:
                for counter in counter_tab:
                    rate_counter = db_curr.find_one({'_id': counter})
                    if rate_counter is not None:
                        t.append(rate_counter['rate']/rate_country['rate'])
                    else:
                        t.append(0)
            if len(t)>1:
                tab.append(t)

        tab = np.array(tab)
        if tab.shape[0]>1:
            weight_t = tab[0][1:].astype(float)
            for line in range(1, tab.shape[0]):
                rate_tab = tab[line][1:].astype(float)
                weight_tab_aju = weight_t[:]
                for v in range(len(rate_tab)):
                    if rate_tab[v]==0:
                        weight_tab_aju[v] = 0
                weight_tab_aju = weight_tab_aju/sum(weight_tab_aju)
                prod = 1

                for v in range(len(rate_tab)):
                    prod = prod*(rate_tab[v]**(weight_tab_aju[v]))
                d[(country,tab[line][0])] = prod


new_db = mydb['currency_index']
for k in d:
    country = k[0]
    date= k[1]
    new_db.insert_one({'country': country, 'date':date, 'rate':d[k]})

