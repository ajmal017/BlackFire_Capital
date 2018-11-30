""" This script will build some strategies base ont the sentiment in the market. As input, we """ + \
    """have the StocksPriceData price, price target and consensus group by NAICS. The following strategy """+\
    """will be implemented.\n 1. Does WLD Naics can predict the NAICS that are the bestin position?"""+\
    """\n 2. Can the analyst predict the country where a NAICS will outprform?"""

import pymongo

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

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
zone_eco_db = myclient["zone_infos"].zone_eco
sector_infos_db = myclient['sector_infos']

level_tab = ['1', '2', '3']
tab_naics = []
tab_zone_eco = []

for level in level_tab:
    s = sector_infos_db[level]
    for naics in s.find_one():
        tab_naics.append(naics['_id'])

for zone_eco in zone_eco_db.find():
    tab_zone_eco.append(zone_eco['eco zone'])


def create_tab_of_stocks_sector():

    tab_date = generate_month('2000M1', '2017M12')
    d = dict()
    entete = ['zone', 'type', 'date']

    for naics in tab_naics:
        entete.append(naics)

    for value in entete:
        d[value] = []

    for date in tab_date:
        sector_db = myclient['sector_'+date]

        for zone in tab_zone_eco:
            sector_zone = sector_db[zone]
            for naics in tab_naics:

                sector = sector_zone.find_one({'_id': naics})

                if sector is not None:
                    price_info = sector['StocksPriceData']
                    price_target_info = sector['price_target']
                    consensus_info = sector['consensus']
                else:
                    d['zone'].append(zone)
                    d['type'].append('pt')
                    d['date'].append(date)




