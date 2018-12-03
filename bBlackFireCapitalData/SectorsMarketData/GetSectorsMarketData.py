import pymongo
from csv import reader
import numpy as np
import collections
import re
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from aBlackFireCapitalClass.ClassStocksMarketData.ClassStocksMarketDataInfos import  StocksMarketDataInfos
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateMonthlyTab, ClientDB

sector_db = ClientDB

set_sector_tuple = collections.namedtuple('set_sector_tuple', [
    'naics',
    'zone_eco',
])


def SetSectorConstituents():

    """This function classify all the stocks within levels for each economic zone"""

    level_naics = ['3', '2', '1']

    for level in level_naics:

        """Find naics in a particular level"""
        query = {'level': str(level)}
        display = {'title': 0, 'description': 0, 'level': 0}
        tab_naics = SectorsMarketDataInfos(ClientDB, query, display).GetDataFromDB()

        for naics_infos in tab_naics:

            print('level', level,naics_infos)
            naics = naics_infos['_id']  # naics in the level

            """All Naics in sub level"""
            level_inf = str(int(level) + 1)
            query = {'level': str(level_inf), '_id': {"$regex": "^" + naics}}
            display = {'title': 0, 'description': 0, 'level': 0}
            tab_naics_level_inf = SectorsMarketDataInfos(ClientDB, query, display).GetDataFromDB()

            query_naics_stock = [{'naics': {"$regex": "^" + naics}}]
            for sub_naics in tab_naics_level_inf:
                query_naics_stock.append({'naics': {"$not": re.compile("^" + sub_naics['_id'])}})
            print(query_naics_stock)

            """Find All Stocks in a level for the Economic Zone"""

            for zone_eco in ['EUR']:

                new_q = query_naics_stock.append({'eco zone': zone_eco})
                stocks_in_zone = StocksMarketDataInfos(ClientDB, new_q, {}).GetDataFromDB()
                tab_of_cusip = []

                for stocks in stocks_in_zone:
                    tab_of_cusip.append(get_list_of_all_cusip(stocks['stock identification']))









SetSectorConstituents()

query_not_in = {'naics': {'$nin': [{'naics': {"$regex": "^512"}}, {'naics': {"$regex": "^515"}}, {'naics': {"$regex": "^517"}}]}}

query_not_in = {'naics': {'$nin': [{'naics': {"$regex": "^1"}}, {'naics': {"$regex": "^2"}}, {'naics': {"$regex": "^3"}}]}}
query = {'level': '2', '_id': {"$regex": "^51"}}
display = {'title': 0, 'description': 0, 'level': 0}
#tab_naics_level_inf = SectorsMarketDataInfos(ClientDB, query, display).GetDataFromDB()
#print(tab_naics_level_inf)

query = {'$and': [
                    {'_id': {"$regex": "^51"}, 'level': '2'},
                    {'_id': {"$not": re.compile("^511")}, 'level': '2'},
                    {'_id': {"$not": re.compile("^518")}, 'level': '2'}

]
        }
#tab_naics_level_inf = SectorsMarketDataInfos(ClientDB, query, display).GetDataFromDB()
#print(tab_naics_level_inf)

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


def get_mean_of_price_and_ibes_for_sector(tab_price_sector, tab_consensus, tab_price_target):
    tab_price_sector = np.array(tab_price_sector)
    tab_consensus = np.array(tab_consensus)
    tab_price_target = np.array(tab_price_target)

    if tab_price_sector.shape[0] != 0:
        csho = sum(tab_price_sector[:, 0])
        price = sum(tab_price_sector[:, 1]) / sum(tab_price_sector[:, 0])
        vol = sum(tab_price_sector[:, 2])

        consensus_number = sum(tab_consensus[:, 1])
        consensus_number_var = sum(tab_consensus[:, 3])
        if consensus_number != 0:
            recom = sum([x * y for (x, y) in zip(tab_consensus[:, 0], tab_consensus[:, 1])]) / consensus_number
        else:
            recom = 0
        if consensus_number_var != 0:
            var_recom = sum([x * y for (x, y) in zip(tab_consensus[:, 2], tab_consensus[:, 3])]) / consensus_number_var
        else:
            var_recom = 0

        price_target_csho = sum(tab_price_target[:, 1])
        price_target_number_var = sum(tab_price_target[:, 3])

        if price_target_csho != 0:
            price_usd = sum(
                [x * y for (x, y) in zip(tab_price_target[:, 0], tab_price_target[:, 1])]) / price_target_csho
        else:
            price_usd = 0

        if price_target_number_var != 0:
            var_price_target = sum(
                [x * y for (x, y) in zip(tab_price_target[:, 2], tab_price_target[:, 3])]) / price_target_number_var
        else:
            var_price_target = 0

        return [[price, csho, vol], [recom, consensus_number, var_recom, consensus_number_var],
                [price_usd, price_target_csho, var_price_target, price_target_number_var]]
    else:
        return None


def return_price_and_ibes_for_sector(naics, zone_eco, stocks_infos_db, stocks_price_db):
    tab_of_cusip = []
    tab_price_sector = []
    tab_consensus = []
    tab_price_target = []

    zone_query = {'naics': {"$regex": "^" + naics}, 'eco zone': zone_eco}
    stocks_in_zone = stocks_infos_db.find(zone_query)

    for stocks in stocks_in_zone:
        tab_of_cusip.append(get_list_of_all_cusip(stocks['stock identification']))

    for list_cusip in tab_of_cusip:
        tab_price = []
        ibes = True
        for cusip in list_cusip:
            stocks_price = stocks_price_db.find_one({'_id': cusip})
            if stocks_price is not None:
                tab_price.append([stocks_price['csho'],
                                  stocks_price['price_close'] * stocks_price['csho'] / stocks_price[
                                      'curr_to_usd'], stocks_price['vol']])
                if ibes:
                    ibes = False
                    if len(stocks_price['consensus']) != 0:
                        """{'recom': recom / number, 'variation': var, 'number': number, 'num_var': n_}"""
                        tab_consensus.append([stocks_price['consensus']['recom'], stocks_price['consensus']['number'],
                                              stocks_price['consensus']['variation'],
                                              stocks_price['consensus']['num_var']])

                    if len(stocks_price['price_target']) != 0:
                        """{'price_usd': value / number, 'variation': var, 'number': number, 'num_var': n_}"""
                        tab_price_target.append([stocks_price['price_target']['price_usd'], stocks_price['csho'],
                                                 stocks_price['price_target']['variation'],
                                                 stocks_price['price_target']['num_var']])

        tab_price = np.array(tab_price)
        if tab_price.shape[0] > 1:
            mask = (tab_price[:, 1] == max(tab_price[:, 1]))
            max_mc = tab_price[mask]
            tab_price_sector.append(max_mc)

    return get_mean_of_price_and_ibes_for_sector(tab_price_sector, tab_consensus, tab_price_target)



def set_sector_data(x):

    zone_eco = x.zone_eco
    level_naics = ['3', '2', '1']

    for level in level_naics:

        print('level', level)
        """Find naics in a particular level"""
        query = {'level': str(level)}
        display = {'title': 0, 'description': 0, 'level': 0}
        tab_naics = SectorsMarketDataInfos(sector_db['infos'], query, display).GetDataFromDB()

        for naics_infos in tab_naics:

            naics = naics_infos['_id']  # naics in the level

            """Find All Stocks in a level for the Economic Zon"""
            zone_query = {'naics': {"$regex": "^" + naics}, 'eco zone': zone_eco}
            stocks_in_zone = StocksMarketDataInfos(ClientDB['stocks_infos'].value,
                                                   zone_query,).GetDataFromDB()

            level_inf = str(int(level) + 1)
            query = {'level': str(level_inf), '_id': {"$regex": "^" + naics}}
            display = {'title': 0, 'description': 0, 'level': 0}
            tab_naics_level_inf = SectorsMarketDataInfos(sector_db['infos'], query, display).GetDataFromDB()

            d = dict()
            for stocks in stocks_in_zone:
                naics_inf = stocks['naics']

                if naics_inf[:level_inf + 1] not in tab_naics_level_inf:
                    print('not in', naics_inf[:level_inf + 1])
                    print(stocks)
                    d[naics_inf[:level_inf + 1]] = naics_inf[:level_inf + 1]  # naics not in level inf
                    # tab_of_cusip.append(get_list_of_all_cusip(StocksPriceData['stock identification']))

            print(d)

            for date in generate_month('1984M1', '2018M12'):

                stocks_price_db = myclient['stocks_' + date].value
                sector_db = myclient['sector_' + date]
                sector_db = sector_db[zone_eco]
                tab_consensus = []
                tab_price_target = []
                tab_price_sector = []

                for key in d:

                    value = return_price_and_ibes_for_sector(key, zone_eco, stocks_infos_db, stocks_price_db)

                    if value is not None:
                        tab_price_sector.append(value[0])
                        tab_consensus.append(value[1])
                        tab_price_target.append(value[2])

                for value in tab_naics_level_inf:
                    _ = sector_db.find_one({'_id': value})

                    if _ is not None:
                        price_sector = _['StocksPriceData']
                        pt = _['price_target']
                        cs = _['consensus']
                        tab_price_sector.append([price_sector['price'], price_sector['csho'], price_sector['vol']])
                        tab_price_target.append([pt['price'], pt['csho'], pt['variation'], pt['num_var']])
                        tab_consensus.append([cs['recom'], cs['number'], cs['variation'], cs['num_var']])

                value = get_mean_of_price_and_ibes_for_sector(tab_price_sector, tab_consensus, tab_price_target)

                if value is not None:
                    price = value[0]
                    pt = value[2]
                    cs = value[1]

                    price_dict = {'price': price[0], 'csho': price[1], 'vol': price[2]}
                    pt_dict = {'price': pt[0], 'csho': pt[1], 'variation': pt[2], 'num_var': pt[3]}
                    cs_dict = {'recom': cs[0], 'number': cs[1], 'variation': cs[2], 'num_var': cs[3]}

                    to_insert = {'_id': naics, 'StocksPriceData': price_dict, 'price_target': pt_dict, 'consensus': cs_dict}

                    sector_db.insert(to_insert)


def set_sector_return(x):

    zone_eco = x.zone_eco
    tab_date = generate_month('1984M1', '2018M12')

    for per in range(1, len(tab_date)):
        sector_db_ac = myclient['sector_' + tab_date[per]]
        sector_db_ac = sector_db_ac[zone_eco]

        sector_db_pr = myclient['sector_' + tab_date[per - 1]]
        sector_db_pr = sector_db_pr[zone_eco]

        mysec = sector_db_ac.find()

        for sec in mysec:
            naics = sec['_id']
            mysec_p = sector_db_pr.find_one({'_id': naics})

            if mysec_p is not None:
                price_ac = sec['StocksPriceData']['price']
                price_pr = mysec_p['StocksPriceData']['price']

                ret = -1 + price_ac / price_pr
            else:
                ret = None

            sec['StocksPriceData']['ret'] = ret
            sector_db_ac.update_one({'_id': naics}, {'$set': {'StocksPriceData', sec['StocksPriceData']}})


# x = set_sector_tuple('5221', 'FRA')
# set_sector_data(x)
# sector_infos_db = sector_infos_db['1']
# print(sector_infos_db.find().count())

# sector_infos_db_ = sector_infos_db['2']
# print(sector_infos_db_.find_one({'_id': {"$regex": "^" + '522'}}))
# sector_infos_db_ = sector_infos_db['3']

# for x in sector_infos_db_.find({'_id': {"$regex": "^" + '522'}}):
#    print(x)

tab = []
print(sum(tab))
