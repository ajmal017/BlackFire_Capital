__author__ = 'pougomg'
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")


country = [['canada','CAN','CAD','7'], ['united states','USA','USD','11','12','14','17'],
           ['austria', 'AUT', 'EUR', '150', '273'],
           ['belgium','BEL', 'EUR', '132', '150', '161', '188', '285', '294'],
           ['denmark', 'DNK', 'DKK', '305', '144'],
           ['finland', 'FIN', 'EUR', '167', '286'],
           ['france', 'FRA', 'EUR', '286', '294', '121', '150', '190', '199', '205', '206', '216', '217', '231'],
           ['germany', 'DEU', 'EUR', '115', '123', '148', '149', '154', '163','165','171','212','257'],
           ['ireland', 'IRL', 'EUR', '150', '172', '173'],
           ['israel', 'ISR', 'ILS', '150', '151', '262', '286'],
           ['italy', 'ITA', 'EUR', '119', '150', '152','160','209','218','230','240','265','267','272','286'],
           ['netherlands', 'NLD', 'EUR', '104', '150', '286','294'],
           ['norway', 'NOR', 'NOK', '224','286'],
           ['portugal', 'PRT', 'EUR', '192', '234'],
           ['spain', 'ESP', 'EUR', '270', '286', '112', '117', '150', '201'],
           ['sweden', 'SWE', 'SEK', '256', '305'],
           ['switzerland', 'CHE', 'CHF', '113', '116','151','159','186','220','254','280'],
           ['united kingdom', 'GBR', 'GBP', '150', '162', '189', '194', '226'],
           ['australia', 'AUS', 'AUD', '101', '106', '126', '169', '207', '232'],
           ['hong kong', 'HKG', 'HKD', '170'],
           ['japan', 'JPN', 'JPY', '153', '156','183','213','227','244','264','293','296'],
           ['new zealand', 'NZL', 'NZD', '108', '225', '277'],
           ['singapore', 'SGP', 'SGD', '251'],
           ['brazil', 'BRA', 'BRL', '238','243'],
           ['chile', 'CHL', 'CLP', '242'],
           ['colombia', 'COL', 'COP', '118'],
           ['mexico', 'MEX', 'MXN', '208'],
           ['peru', 'PER', 'PEN', '191'],
           ['czech republic', 'CZE', 'CZK', '235'],
           ['egypt', 'EGY', 'EGP', '136', '299'],
           ['greece', 'GRC', 'EUR', '107'],
           ['hungary', 'HUN', 'HUF', '134'],
           ['poland', 'POL', 'PLN', '276'],
           ['qatar', 'QAT', 'QAR', '315'],
           ['russia', 'RUS', 'RUB', '211', '224', '255', '275'],
           ['south africa', 'ZAF', 'ZAR', '177'],
           ['turkey', 'TUR', 'TRY', '174'],
           ['united arab emirates', 'ARE', 'AED', '318','323','331'],
           ['china', 'CHN', 'CNY', '249', '250'],
           ['india', 'IND', 'INR', '120', '137', '147', '200', '219'],
           ['indonesia', 'IDN', 'IDR', '175', '258'],
           ['korea', 'KOR', 'KRW', '248', '298'],
           ['malaysia', 'MYS', 'MYR', '304', '181'],
           ['pakistan', 'PAK', 'PKR', '178'],
           ['philippines', 'PHL', 'PHP', '202' ,'203'],
           ['taiwan', 'TWN', 'TWD', '245', '260', '303'],
           ['thailand', 'THA', 'THB', '110']]

zone_eco_db = myclient["zone_infos"]

zone_eco = zone_eco_db["zone_eco"]

for value in country:

    d = {"_id":value[1], "eco zone": value[2], "name": value[0]}
    zone_eco.insert(d)


zone_eco = zone_eco_db["stock_exchange"]

for value in country:
    for st in value[3:]:
        d = {'exhg': st, 'excntry': value[1]}
        zone_eco.insert(d)

stocks_infos_db = myclient["stocks_infos"].value
zone_eco = zone_eco_db["zone_eco"]

for stocks in stocks_infos_db.find():

    id = stocks["_id"]
    inc = stocks["incorporation location"]
    v = zone_eco.find_one({"_id": inc})

    if v is not None:
        stocks_infos_db.update_one({'_id': id}, {"$set": {"eco zone": v["eco zone"]}})