# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 20:05:00 2018

@author: Utilisateur
"""
import wrds
import datetime
import multiprocessing
import collections

from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target, ClientDB, \
    secondary_processor, GenerateMonthlyTab

table = collections.namedtuple('table', [
    'value', "position", "type",
])


def GetStocksPriceRecommendations(params):

    db = wrds.Connection()

    global SetPriceRecommendationsInDB

    if params.type == type_price_target:
        entete = ['ticker', 'cusip', 'cname', 'estimid', 'horizon', 'value',
                  'estcur', 'anndats', 'amaskcd']
    if params.type == type_consensus:
        entete = ['ticker', 'cusip', 'cname', 'estimid', 'ireccd',
                  'anndats', 'amaskcd']

    res = db.get_table(library=params.library,
                       table=params.table,
                       columns=entete,
                       obs=params.observation,
                       offset=params.offset)
    db.close()

    def SetPriceRecommendationsInDB(params):

        tab = params.value

        if params.type == type_price_target:

            for res in tab:

                tic = res[0]
                cusip = res[1]
                cname = res[2]
                estim = res[3]
                hor = res[4]
                value = res[5]
                cur = res[6]
                date = res[71]
                mask_code = res[8]
                if cusip == None:
                    cusip = tic

                date_str = str(date.year) + '-' + str(date.month) + '-' + str(date.day)
                d = str(date.year) + 'M' + str(date.month)
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

                data =  {'_id': cusip, 'comn':cname, 'ticker':tic}
                PriceTargetAndconsensusInfosData(ClientDB,params.type,data).SetInfosInDB()

                data = {'cusip':cusip,'ticker':tic,'analyst':estim,'price':value,'horizon':hor
                ,'curr':cur,'date_activate':date,'mask_code':mask_code,'variation':None,'price_usd':None}

                PriceTargetAndconsensusValuesData(ClientDB,d,params.type,data).SetValuesInDB()

        if params.type == type_consensus:

            for res in range(tab):

                tic = res[0]
                cusip = res[1]
                cname = res[2]
                estim = res[3]
                value = res[4]
                date = res[5]
                mask_code = res[6]
                if cusip == None:
                    cusip = tic

                date_str = str(date.year) + '-' + str(date.month) + '-' + str(date.day)
                d = str(date.year) + 'M' + str(date.month)
                date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

                data = {'_id': cusip, 'comn': cname, 'ticker': tic}
                PriceTargetAndconsensusInfosData(ClientDB, params.type, data).SetInfosInDB()

                "'consensus': {'cusip', 'ticker', 'analyst', 'recom', "" \
                        ""'horizon','date_activate','mask_code','variation'}"
                data = {'cusip': cusip, 'ticker': tic, 'analyst': estim, 'recom': value, 'horizon': 6
                        ,'date_activate': date, 'mask_code': mask_code, 'variation': None}

                PriceTargetAndconsensusValuesData(ClientDB, d, params.type, data).SetValuesInDB()

        return 'lot : [', params.position, "] Completed"

    res = res.values
    count = res.shape[0]
    observ = 200000
    iter = int(count / observ) if count % observ == 0 else int(count / observ) + 1

    pt = ()
    for v in range(iter):

        start = v * observ
        end = (v + 1) * observ
        if end > count:
            end = count
        pt += table(value=res[start:end, :], position=params.offset, type=params.type),

    pool = multiprocessing.Pool(processes=secondary_processor)
    result = pool.map(SetPriceRecommendationsInDB, pt)
    print(result)
    pool.close()
    pool.join()

    return 'lot : [', params.offset, ", ", params.observation + params.offset, "] Completed"


def ConvertPriceTagetToUSD(params):

    date = params.date
    list_sp = PriceTargetAndconsensusValuesData(ClientDB, date, type_price_target, {}, None).GetValuesFromDB()

    for pt in list_sp:

        id = pt['_id']
        curr = pt['curr']
        tab_rate = CurrenciesExchangeRatesData(ClientDB,{'from':'USD', 'to': curr, 'date': date}, None)\
            .GetExchangeRatesEndofMonthFromDB()

        for value in tab_rate:

            try:
                price_usd = pt['price']/value['rate']
            except ZeroDivisionError:
                price_usd = None
            except TypeError:
                price_usd = None

            PriceTargetAndconsensusValuesData(ClientDB, date,type_price_target,id,{'price_usd': price_usd}).UpdateValuesInDB()


def PatchStocksPriceRecommendations(params):

    """This function patch all the data for the Price Target and the Recommendations giver the horizon

    """

    cusip_query = params.query[0]
    ticker_query = params.query[1]

    tab_date = GenerateMonthlyTab("1984M1", "2018M12")

    for per in range(1, len(tab_date)):

        recommendationsPreviousYearData = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per - 1], params.type,
                                                         cusip_query, None).GetValuesFromDB()
        if len(recommendationsPreviousYearData) == 0:
            recommendationsPreviousYearData = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per - 1], params.type, ticker_query,
                                                             None).GetValuesFromDB()
        for value in recommendationsPreviousYearData:

            cusip = value['cusip']
            tic = value['ticker']
            mask_code = value['mask_code']
            act_date = value['date_activate']
            hor = int(value['horizon'])

            if params.type == type_consensus:
                previous = value['recom']
            if params.type == type_price_target:
                previous = value['price_usd']

            newquery = {'cusip': cusip, 'mask_code': mask_code}
            recommendationsActualYeardata = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per], params.type,
                                                         newquery, None).GetValuesFromDB()

            if len(recommendationsActualYeardata) == 0:
                newquery = {'ticker': tic, 'mask_code': mask_code}
                recommendationsActualYeardata = PriceTargetAndconsensusValuesData(ClientDB, tab_date[per], params.type,
                                                                                  newquery, None).GetValuesFromDB()

            if len(recommendationsActualYeardata) == 0:
                act = str(act_date.year) + 'M' + str(act_date.month)
                act_date_pos = tab_date.index(act)

                if per < act_date_pos + hor: #patch if current date < activate date + horizon
                    PriceTargetAndconsensusValuesData(ClientDB,tab_date[per], params.type, value).SetValuesInDB()
            else:

                for actual_value in recommendationsActualYeardata: #if there is a new value don't patch and calculate variation of consensu

                    if act_date != actual_value['date_activate']:
                        if params.type == type_consensus:
                            try:
                                var = actual_value['recom'] - previous
                            except TypeError:
                                var = None

                        if params.type == type_price_target:
                            try:
                                var = (actual_value['price_usd'] - previous) / previous
                            except TypeError:
                                var = None
                            except ZeroDivisionError:
                                var = None

                        PriceTargetAndconsensusValuesData(ClientDB, tab_date[per], params.type,
                                                              actual_value['_id'], {'$set': {"variation": var}})