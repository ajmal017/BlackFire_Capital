# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 23:57:38 2018

@author: Utilisateur
"""
import pymongo
import wrds
import datetime
from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData


"""This funstion set all the currency data from WRDS inside the MongoDB"""


def SetExchangeRatesCurrencyInDB(currency_from):

    ClientDB = pymongo.MongoClient("mongodb://localhost:27017/")

    db = wrds.Connection()

    if currency_from == 'USD':

        entete = ['pubdats', 'curr', 'exrat']

        res = db.get_table(library="ibes",
                           columns=entete,
                           table="hsxrat")

    if currency_from == 'EUR':
        entete = ['sdates', 'curcodef', 'exchrate']

        res = db.get_table(library="ibes",
                           columns=entete,
                           table="eurx")

    if currency_from == 'GBP':
        entete = ['datadate', 'tocurm', 'exratm']

        res = db.get_table(library="comp",
                           columns=entete,
                           table="g_exrt_mth")

    db.close()
    for pos in range(res.shape[0]):
        date = res[entete[0]][pos]
        date_str = str(date.year) + '-' + str(date.month) + '-' + str(date.day)
        d = str(date.year) + 'M' + str(date.month)
        date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

        to = res[entete[1]][pos]
        rate = res[entete[2]][pos]

        """data = {'to', 'from', 'date', 'rate'}"""
        data = {'from': currency_from, 'to': to, 'date': date, 'rate': rate, '_id':to + '_' + d}
        CurrenciesExchangeRatesData(ClientDB, data).SetExchangeRatesInDB()
    ClientDB.close()

    return


# TODO: Create query to get the exchange rate for the last month.