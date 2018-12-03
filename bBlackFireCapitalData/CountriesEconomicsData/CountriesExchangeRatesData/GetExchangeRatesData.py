# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 23:57:38 2018

@author: Utilisateur
"""
import wrds
import datetime
from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import ClientDB


"""This funstion set all the currency data from WRDS inside the MongoDB"""


def SetExchangeRatesCurrencyInDB(currency_from):

    db = wrds.Connection()

    if currency_from == 'USD':

        entete = ['pubdats', 'exrat', 'curr']

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

    for pos in range(5):
        date = res[entete[0]][pos]
        date_str = str(date.year) + '-' + str(date.month) + '-' + str(date.day)
        date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

        rate = res[entete[1]][pos]
        to = res[entete[2]][pos]
        """data = {'to', 'from', 'date', 'rate'}"""
        data = {'from': currency_from, 'to': to, 'date': date, 'rate': rate, '_id':to + '_' + date_str, 'last_update': datetime.datetime.utcnow()}
        CurrenciesExchangeRatesData(ClientDB, data).SetExchangeRatesInDB()
    return

#SetExchangeRatesCurrencyInDB('USD')


# TODO: Create query to get the exchange rate for the last month.