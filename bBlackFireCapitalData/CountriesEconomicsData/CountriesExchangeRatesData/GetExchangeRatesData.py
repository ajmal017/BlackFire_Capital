# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 23:57:38 2018

@author: Utilisateur
"""
import motor
import tornado
import wrds
import datetime
from pymongo import UpdateOne

from tornado import gen

from aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData import CurrenciesExchangeRatesData

"""This funstion set all the currency data from WRDS inside the MongoDB"""


def SetExchangeRatesCurrencyInDB(currency_from, connectionString):

    ClientDB = motor.motor_tornado.MotorClient(connectionString)

    db = wrds.Connection()

    if currency_from == 'USD':

        entete = ['anndats', 'curr', 'exrat']

        res = db.get_table(library="ibes",
                           columns=entete,
                           table="hdxrati")

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
    d_result = dict()

    for pos in range(res.shape[0]):

        date = res[entete[0]][pos]
        yr = str(date.year)
        if date.month < 10:
            month = "0" + str(date.month)
        else:
            month = str(date.month)
        if date.day < 10:
            day = "0"+str(date.day)
        else:
            day = str(date.day)

        date_str = str(date.year) + '-' + month + '-' + day
        date = datetime.datetime(date.year, date.month, date.day,16,0,0,0)
        # date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

        to = res[entete[1]][pos]
        rate = res[entete[2]][pos]

        ID = currency_from + '_' + to + '_' + date_str

        """data = {'to', 'from', 'date', 'rate'}"""
        data = {'from': currency_from, 'to': to, 'date': date, 'rate': rate, '_id': ID}

        if d_result.get(ID, False):
            print('true', ID)

        else:
            d_result[ID] = data

    data = []

    for key in d_result:
        data.append(d_result[key])

    tornado.ioloop.IOLoop.current().run_sync(CurrenciesExchangeRatesData(ClientDB, data).SetExchangeRatesInDB)

    ClientDB.close()

    return


