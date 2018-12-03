import pymongo


class CurrenciesExchangeRatesData:

    def __init__(self, database, *data):

        self.database = database['currency']['exchg_rates']
        self.data = data

    def __str__(self):
        return ("""\nThis class allows to set and to get all the exchange rates values from the DB. \n\n""" +
                """1. The SetExchangeRates is used to save of the data with 2 params (ClientDB, dictionnary of value).\n"""
                + "2. GetExchangeRates is used to get the rates from the DB with 3 params (ClientDB, query, value to display).\n\n" +
                "This class could be found in aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData")

    def SetExchangeRatesInDB(self):

        """data = {'to', 'from', 'date', 'rate', '_id'(date + to)}"""

        try:
            self.database.insert(self.data[0])
        except pymongo.errors.DuplicateKeyError:
            print('CurrenciesExchangeRatesData.SetExchangeRates.DuplicateKeyError', self.data[0]['_id'])

    def GetExchangeRatesFromDB(self):

        tab = []
        query = self.data[0]
        display = self.data[1]

        for value in self.database.find(query, display):
            tab.append(value)
        return tab

    def GetExchangeRatesEndofMonthFromDB(self):
        return 0
