from tornado import gen
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import CurrenciesExchangeRatesDBName


class CurrenciesExchangeRatesData:

    """This class allows to set and to get all the exchange rates values from the DB. \n\n
    1. The SetExchangeRates is used to save of the data with 2 params (ClientDB, dictionnary of value).\n
     2. GetExchangeRates is used to get the rates from the DB with 3 params (ClientDB, query, value to display).\n\n
    This class could be found in aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData"""

    def __init__(self, database, *data):

        self.database = database[CurrenciesExchangeRatesDBName]['exchg_rates']
        self.data = data

    def __str__(self):
        return ("\nThis class allows to set and to get all the exchange rates values from the DB. \n\n" +
                "1. The SetExchangeRates is used to save of the data with 2 params (ClientDB, dictionnary of value).\n"
                + "2. GetExchangeRates is used to get the rates from the DB with 3 params (ClientDB, query, value to "
                "display).\n\n" +
                "This class could be found in "
                "aBlackFireCapitalClass.ClassCurrenciesData.ClassCurrenciesExchangeRatesData")

    @gen.coroutine
    def SetExchangeRatesInDB(self):

        """data = {'to', 'from', 'date', 'rate', '_id'(to + date)}"""

        yield self.database.insert_many(self.data[0])
        count = yield self.database.count_documents({})
        print("Final count: %d" % count)

    @gen.coroutine
    def GetExchangeRatesFromDB(self):

        """this function is to display all the currency pair price. query= {'_id': {'$regex':'^USD_EUR'}}"""
        tab = []
        query = self.data[0]
        display = self.data[1]
        cursor = self.database.find(query, display).sort('date', 1)
        while (yield cursor.fetch_next):
            tab.append(cursor.next_object())

        return tab

    async def GetExchangeRatesEndofMonthFromDB(self):

        """This function is used to get the last rate of the month. query= {'_id': {'$regex':'^USD_EUR_2015-01'}}"""
        query = self.data[0]
        display = self.data[1]
        cursor = self.database.find(query, display).sort('date', -1).limit(1)
        v = None

        for value in await cursor.to_list(length = 1):
            v = value
        return v
