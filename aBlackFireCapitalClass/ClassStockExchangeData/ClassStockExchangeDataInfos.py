import pymongo


class StockExchangeDataInfos:

    def __init__(self, database, *data):
        self.database = database['stocks']['stock_exchange']
        self.data = data

    def __str__(self):
        description = "This class allows to save the stock exchange informations for each country is part of.\n1." \
                      " SetStockExchangeInDB save the value in the DB params(ClientDB, data to save). the data input params" \
                      " is {'exchg'(_id), 'exchg country'} \n2." \
                      " GetStockExchangeFromDB retrieve values from the DB params(ClientDB, query to search, data" \
                      "to display."
        return description

    async def SetStockExchangeInDB(self):
        "{'exchg'(_id), 'exchg country'}"
        try:
            self.database.insert_one(self.data[0])
        except pymongo.errors.DuplicateKeyError:
            print('StocksExchangeDataInfos.SetStockExchangeInDB.DuplicateKeyError', self.data[0]['_id'])

    def GetStockExchangeFromDB(self):

        query = self.data[0]
        display = self.data[1]
        tab = []

        for value in self.database.find(query, display):
            tab.append(value)
        return tab