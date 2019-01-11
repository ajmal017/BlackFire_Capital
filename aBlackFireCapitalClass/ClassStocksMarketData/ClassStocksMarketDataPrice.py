import asyncio

import pymongo
from tornado import gen


class StocksMarketDataPrice:
    """This class allows to set and get the price of all the stocks from WRDS. \n" \
    "1. SetStocksPriceInDB is used to save the stock price data in the DB. The inputs " \
    "params (ClientDB, data to save in DB). The data to save is a dictionnary containing: " \
    "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high','price_low'," \
    "'return','ret_usd','curr_to_usd','consensus','price_target'} \n 2. GetStocksPriceInDB is used to get all the" \
    " price data saved in DB. The inputs params (ClientDB, query, data to display)."""

    def __init__(self, database, date, *data):
        self.database = database['stocks']['summary'][date]
        self.data = data

    def __str__(self):
        description = "This class allows to set and get the price of all the stocks from WRDS. \n" \
                      "1. SetStocksPriceInDB is used to save the stock price data in the DB. The inputs " \
                      "params (ClientDB, data to save in DB). The data to save is a dictionnary containing: " \
                      "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high','price_low'," \
                      "'return','ret_usd','curr_to_usd','consensus','price_target'} \n 2. GetStocksPriceInDB is used " \
                      "to get all the price data saved in DB. The inputs params (ClientDB, query, data to display)."

        return description

    async def SetStocksPriceInDB(self):
        """
            :param: {'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',
                    "price_low','return','ret_usd','curr_to_usd','consensus','price_target'}

        """
        try:
            await self.database.bulk_write(self.data[0], ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)

    @gen.coroutine
    def GetStocksPriceFromDB(self):
        tab = []
        query = self.data[0]
        display = self.data[1]
        cursor = self.database.find(query, display).sort('date', 1)
        while (yield cursor.fetch_next):
            tab.append(cursor.next_object())

        return tab

    async def SetManyStocksPriceInDB(self):

        await asyncio.wait([self.SetStocksPriceInDBInside(self.database[data[0]], data[1]) for data in self.data[0]])


    @gen.coroutine
    def UpdateStocksPriceInDB(self):

        query = self.data[0]
        newvalue = self.data[1]

        try:
            yield self.database.update_many(query, {'$set': newvalue})
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)

    @staticmethod
    async def SetStocksPriceInDBInside(ClientDB, data):

        try:
            await ClientDB.bulk_write(data)
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)


    async def GetListOfCurrencyFromDB(self):

        cursor= await self.database.distinct("curr")

        return cursor

    @gen.coroutine
    def SetIndexCreation(self):

        #index = self.data[0]

        yield self.database.create_index([("isin_or_cusip", pymongo.DESCENDING),
                                  ("date", pymongo.DESCENDING),])

    async def GetMontlyPrice(self):

        """ This function is used to find the price of the ends of month for the stocks in DB
        :parameter: Motor.collection for the month, pipeline of the data to return
        [{
        :return: Table of Monthly Price
        """""
        # self.database.adminCommand({'setParameter': 1, 'internalQueryExecMaxBlockingSortBytes':50151432})
        tab = []
        async for doc in self.database.aggregate(self.data[0]):
            tab.append(doc)

        return tab