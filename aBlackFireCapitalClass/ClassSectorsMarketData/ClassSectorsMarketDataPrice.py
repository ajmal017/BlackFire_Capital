import pymongo
from tornado import gen


class SectorsMarketDataPrice:

    def __init__(self, database, *data):

        self.database = database['sector']['summary']
        self.data = data

    async def SetSectorsPriceInDB(self):
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
        cursor = self.database.find(query, display)
        while (yield cursor.fetch_next):
            tab.append(cursor.next_object())

        return tab

    def UpdateDataInDB(self):

        id = self.data[0]
        value = self.data[1]
        self.database.value.update({'_id': id}, {'$set': value})

    async def create_index(self):

        await self.database.create_index([("naics", pymongo.DESCENDING),
                                  ("date", pymongo.DESCENDING),("eco zone", pymongo.DESCENDING)])