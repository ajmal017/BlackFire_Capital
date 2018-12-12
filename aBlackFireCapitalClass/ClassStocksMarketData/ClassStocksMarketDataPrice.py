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

        self.database = database['stocks']['summary']
        self.data = data

    def __str__(self):
        description = "This class allows to set and get the price of all the stocks from WRDS. \n" \
                      "1. SetStocksPriceInDB is used to save the stock price data in the DB. The inputs " \
                      "params (ClientDB, data to save in DB). The data to save is a dictionnary containing: " \
                      "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high','price_low'," \
                      "'return','ret_usd','curr_to_usd','consensus','price_target'} \n 2. GetStocksPriceInDB is used " \
                      "to get all the price data saved in DB. The inputs params (ClientDB, query, data to display)."

        return description

    @gen.coroutine
    def SetStocksPriceInDB(self):

        """
            :param: {'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',
                    "price_low','return','ret_usd','curr_to_usd','consensus','price_target'}

        """

        yield self.database.insert_many(self.data[0])
        count = yield self.database.count_documents({})
        print("Final count: %d" % count)

    def GetStocksPriceFromDB(self):

        tab_of_result = []
        query = self.data[0]
        to_display = self.data[1]

        for value in self.database.find(query, to_display):
            tab_of_result.append(value)

        return tab_of_result

    def UpdateStocksPriceInDB(self):

        id = self.data[0]
        newvalue = self.data[1]
        self.database.update_one({'_id': id}, {'$set': newvalue})