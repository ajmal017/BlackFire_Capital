from tornado import gen

from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import StocksMarketDataInfosDBName


class StocksMarketDataInfos():

    """This Class set all the informations of the stocks."""

    def __init__(self, database, *data):

        self.database = database['stocks'][StocksMarketDataInfosDBName]
        self.data = data

    @gen.coroutine
    def SetDataInDB(self):

        "{'_id', 'company name', 'incorporation location', 'naics', 'sic', 'gic sector','gic ind'"
        "'eco zone', 'stock identification'}"

        yield self.database.bulk_write(self.data[0])
        count = yield self.database.count_documents({})
        print("Final count: %d" % count)

    async def GetDataFromDB(self):

        tab_of_result = []
        query = self.data[0]
        to_display = self.data[1]
        cursor = self.database.find(query, to_display)
        async for document in cursor:
            tab_of_result.append(document)

        return tab_of_result

    @gen.coroutine
    def UpdateDataInDB(self):

        query = self.data[0]
        value = self.data[1]
        yield self.database.update_many(query, {"$set": value})