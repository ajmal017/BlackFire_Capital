import pymongo
from tornado import gen


class PriceTargetAndconsensusInfosData:

    def __init__(self, database,type, *data):

        self.database = database['stocks'][type + '_infos']
        self.data = data
        self.type = type

    def __str__(self):
        description = "This call allow to get and set recommandations data Infos from the Db. \n1." \
                      "SetInfosInDB set the infos in the DB. The params are (ClientDB, type, data to save)." \
                      "type = 'price_target/consensus. data = {'cusip'(_id), 'comn', ticker}. \2." \
                      "GetInfosfromDB get the recommendations infos from the DB. Its take 4 params(ClientDB, type," \
                      "query, data to display)."
        return description

    @gen.coroutine
    def SetInfosInDB(self):

        """ {'cusip'(_id), 'comn', ticker}"""
        try:
            yield self.database.bulk_write(self.data[0])
            count = yield self.database.count_documents({})
            print("Final count: %d" % count)
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)
            #you can also take this component and do more analysis
            #werrors = bwe.details['writeErrors']
            raise

    @gen.coroutine
    def GetInfosFromDB(self):

        query = self.data[0]
        display = self.data[1]
        tab = []
        cursor = self.database.find(query, display)

        while (yield from cursor.fetch_next) :
            tab.append(cursor.next_object())
        return tab

    @gen.coroutine
    def UpdateDataInDB(self):

        query = self.data[0]
        value = self.data[1]
        yield self.database.update_many(query, {"$set": value})