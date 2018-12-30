import pymongo
from tornado import gen

class PriceTargetAndconsensusValuesData:

    def __init__(self, database, date, type, *data):

        self.database = database['stocks'][type][date]
        self.data = data
        self.type = type

    def __str__(self):
        description = "This class allows to set and get Recommendations Data.\n1." \
                      " SetValuesInDb store the last monthly recommendations for " \
                      "each analyst. It takes 4 params (ClientDB, date, type, data to save" \
                      "type = 'price_target/consensus'. The data to store depends on the type" \
                      "'price_target': {'cusip','ticker','analyst','price','horizon'," \
                      "'curr','date_activate','mask_code','variation','price_usd'}, " \
                      "'consensus': {'cusip', 'ticker', 'analyst', 'recom','horizon'," \
                      "'date_activate','mask_code','variation'}. \2. GetValuesInDB is used to" \
                      "get values from the DB. Ittakes 5 params(ClientDB, date, type, query, values to display."

        return description


    async def SetValuesInDB(self):

        """
            :param: price_target: {'cusip','ticker','analyst','price','horizon','curr','
                                    date_activate','mask_code','variation','price_usd'};

            :param  consensus: {'cusip', 'ticker', 'analyst', 'recom', 'horizon',
                                'date_activate','mask_code','variation'}."""

        try:
            await self.database.bulk_write(self.data[0])
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)


    @gen.coroutine
    def GetValuesFromDB(self):

        tab = []
        query = self.data[0]
        display = self.data[1]
        cursor = self.database.find(query, display).sort('date', 1)
        while (yield cursor.fetch_next):
            tab.append(cursor.next_object())

        return tab

    @gen.coroutine
    def UpdateValuesInDB(self):

        try:
            yield self.database.bulk_write(self.data[0], ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)





