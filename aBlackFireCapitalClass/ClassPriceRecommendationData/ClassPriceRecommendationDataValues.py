import pymongo
from tornado import gen
import multiprocessing
import collections

from concurrent.futures import ProcessPoolExecutor

StocksRec = collections.namedtuple('StocksRec',[
    'month',
    'data',])

class PriceTargetAndconsensusValuesData:

    def __init__(self, database, date, type, *data):

        self.database = database['stocks'][type]
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

    @gen.coroutine
    def SetValuesInDB(self):

        """
            :param: price_target: {'cusip','ticker','analyst','price','horizon','curr','
                                    date_activate','mask_code','variation','price_usd'};

            :param  consensus: {'cusip', 'ticker', 'analyst', 'recom', 'horizon',
                                'date_activate','mask_code','variation'}."""
        yield self.database.insert_many(self.data[0])
        count = yield self.database.count_documents({})
        print("Final count: %d" % count)



    def GetValuesFromDB(self):

        query = self.data[0]
        to_display = self.data[1]

        tab = []

        for value in self.database.find(query, to_display):
            tab.append(value)

        return tab

    def UpdateValuesInDB(self):

        id = self.data[0]
        newvalue = self.data[1]

        self.database.update({'_id': id}, {'$set': newvalue})




