import pymongo
from tornado import gen


class EconomicsZonesDataInfos:

    """This class allows to save the economics zones where each country is part of.\n1." \
    " SetEconomicsZoneInDB save the value in the DB params(ClientDB, data to save). The " \
    "data input params is {'country'(_id), 'eco zone', 'name'}\n2." \
    " GetEconomicsZoneFromDB retrieve values from the DB params(ClientDB, query to search, data" \
    "to display."""

    def __init__(self, database, *data):
        self.database = database['economics_zones']['infos']
        self.data = data

    def __str__(self):
        description = "This class allows to save the economics zones where each country is part of.\n1." \
                      " SetEconomicsZoneInDB save the value in the DB params(ClientDB, data to save). The " \
                      "data input params is {'country'(_id), 'eco zone', 'name'}\n2." \
                      " GetEconomicsZoneFromDB retrieve values from the DB params(ClientDB, query to search, data" \
                      "to display."
        return description

    @gen.coroutine
    def SetEconomicsZonesInDB(self):
        """{'country'(_id), 'eco zone', 'name'}"""

        yield self.database.insert_many(self.data[0])
        count = yield self.database.count_documents({})
        print("Final count: %d" % count)

    @gen.coroutine
    def GetEconomicsZonesFromDB(self):

        query = self.data[0]
        display = self.data[1]
        cursor = self.database.find(query, display)
        tab = []

        while (yield cursor.fetch_next):
            tab.append(cursor.next_object())

        return tab