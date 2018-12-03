import pymongo


class EconomicsZonesDataInfos:

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

    def SetEconomicsZonesInDB(self):
        "{'country'(_id), 'eco zone', 'name'}"
        try:
            self.database.insert_one(self.data[0])
        except pymongo.errors.DuplicateKeyError:
            print('EconomicsZonesdataInfos.SetEconomicsZonesInDB.DuplicateKeyError', self.data[0]['_id'])

    def GetEconomicsZonesFromDB(self):

        query = self.data[0]
        display = self.data[1]
        tab = []

        for value in self.database.find(query, display):
            tab.append(value)
        return tab