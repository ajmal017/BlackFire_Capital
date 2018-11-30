import pymongo



class StocksMarketDataInfos():

    def __init__(self, database, *data):

        self.database = database
        self.data = data

    def SetDataInDB(self):
        print(self.data[0])
        try:
            self.database.insert_one(self.data[0])
        except pymongo.errors.DuplicateKeyError:
            self.database.value.update({'_id': self.data[0]['_id']}, {'$set': self.data[0]})

    def GetDataFromDB(self):

        tab_of_result = []
        query = self.data[0]
        to_display = self.data[1]
        for value in self.database.find(query, to_display):
            tab_of_result.append(value)

        return tab_of_result

    def UpdateDataInDB(self):

        id = self.data[0]
        value = self.data[1]
        self.database.value.update({'_id': id}, {'$set': value})
