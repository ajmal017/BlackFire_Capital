import pymongo


class PriceTargetAndconsensusInfosData:

    def __init__(self, database,type, *data):

        self.database = database[type + '_infos'].value
        self.data = data
        self.type = type

    def __str__(self):
        description = "This call allow to get and set recommandations data Infos from the Db. \n1." \
                      "SetInfosInDB set the infos in the DB. The params are (ClientDB, type, data to save)." \
                      "type = 'price_target/consensus. data = {'cusip'(_id), 'comn', ticker}. \2." \
                      "GetInfosfromDB get the recommendations infos from the DB. Its take 4 params(ClientDB, type," \
                      "query, data to display)."
        return description

    def SetInfosInDB(self):

        """ {'cusip'(_id), 'comn', ticker}"""

        data = self.data[0]
        ticker = data['ticker']
        self.database.insert(data)

        r = self.database.find({'_id':ticker, 'ticker': ticker})

        # if r is not None and data['ticker'] != data['_id']:
        #     self.database.delete_one({'_id': ticker, 'ticker': ticker})
        # try:
        # except pymongo.errors.DuplicateKeyError:
        #     e = "ClassPriceRecommendationData.ClasspricerecommendationDataInfos.SetinfosInDB.DuplicateKeyError " \
        #         "" + self.type +' '+ data['_id']

    def GetInfosFromDB(self):

        query = self.data[0]
        display = self.data[1]
        tab = []

        for value in self.database.find(query, display):
            tab.append(value)
        return tab

    def UpdateInfosInDB(self):

        id = self.data[0]
        query = self.data[1]
        self.database.update_one({"_id": id}, query)