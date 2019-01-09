import pymongo


class SectorsMarketDataInfos:
    """This class is used to add all the NAICS informations to the DB"""

    def __init__(self, database, *data):

        self.database = database['sector']['naics']
        self.data = data

    async def SetDataInDB(self):

        """"{'_id (NAICS)', 'title', 'description','level'}"""

        try:
            await self.database.bulk_write(self.data[0])
        except pymongo.errors.BulkWriteError as bwe:
            print(bwe.details)

    async def GetDataFromDB(self):

        tab_of_result = []
        query = self.data[0]
        to_display = self.data[1]
        async for value in self.database.find(query, to_display):
            tab_of_result.append(value)

        return tab_of_result

    def UpdateDataInDB(self):

        id = self.data[0]
        value = self.data[1]
        self.database['infos'].update({'_id': id}, {'$set': value})
