import pymongo


class StocksMarketDataInfos():

    def __init__(self, database, *data):

        self.database = database['stocks_infos'].value
        self.data = data

    def SetDataInDB(self):

        "{'_id', 'company name', 'incorporation location', 'naics', 'sic', 'gic sector','gic ind'"
        "'eco zone', 'stock identification'}"

        try:
            self.database.insert_one(self.data[0])
        except pymongo.errors.DuplicateKeyError:

            data = self.data[0]
            value = self.database.find_one(data['_id'])

            if data['company name'] is None:
                value['company name'] = data['company name']

            if data['incorporation location'] is None:
                value['incorporation location'] = data['incorporation location']

            if data['naics'] is None:
                value['naics'] = data['naics']

            if data['sic'] is None:
                value['sic'] = data['sic']

            if data['gic sector'] is None:
                value['gic sector'] = data['gic sector']

            if data['gic ind'] is None:
                value['gic ind'] = data['gic ind']

            if data['eco zone'] is None:
                value['eco zone'] = data['eco zone']

            if data['stock identification'] is not None:
                value['stock identification'] = value['stock identification'].append(data['stock identification'])

            self.database.value.update({'_id': data['_id']}, {'$set': value})

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
        self.database.update({'_id': id}, {'$set': value})