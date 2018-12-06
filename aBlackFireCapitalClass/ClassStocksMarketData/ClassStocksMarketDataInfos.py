import pymongo

from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import StocksMarketDataInfosDBName


class StocksMarketDataInfos():

    def __init__(self, database, *data):

        self.database = database[StocksMarketDataInfosDBName].value
        self.data = data

    def SetDataInDB(self):

        "{'_id', 'company name', 'incorporation location', 'naics', 'sic', 'gic sector','gic ind'"
        "'eco zone', 'stock identification'}"

        try:
            self.database.insert_one(self.data[0])
        except pymongo.errors.DuplicateKeyError:

            data = self.data[0]
            value = self.database.find_one({"_id": data['_id']})

            if data['company name'] is not None:
                value['company name'] = data['company name']

            if data['incorporation location'] is not None:
                value['incorporation location'] = data['incorporation location']

            if data['naics'] is not None:
                value['naics'] = data['naics']

            if data['sic'] is not None:
                value['sic'] = data['sic']

            if data['gic sector'] is not None:
                value['gic sector'] = data['gic sector']

            if data['gic ind'] is not None:
                value['gic ind'] = data['gic ind']

            if data['eco zone'] is not None:
                value['eco zone'] = not data['eco zone']

            if data['stock identification'] is not None:
                tab = [data['stock identification'][0]]
                for v in value['stock identification']:
                    tab.append(v)
                value['stock identification'] = tab[:]

            self.database.update_one({'_id': data['_id']}, {'$set': value})

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
        self.database.update_one({'_id': id}, {'$set': value})