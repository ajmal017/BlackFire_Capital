import pymongo


class StocksMarketDataPrice:

    def __init__(self, database, date, *data):

        self.database = database['stocks_' + date].value
        self.data = data

    def __str__(self):
        description = "This class allows to set and get the price of all the stocks from WRDS. \n" \
                      "1. SetStocksPriceInDB is used to save the stock price data in the DB. The inputs " \
                      "params (ClientDB, data to save in DB). The data to save is a dictionnary containing: " \
                      "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high','price_low'," \
                      "'return','ret_usd','curr_to_usd','consensus','price_target'} \n 2. GetStocksPriceInDB is used to get all the" \
                      " price data saved in DB. The inputs params (ClientDB, query, data to display)."

        return description

    def SetStocksPriceInDB(self):

        "{'_id','gvkey','date','curr','csho','vol','adj_factor','price_close','price_high',"
        "price_low','return','ret_usd','curr_to_usd','consensus','price_target'}"

        data = self.data[0]

        try:
            self.database.insert(data)
        except pymongo.errors.DuplicateKeyError:

            myquery = {"_id": data["_id"]}

            mydoc = self.database.find_one(myquery)
            previous_date = mydoc['date']
            new_date = mydoc['date']
            mydoc['vol'] += data['vol']
            mydoc['price_low'] = min(mydoc['price_low'], data['price_low'])
            mydoc['price_high'] = max(mydoc['price_high'], data['price_high'])

            if new_date > previous_date:
                mydoc['csho'] = data['csho']
                mydoc['date'] = data['date']
                mydoc['adj_factor'] = data['adj_factor']
                mydoc['price_close'] = data['price_close']
                mydoc['curr'] = data['curr']

            newvalue = {"$set": mydoc}
            self.database.update_one(myquery, newvalue)
            #print('ClassStocksMarketData.ClassStocksMarketDataPrice.StocksMarketDataPrice.SetStocksPriceInDB.DuplicateKeyError', self.data)

    def GetStocksPriceFromDB(self):

        tab_of_result = []
        query = self.data[0]
        to_display = self.data[1]

        for value in self.database.find(query, to_display):
            tab_of_result.append(value)

        return tab_of_result

    def UpdateStocksPriceInDB(self):

        id = self.data[0]
        newvalue = self.data[1]
        print
        self.database.update_one({'_id': id}, {'$set': newvalue})