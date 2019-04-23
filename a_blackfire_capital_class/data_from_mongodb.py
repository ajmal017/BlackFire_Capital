import tornado

import motor
import pymongo
import pandas as pd
import time

import functools
import logging
from tornado.gen import coroutine

from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import CURRENCIES_EXCHANGE_RATES_DB_NAME, \
    CURRENCIES_EXCHANGE_RATES_DB_COL_NAME

CHUNK_SIZE = 10000
MAX_AUTO_RECONNECT_ATTEMPTS = 5

def graceful_auto_reconnect(mongo_op_func):

  """Gracefully handle a reconnection event."""
  @functools.wraps(mongo_op_func)
  def wrapper(*args, **kwargs):
    for attempt in range(MAX_AUTO_RECONNECT_ATTEMPTS):
      try:
        return mongo_op_func(*args, **kwargs)
      except pymongo.errors.AutoReconnect as e:
        wait_t = 0.5 * pow(2, attempt) # exponential back off
        logging.warning("PyMongo auto-reconnecting... %s. Waiting %.1f seconds.", str(e), wait_t)
        time.sleep(wait_t)

  return wrapper

class DataFromMongoDB:

    """
    This Class is used to get and save data to the mongo db.
    """
    def __init__(self, data_table, *data):
        """

        :param data_table: mongo DB Data table instance
        :param data:
        """
        self._data_table = data_table
        self._data = data

    async def set_data_in_db(self):

        """
        This function is used to set the data in the mongo DB
        :return: None
        """
        try:
            result = await self._data_table.bulk_write(self._data[0], ordered=False)
            print('Insertion result %s' % repr(result.bulk_api_result))
        except pymongo.errors.BulkWriteError as bwe:
            result = bwe.details

    async def get_data_from_db(self) -> pd.DataFrame:

        """
        This function is used to get data from a mongo DB collection
        :return: Data frame of the result
        """
        query = self._data[0]
        display = self._data[1]
        cursor = self._data_table.find(query, display)

        records = []
        frames = []
        i = 0

        for document in await cursor.to_list(None):
            records.append(document)
            if i % CHUNK_SIZE == CHUNK_SIZE - 1:
                frames.append(pd.DataFrame(records))
                records = []
            i += 1

        if records:
            frames.append(pd.DataFrame(records))
        return pd.concat(frames)

    async def get_data_from_db_with_pipeline(self) -> pd.DataFrame:

        """ This function is used to perform a query in the DB using a pipeline
        :parameter: pipeline.
        :return: pd.DataFrame of the value in the pipeline.
        """""

        chunk_size = 10000
        records = []
        frames = []
        i = 0
        pipeline = self._data[0]

        async for doc in self._data_table.aggregate(pipeline):
            records.append(doc)
            if i % chunk_size == chunk_size - 1:
                frames.append(pd.DataFrame(records))
                records = []
            i += 1

        if records:
            frames.append(pd.DataFrame(records))
        return pd.concat(frames)

    async def drop_col_from_db(self):

        """
        This fucntion is used to delete a collection from the mongo DB
        """

        await self._data_table.drop_collection(self._data[0])

    @coroutine
    def create_index(self):
        yield self._data_table.create_index(self._data[0])


if __name__ == '__main__':
    start = time.time()
    client_db = motor.motor_tornado.MotorClient(TEST_CONNECTION_STRING)
    db = client_db[CURRENCIES_EXCHANGE_RATES_DB_NAME][CURRENCIES_EXCHANGE_RATES_DB_COL_NAME]
    print(tornado.ioloop.IOLoop.current().run_sync(DataFromMongoDB(db, {}, None).get_data_from_db))
    end = time.time()
    print(start-end)