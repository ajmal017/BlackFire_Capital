import tornado
from csv import reader

import motor

from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString
from pymongo import InsertOne


def SetSectorInfosInDB():

    """This function save all the naics names and descriptions in the DB"""

    file = open('naics_.csv', 'r')
    file.readline()

    tabToWriteInDB = []

    for entete in file:

        value = list(reader([entete]))[0]
        level = value[0]
        naics = value[2]
        class_title = value[3]
        scri = value[4]
        class_definition = value[5]
        if int(level) < 3 and scri != 'CAN':
            data = InsertOne({'_id': naics, 'title': class_title,
                    'description': class_definition,
                    'level': level})
            tabToWriteInDB.append(data)
    ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    tornado.ioloop.IOLoop.current().run_sync(SectorsMarketDataInfos(ClientDB, tabToWriteInDB).SetDataInDB)
    ClientDB.close()

    return


if __name__ == "__main__":
    SetSectorInfosInDB()
