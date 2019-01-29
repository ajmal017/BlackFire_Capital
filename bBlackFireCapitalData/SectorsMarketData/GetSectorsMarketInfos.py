import tornado
from csv import reader

import motor
from pathlib import Path
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from zBlackFireCapitalImportantFunctions.ConnectionString import ProdConnectionString
from pymongo import InsertOne


def SetSectorInfosInDB():

    """This function save all the naics names and descriptions in the DB"""
    my_path = Path(__file__).parent.parent.parent.resolve()
    file = open(str(my_path) + '/bBlackFireCapitalData/SectorsMarketData/naics_.csv', 'r', encoding="ISO-8859-1")
    file.readline()

    tabToWriteInDB = []

    for entete in file:

        value = list(reader([entete]))[0]
        level = value[0]
        naics = value[2]
        class_title = value[3]
        scri = value[4]
        class_definition = value[5]
        # if int(level) < 3 and scri != 'CAN':
        #     data = InsertOne({'_id': naics, 'title': class_title,
        #             'description': class_definition,
        #             'level': level})
        #     tabToWriteInDB.append(data)
    # ClientDB = motor.motor_tornado.MotorClient(ProdConnectionString)
    # tornado.ioloop.IOLoop.current().run_sync(SectorsMarketDataInfos(ClientDB, tabToWriteInDB).SetDataInDB)
    # ClientDB.close()

    return

def getSectorForLevel(lev):

    my_path = Path(__file__).parent.parent.parent.resolve()
    file = open(str(my_path) + '/bBlackFireCapitalData/SectorsMarketData/naics_.csv', 'r',encoding="ISO-8859-1")

    print(file.readline())

    tabToWriteInDB = []

    for entete in file:

        value = list(reader([entete]))[0]
        level = value[0]
        naics = value[2]
        scri = value[4]
        if int(level) == lev and scri != 'CAN':
            tabToWriteInDB.append(naics)
    return tabToWriteInDB


if __name__ == "__main__":
    # SetSectorInfosInDB()
    print(getSectorForLevel(2))
