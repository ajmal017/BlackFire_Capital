from csv import reader
from aBlackFireCapitalClass.ClassSectorsMarketData.ClassSectorsMarketDataInfos import SectorsMarketDataInfos
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import ClientDB



def SetSectorInfosInDB():
    """This function save all the naics names and descriptions in the DB"""

    file = open('naics_.csv', 'r')
    file.readline()

    for entete in file:
        value = list(reader([entete]))[0]
        level = value[0]
        naics = value[2]
        class_title = value[3]
        scri = value[4]
        class_definition = value[5]
        if int(level) < 4 and scri != 'CAN':
            data = {'_id': naics, 'title': class_title,
                     'description': class_definition,
                     'level': level}
            SectorsMarketDataInfos(ClientDB, data).SetDataInDB()
            print(level, naics, class_title)

    return

ClientDB.close()

