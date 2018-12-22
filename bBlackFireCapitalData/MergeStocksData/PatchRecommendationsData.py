from datetime import datetime

import motor
import tornado

from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataValues import \
    PriceTargetAndconsensusValuesData
from zBlackFireCapitalImportantFunctions.ConnectionString import TestConnectionString, ProdConnectionString
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import GenerateDailyTab, GenerateMonthlyTab
import collections
import numpy as np
table = collections.namedtuple('table', [
    'start_date', "end_date", "type", "connectionstring"
])


def ReturnQuery(year, month):

    return {"$expr":
                 {"$and":
                      [{"$eq": [{"$year": "$date_activate"}, year]},
                       {"$eq": [{"$month": "$date_activate"}, month]}
                       ]
                  }
             }

def ReturnVariation(tab_, tab_2):
    return


def GetVarPerMaskCode(tabRecommendationPerAnalyst):
    tabRecommendationPerAnalyst.append([datetime(2014, 3, 10), {'cusip': "96638710", "ticker": "WLL"}])
    tabRecommendationPerAnalyst.append([datetime(2014, 3, 9), {'cusip': "WLL", "ticker": "WLL"}])
    tabRecommendationPerAnalyst.append([datetime(2014, 3, 8), {'cusip': "96638710", "ticker": "WLL"}])
    tabRecommendationPerAnalyst.append([datetime(2014, 3, 7), {'cusip': "96638710", "ticker": "96638710"}])
    tab = np.array(tabRecommendationPerAnalyst)

    tab = tab[tab[:,0].argsort()[::-1]]

    print(tab)

    print("")
    done = []
    tab_to_return = []
    for init in range(tab.shape[0]):
        init_2 = init+1
        value = tab[init][1]
        print(value['cusip'])
        print(done)
        find = False
        if tab[init][1]['cusip'] not in done and tab[init][1]['ticker'] not in done:

            for init_ in range(init_2, tab.shape[0]):

                if tab[init][1]['cusip'] == tab[init_][1]['cusip'] or tab[init][1]['ticker'] == tab[init_][1]['ticker']:
                    value = ReturnVariation(value, tab[init_][1])
                    find = True
                if find:
                    break
            done.append(tab[init][1]['cusip'])
            done.append(tab[init][1]['ticker'])
            if find:
                print('u')
                tab_to_return.append([value, 'u'])
            else:
                print('i', value['cusip'])
                tab_to_return.append([value, 'i'])

    return tab_to_return




def GetVariation(tab):
    d = dict()

    for value in tab:

        if str(int(value['mask_code'])) in d:
            d[str(int(value['mask_code']))].append([value['date_activate'], value])
        else:
            d[str(int(value['mask_code']))] = [[datetime(2014,3,11), value]]

    print(len(tab))
    print(len(d))
    tab_result = []
    for key in d:
       if


    GetVarPerMaskCode(d['716'])


def PatchRecommendationsData(params):
    tab_date = GenerateMonthlyTab(params.start_date, params.end_date)
    ClientDB = motor.motor_tornado.MotorClient(params.connectionstring)
    print(tab_date)
    for per in range(1, len(tab_date)):

        value_bf = PriceTargetAndconsensusValuesData(ClientDB, '', params.type,
                                                     ReturnQuery(int(tab_date[per -1][:4]), int(tab_date[per -1][5:])),
                                                     None)
        value_act = PriceTargetAndconsensusValuesData(ClientDB, '', params.type,
                                                      ReturnQuery(int(tab_date[per][:4]), int(tab_date[per][5:])),
                                                      None)
        tab_bf = tornado.ioloop.IOLoop.current().run_sync(value_bf.GetValuesFromDB)
        tab_act = tornado.ioloop.IOLoop.current().run_sync(value_act.GetValuesFromDB)
        np.save('tab_bf',tab_bf)
        np.save('tab_act', tab_act)

        print(tab_bf[0])
        print(len(tab_bf))
        print(len(tab_act))

    ClientDB.close()


# params = table(start_date="2014-3",
#                end_date="2014-4",
#                type='price_target',
#                connectionstring=ProdConnectionString)
#
# PatchRecommendationsData(params)

if __name__ == '__main__':

    tab = np.load('tab_bf.npy')
    var = GetVariation(tab)
