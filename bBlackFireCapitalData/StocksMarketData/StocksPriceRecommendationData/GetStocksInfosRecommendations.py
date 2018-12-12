import motor
import tornado
from aBlackFireCapitalClass.ClassPriceRecommendationData.ClassPriceRecommendationDataInfos import \
    PriceTargetAndconsensusInfosData
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import type_consensus, type_price_target

__author__ = 'pougomg'
import wrds

def SetStocksInfosRecommendationsInDB(type, connectionstring):

    """
        This fucntion set all the Stocks Recommendations Infos in the DB.
        :param: type: price_target/consensus DB
        :param: connectionstring. The DB location where the data will be store.

    """

    if type == type_consensus:
        db = wrds.Connection()
        res = db.raw_sql("select a.cusip, a.ticker from ibes.recddet a group by a.cusip, a.ticker")
        db.close()
    elif type == type_price_target:
        db = wrds.Connection()
        res = db.raw_sql("select a.cusip, a.ticker from ibes.ptgdet a group by a.cusip, a.ticker")
        db.close()
    else:
        error = "Incorrection Argument Type It must be {} or {}."
        raise TypeError(error.format(type_price_target, type_consensus))

    dict_infos = dict()
    for pos in range(res.shape[0]):
        cusip = res['cusip'][pos]
        ticker = res['ticker'][pos]

        if cusip is None:
            cusip = ticker

        dict_infos[(cusip, ticker)] = {'ticker': ticker, 'cusip': cusip}

        if (cusip != ticker):
            if dict_infos.get((ticker, ticker), False):
                del dict_infos[(ticker, ticker)]
    data = []
    for key in dict_infos:
        data.append(dict_infos[key])
    ClientDB = motor.motor_tornado.MotorClient(connectionstring)
    tornado.ioloop.IOLoop.current().run_sync(PriceTargetAndconsensusInfosData(ClientDB,type,data).SetInfosInDB)
    ClientDB.close()
