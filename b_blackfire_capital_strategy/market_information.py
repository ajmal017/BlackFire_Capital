__author__ = 'pougomg'
import numpy as np
import pandas as pd
from datetime import date
from a_blackfire_capital_class.sectors import Sectors
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import NAICS
from zBlackFireCapitalImportantFunctions.ConnectionString import TEST_CONNECTION_STRING, PROD_CONNECTION_STRING


class MarketInformation:

    def __init__(self, data, groupby):

        self._data = data
        self._groupby = groupby


if __name__ == '__main__':

    # r = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING)\
    #     .get_monthly_sectors_summary(start_date=date(1999, 1, 1),
    #                                  end_date=date(2017, 12, 31),
    #                                  query={'eco zone': 'USD', 'level': '2'},
    #                                  to_display=None)
    #
    # d= dict()
    # d['header'] = r.columns
    # d['data'] = r
    # np.save('usa_summary_sectors.npy', d)
    tab = []
    for yr in range(1999, 2018):
        r = Sectors(by=NAICS, connection_string=PROD_CONNECTION_STRING)\
            .get_monthly_stocks_summary_with_eco_zone_and_sector_from_mongodb(
            start_date=date(yr, 1, 1), end_date=date(yr, 12, 31),
            query_sector_mapping={'eco zone': 'USD', 'level': '2'},
            to_display=None)
        tab.append(r)

    tab = pd.concat(tab, ignore_index=True)

    d= dict()
    d['header'] = tab.columns
    d['data'] = tab
    np.save('usa_summary_stocks.npy', d)