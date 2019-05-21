__author__ = 'pougomg'
import numpy as np
import pandas as pd
from datetime import date
from sklearn import tree, metrics
from sklearn.ensemble import RandomForestClassifier
import wrds

from a_blackfire_capital_class.displaysheet import DisplaySheetStatistics
from a_blackfire_capital_class.useful_class import MiscellaneousFunctions
from b_blackfire_capital_strategy.input_output import IOStrategy
from b_blackfire_capital_strategy.market_information import MarketInformation
from zBlackFireCapitalImportantFunctions.SetGlobalsFunctions import IO_SUPPLY, IO_DEMAND, STOCKS_MARKET_DATA_DB_NAME


class StockSelectionWithMLAlgorithm:

    def __init__(self, data):

        self._data = data

    @staticmethod
    def _train_data(data):

        X = data.loc[:, data.columns != 'ranking_ret']
        Y = data['ranking_ret'].astype(int).values

        clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=0)
        clf = clf.fit(X, Y)

        return clf

    @staticmethod
    def _predict_data(clf, data):

        return clf.predict(data)

    def get_signal(self, rolling_periods=60):

        data = self._data

        # Create datetime range between start and end date.
        date_tab = pd.date_range(date(2000, 7, 31), date(2017, 12, 31), freq='M').strftime('%Y-%m-%d').tolist()
        tab_result = []

        for month in range(rolling_periods + 1, len(date_tab)):
            # print(date_tab[month-rolling_periods-1], date_tab[month -1])
            result = data.loc[data['date'].between(date_tab[month-rolling_periods-1], date_tab[month - 1], inclusive=True)]
            train = self._train_data(result[['io supply signal', 'io demand signal', 'pt return signal', 'ranking_ret']])

            result = data.loc[data['date'].between(date_tab[month], date_tab[month], inclusive=True)]
            predict = self._predict_data(train, result[['io supply signal', 'io demand signal', 'pt return signal']])

            result['signal'] = None
            result.loc[result.index, 'signal'] = predict.tolist()
            tab_result.append(result)
            # print(result['date'].unique())
            # print(predict)
            #
            # print('')
        return pd.concat(tab_result, ignore_index=True)



# StockSelectionWithMLAlgorithm(0).get_signal()