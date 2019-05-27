__author__ = 'pougomg'
import pandas as pd
from datetime import date
from sklearn import tree, metrics
from sklearn.ensemble import RandomForestClassifier


class StockSelectionWithMLAlgorithm:

    def __init__(self, data, feature):

        self._data = data
        self._feature = feature

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

            result = data.loc[data['date'].between(date_tab[month-rolling_periods-1], date_tab[month - 1], inclusive=True)]
            train = self._train_data(result[self._feature + ['ranking_ret']])

            result = data.loc[data['date'].between(date_tab[month], date_tab[month], inclusive=True)]
            predict = self._predict_data(train, result[self._feature])

            result.loc[:, 'signal'] = None
            predict = pd.DataFrame(predict.tolist(), index=result.index, columns=['signal'])
            result.loc[result.index, 'signal'] = predict
            tab_result.append(result)

        return pd.concat(tab_result, ignore_index=True)



# StockSelectionWithMLAlgorithm(0).get_signal()