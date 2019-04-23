__author__ = 'pougomg'

from itertools import groupby

import numpy as np
import pandas as pd
from scipy.stats import linregress


def roll(df, w, **kwargs):

    roll_array = np.dstack([df.values[i:i+w, :] for i in range(len(df.index) - w + 1)]).T
    panel = pd.Panel(roll_array,
                     items=df.index[w-1:],
                     major_axis=df.columns,
                     minor_axis=pd.Index(range(w), name='roll'))
    return panel.to_frame().unstack().T.groupby(level=0, **kwargs)


def aggregate_returns(returns, convert_to):

    """
	Aggregates returns by day, week, month, or year.
	"""

    def cumulate_returns(x):
        """

		:param x:
		:return:
		"""
        return np.exp(np.log(1 + x).cumsum()).iloc[-1, 0] - 1

    if convert_to == 'weekly':
        return returns.groupby(
            [lambda x: x.year,
             lambda x: x.month,
             lambda x: x.isocalendar()[1]]).apply(cumulate_returns)
    elif convert_to == 'monthly':
        return returns.groupby(
            [lambda x: x.year, lambda x: x.month]).apply(cumulate_returns)
    elif convert_to == 'yearly':
        return returns.groupby(
            [lambda x: x.year]).apply(cumulate_returns)
    else:
        ValueError('convert_to must be weekly, monthly or yearly')


def create_cagr(equity, periods=252):
    """
	Calculates the Compound Annual Growth Rate (CAGR)
	for the portfolio, by determining the number of years
	and then creating a compound annualised rate based
	on the total return.
	Parameters:
	equity - A pandas Series representing the equity curve.
	periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
	"""
    years = len(equity) / float(periods)
    return (equity.iloc[-1, 0] ** (1.0 / years)) - 1.0


def create_sharpe_ratio(returns, periods=252):
    """
	Create the Sharpe ratio for the strategy, based on a
	benchmark of Bonds (i.e. no risk-free rate information).
	Parameters:
	returns - A pandas Series representing period percentage returns.
	periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
	"""

    def calculate_sharpe_ratio(rolling):
        try:
            return ((rolling['return'] - rolling['bonds']).mean())/rolling['return'].std()

        except:
            return None

    tab = returns[['bonds']]/100
    # Annualised PF returns
    tab.loc[:, 'return'] = (1 + returns.iloc[:, 0]) ** periods - 1

    # rolling
    # rolling_sharpe = roll(tab, periods).apply(calculate_sharpe_ratio)
    rolling = tab.loc[:, 'return'].rolling(window=periods)
    rolling_sharpe = np.sqrt(periods) * (
            rolling.mean() / rolling.std()
        )

    sharpe = tab['return'].mean()/tab['return'].std()

    return sharpe, rolling_sharpe


def create_sortino_ratio(returns, periods=252):
    """
	Create the Sortino ratio for the strategy, based on a
	benchmark of zero (i.e. no risk-free rate information).
	Parameters:
	returns - A pandas Series representing period percentage returns.
	periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
	"""
    return (np.sqrt(periods) * (np.mean(returns)) / np.std(returns[returns < 0])).values[0]


def create_drawdowns(returns):
    """
	Calculate the largest peak-to-trough drawdown of the equity curve
	as well as the duration of the drawdown. Requires that the
	pnl_returns is a pandas Series.
	Parameters:
	equity - A pandas Series representing period percentage returns.
	Returns:
	drawdown, drawdown_max, duration
	"""
    # Calculate the cumulative returns curve
    # and set up the High Water Mark
    idx = returns.index
    hwm = np.zeros(len(idx))

    # Create the high water mark
    for t in range(1, len(idx)):
        hwm[t] = max(hwm[t - 1], returns.iloc[t, 0])

    # Calculate the drawdown and duration statistics
    perf = pd.DataFrame(index=idx)
    perf["Drawdown"] = (hwm - returns.iloc[:, 0]) / hwm
    perf["Drawdown"].ix[0] = 0.0
    perf["DurationCheck"] = np.where(perf["Drawdown"] == 0, 0, 1)
    duration = max(
        sum(1 for i in g if i == 1)
        for k, g in groupby(perf["DurationCheck"])
    )
    return perf["Drawdown"], np.max(perf["Drawdown"]), duration


def rsquared(x, y):
    """
	Return R^2 where x and y are array-like.
	"""
    slope, intercept, r_value, p_value, std_err = linregress(x, y.iloc[:, 0])
    return r_value ** 2


def turnover(returns):
    t = returns.groupby('date')['constituent'].apply(lambda x: set(x.values.tolist()))
    t = t.combine(t.shift(), lambda a, b: len(b - a) / len(b) if isinstance(b, set) else np.nan).dropna()
    return t.mean(), t


def strategy_returns(portfolio, methods='market_cap_weighted'):

    def calculate_return(group):
        return (group['weight'] * group['return']).sum() / group['weight'].sum()

    if methods == 'market_cap_weighted':
        portfolio.loc[:, 'weight'] = portfolio.loc[:, 'mc']
    elif methods == 'equal_weighted':
        portfolio.loc[:, 'weight'] = 1
    else:
        raise ValueError("methods must be market_cap_weighted or equal_weighted")

    portfolio.loc[portfolio['position'] == 's', 'return'] *= - 1
    returns = portfolio[['constituent', 'weight', 'position', 'return']].groupby(portfolio.index).apply(
        calculate_return)
    # Shift for one month to get the actuals returns for the month, resample to add the missing month in case of no trades
    t = pd.DataFrame(returns, columns=['return']).resample('1M').fillna(method='bfill', limit=1).fillna(0)

    return t


def strategy_beta(portfolio):

    beta = portfolio['return'].cov(portfolio['benchmark'])/ portfolio['benchmark'].var()
    return beta
    # return beta.loc['return', 'benchmark']


def cum_returns(portfolio, name, of='return'):
    portfolio.sort_index(inplace=True)
    data = portfolio[[of]].where(portfolio.index.to_series() != portfolio.index[0], 0)
    data.loc[:, name] = np.exp(np.log(1 + data[of]).cumsum())

    return data[[name]]

def create_alpha(returns, beta, periods=12):

    alpha = (returns.loc[:, 'return'] - returns.loc[:, 'bonds']/100 -
            beta * (returns.loc[:, 'benchmark'] - returns.loc[:, 'bonds']/100)).mean()
    return (1 + alpha) ** periods - 1
