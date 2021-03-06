from a_blackfire_capital_class import statistics as cstat
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from matplotlib import cm
from textwrap import wrap

import pandas as pd

__author__ = 'pougomg'
import numpy as np
from pathlib import Path


class DisplaySheetStatistics:
    """
	Displays a one pager including some basics statistics for the portfolio strategy.
	"""

    def __init__(self, portfolio, title, description, benchmark=None, rolling_periods=12, rolling_sharpe=True):

        self.portfolio = portfolio.sort_index()
        if benchmark is not None:
            self.benchmark = benchmark.sort_index()
        else:
            self.benchmark = None

        self.rolling_periods = rolling_periods
        self.title = title
        self.description = description
        self.rolling_sharpe = rolling_sharpe

    def get_results(self):

        """
		This function return some key statistics and results for the strategy.
		:return:
		"""
        portfolio = self.portfolio
        stats = dict()

        my_path = Path(__file__).parent.parent.resolve()
        us_bonds = np.load(str(my_path) + '/e_blackfire_capital_files/monthly_us_bonds_prices.npy')
        us_bonds = pd.DataFrame(us_bonds, columns=['date', 'bonds'])
        us_bonds['date'] = pd.DatetimeIndex(us_bonds['date'].dt.strftime('%Y-%m-%d'))
        us_bonds.set_index('date', inplace=True)
        us_bonds.sort_index(inplace=True)

        # PortFolio Constituents Turnover
        stats['turnover'] = cstat.turnover(portfolio)[0]

        # Strategy Monthly returns
        stats['return'] = cstat.strategy_returns(portfolio)

        # print(stats['return'])
        # return

        portfolio = pd.merge(stats['return'], us_bonds, left_index=True, right_index=True)
        portfolio = portfolio.astype(float)

        # Strategy Cumulative returns
        stats['price'] = cstat.cum_returns(stats['return'], 'back test')

        # Rolling Annualised Sharpe
        sharpe, rolling_sharpe_s = cstat.create_sharpe_ratio(
            portfolio[['return', 'bonds']], self.rolling_periods
        )
        # print(stats['return'])
        stats["rolling_sharpe"] = rolling_sharpe_s

        # Equity statistics
        stats["sharpe"] = sharpe

        # Drawdown, max drawdown, max drawdown duration
        dd_s, max_dd, dd_dur = cstat.create_drawdowns(stats['price'])

        stats["drawdowns"] = dd_s
        stats["max_drawdown"] = max_dd
        stats["max_drawdown_pct"] = max_dd
        stats["max_drawdown_duration"] = dd_dur

        if self.benchmark is not None:
            portfolio = pd.merge(portfolio, self.benchmark, left_index=True, right_index=True)
            portfolio = portfolio.astype(float)
            stats['beta'] = cstat.strategy_beta(portfolio)
            stats['return_b'] = portfolio[['benchmark']]
            stats['price_b'] = cstat.cum_returns(portfolio[['benchmark']], 'benchmark', of='benchmark')

            # Drawdown, max drawdown, max drawdown duration
            dd_s, max_dd, dd_dur = cstat.create_drawdowns(stats['price_b'])

            stats["drawdowns_b"] = dd_s
            stats["max_drawdown_b"] = max_dd
            stats["max_drawdown_pct_b"] = max_dd
            stats["max_drawdown_duration_b"] = dd_dur

            # Sharpe Ratio
            sharpe, rolling_sharpe_s = cstat.create_sharpe_ratio(
                portfolio[['benchmark', 'bonds']], self.rolling_periods
            )
            stats["rolling_sharpe_b"] = rolling_sharpe_s
            stats["sharpe_b"] = sharpe

            # Alpha Jensen
            alpha = cstat.create_alpha(
                portfolio[['return', 'benchmark', 'bonds']], stats['beta']
            )
            stats['alpha'] = alpha

        return stats

    def _plot_strategy_description(self, ax=None, **kwargs):

        def format_perc(x, pos):
            return '%.0f%%' % x

        if ax is None:
            ax = plt.gca()

        y_axis_formatter = FuncFormatter(format_perc)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))


        # ax.text(9.5, 6.9, self.description, fontsize=8, fontweight='bold',
        #         color='grey', horizontalalignment='right')
        # build a rectangle in axes coords
        left, width = .25, .5
        bottom, height = .25, .5
        right = left + width
        top = bottom + height
        ax.text(0.5*(left+right), 0.5*(bottom+top), "\n".join(wrap(self.description)),
                horizontalalignment='center',
                verticalalignment='center',
                fontsize=8, color='black')
        ax.grid(False)
        ax.set_title('Description', fontweight='bold')
        ax.spines['top'].set_linewidth(2.0)
        ax.spines['bottom'].set_linewidth(2.0)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.get_yaxis().set_visible(False)
        ax.get_xaxis().set_visible(False)
        ax.set_ylabel('')
        ax.set_xlabel('')
        # ax.axis([0, 10, 0, 12])
        return ax

    def _plot_equity(self, stats, ax=None, **kwargs):

        def format_two_dec(x, pos):
            return '%.2f' % x

        equity = stats['price']
        if ax is None:
            ax = plt.gca()
        y_axis_formatter = FuncFormatter(format_two_dec)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
        ax.xaxis.set_tick_params(reset=True)
        ax.yaxis.grid(linestyle=':')
        ax.xaxis.set_major_locator(mdates.YearLocator(1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.grid(linestyle=':')
        if self.benchmark is not None:
            benchmark = stats['price_b']
            benchmark.plot(
                lw=2, color='red', label='Benchmark', alpha=0.60, linestyle='--',
                ax=ax, **kwargs
            )

        equity.plot(lw=2, color='green', alpha=0.6, x_compat=False,
                    label='Backtest', ax=ax, **kwargs)
        ax.axhline(1.0, linestyle='--', color='black', lw=1)
        ax.set_ylabel('Cumulative returns')
        ax.legend(loc='best')
        ax.set_xlabel('')

        plt.setp(ax.get_xticklabels(), visible=True, rotation=0, ha='center')

        # if self.log_scale:
        # 	ax.set_yscale('log')

        return ax

    def _plot_rolling_sharpe(self, stats, ax=None, **kwargs):
        """
        Plots the curve of rolling Sharpe ratio.
        """

        def format_two_dec(x, pos):
            return '%.2f' % x

        sharpe = stats['rolling_sharpe']

        if ax is None:
            ax = plt.gca()

        y_axis_formatter = FuncFormatter(format_two_dec)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
        ax.xaxis.set_tick_params(reset=True)
        ax.yaxis.grid(linestyle=':')
        ax.xaxis.set_major_locator(mdates.YearLocator(1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.grid(linestyle=':')

        if self.benchmark is not None:
            benchmark = stats['rolling_sharpe_b']
            benchmark.plot(
                lw=2, color='red', label='Benchmark', alpha=0.60, linestyle='--',
                ax=ax, **kwargs
            )

        sharpe.plot(lw=2, color='green', alpha=0.6, x_compat=False,
                    label='Backtest', ax=ax, **kwargs)

        ax.axvline(sharpe.index[-1], linestyle="dashed", c="gray", lw=2)
        ax.set_ylabel('Rolling Annualised Sharpe')
        ax.legend(loc='best')
        ax.set_xlabel('')
        plt.setp(ax.get_xticklabels(), visible=True, rotation=0, ha='center')

        return ax

    def _plot_drawdown(self, stats, ax=None, **kwargs):

        """
		Plots the underwater curve
		"""

        def format_perc(x, pos):
            return '%.0f%%' % x

        drawdown = stats['drawdowns']

        if ax is None:
            ax = plt.gca()

        y_axis_formatter = FuncFormatter(format_perc)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
        ax.yaxis.grid(linestyle=':')
        ax.xaxis.set_tick_params(reset=True)
        ax.xaxis.set_major_locator(mdates.YearLocator(1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.grid(linestyle=':')

        underwater = -100 * drawdown
        underwater.plot(ax=ax, lw=2, kind='area', color='red', alpha=0.3, **kwargs)
        ax.set_ylabel('')
        ax.set_xlabel('')
        plt.setp(ax.get_xticklabels(), visible=True, rotation=0, ha='center')
        ax.set_title('Drawdown (%)', fontweight='bold')

        return ax

    def _plot_monthly_returns(self, stats, ax=None, **kwargs):
        """
		Plots a heatmap of the monthly returns.
		"""
        returns = stats['return']
        if ax is None:
            ax = plt.gca()

        monthly_ret = cstat.aggregate_returns(returns, 'monthly')
        monthly_ret = monthly_ret.unstack()
        monthly_ret = np.round(monthly_ret, 3)

        monthly_ret.rename(
            columns={1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
                     5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                     9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'},
            inplace=True
        )

        sns.heatmap(
            monthly_ret.fillna(0) * 100.0,
            annot=True,
            fmt="0.1f",
            annot_kws={"size": 8},
            alpha=1.0,
            center=0.0,
            cbar=False,
            cmap=cm.RdYlGn,
            ax=ax, **kwargs)
        ax.set_title('Monthly Returns (%)', fontweight='bold')
        ax.set_ylabel('')
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0)
        ax.set_xlabel('')

        return ax

    def _plot_yearly_returns(self, stats, ax=None, **kwargs):
        """
		Plots a barplot of returns by year.
		"""

        def format_perc(x, pos):
            return '%.0f%%' % x

        returns = stats['return']

        if ax is None:
            ax = plt.gca()

        y_axis_formatter = FuncFormatter(format_perc)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
        ax.yaxis.grid(linestyle=':')

        yly_ret = cstat.aggregate_returns(returns, 'yearly') * 100.0
        yly_ret = yly_ret.to_frame()
        yly_ret.columns = ['back test']

        if 'return_b' in stats:
            yly_ret_b = cstat.aggregate_returns(stats['return_b'] , 'yearly') * 100.0
            yly_ret_b = yly_ret_b.to_frame()
            yly_ret_b.columns = ['benchmark']
            yly_ret = pd.merge(yly_ret, yly_ret_b, left_index=True, right_index=True)

        yly_ret.plot(ax=ax, kind="bar")
        ax.set_title('Yearly Returns (%)', fontweight='bold')
        ax.set_ylabel('')
        ax.set_xlabel('')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        ax.xaxis.grid(False)

        return ax

    def _plot_txt_curve(self, stats, ax=None, **kwargs):
        """
        Outputs the statistics for the equity curve.
        """

        def format_perc(x, pos):
            return '%.0f%%' % x

        returns = stats["return"]
        cum_returns = stats['price']

        if ax is None:
            ax = plt.gca()

        y_axis_formatter = FuncFormatter(format_perc)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
        tot_ret = cum_returns.iloc[-1, 0] - 1.0
        cagr = cstat.create_cagr(cum_returns, self.rolling_periods)
        sharpe = stats["sharpe"]
        sortino = cstat.create_sortino_ratio(returns, self.rolling_periods)
        rsq = cstat.rsquared(range(cum_returns.shape[0]), cum_returns)
        dd, dd_max, dd_dur = cstat.create_drawdowns(cum_returns)

        ax.text(0.25, 10.9, 'Total Return', fontsize=8)
        ax.text(7.50, 10.9, '{:.0%}'.format(tot_ret), fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 9.9, 'CAGR', fontsize=8)
        ax.text(7.50, 9.9, '{:.2%}'.format(cagr), fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 8.9, 'Sharpe Ratio', fontsize=8)
        ax.text(7.50, 8.9, '{:.2f}'.format(sharpe), fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 7.9, 'Sortino Ratio', fontsize=8)
        ax.text(7.50, 7.9, '{:.2f}'.format(sortino), fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 6.9, 'Annual Volatility', fontsize=8)
        ax.text(7.50, 6.9, '{:.2%}'.format((returns.std() * np.sqrt(self.rolling_periods)).values[0]),
                fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 5.9, 'R-Squared', fontsize=8)
        ax.text(7.50, 5.9, '{:.2f}'.format(rsq), fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 4.9, 'Max Monthly Drawdown', fontsize=8)
        ax.text(7.50, 4.9, '{:.2%}'.format(dd_max), color='red', fontweight='bold', horizontalalignment='right',
                fontsize=8)

        ax.text(0.25, 3.9, 'Max Drawdown Duration', fontsize=8)
        ax.text(7.50, 3.9, '{:.0f}'.format(dd_dur), fontweight='bold', horizontalalignment='right', fontsize=8)

        ax.text(0.25, 2.9, 'Mean Monthly Stocks Turnover', fontsize=8)
        ax.text(9.75, 2.9, '{:.2f}'.format(stats['turnover']), fontweight='bold', horizontalalignment='right',
                fontsize=8)

        # ax.text(0.25, 0.9, 'Trades per Year', fontsize=8)
        # ax.text(7.50, 0.9, '{:.1f}'.format(trd_yr), fontweight='bold', horizontalalignment='right', fontsize=8)
        # ax.set_title('Curve', fontweight='bold')
        ax.set_title('Curve', fontweight='bold')

        if self.benchmark is not None:
            returns_b = stats['return_b']
            equity_b = stats['price_b']
            tot_ret_b = equity_b.iloc[-1, 0] - 1.0
            cagr_b = cstat.create_cagr(equity_b, self.rolling_periods)
            sharpe_b = stats["sharpe_b"]
            sortino_b = cstat.create_sortino_ratio(returns_b, self.rolling_periods)
            rsq_b = cstat.rsquared(range(equity_b.shape[0]), equity_b)
            dd_b, dd_max_b, dd_dur_b = cstat.create_drawdowns(equity_b)
            beta = stats['beta']

            ax.text(9.75, 10.9, '{:.0%}'.format(tot_ret_b), fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.text(9.75, 9.9, '{:.2%}'.format(cagr_b), fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.text(9.75, 8.9, '{:.2f}'.format(sharpe_b), fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.text(9.75, 7.9, '{:.2f}'.format(sortino_b), fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.text(9.75, 6.9, '{:.2%}'.format((returns_b.std() * np.sqrt(self.rolling_periods)).values[0]),
                    fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.text(9.75, 5.9, '{:.2f}'.format(rsq_b), fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.text(9.75, 4.9, '{:.2%}'.format(dd_max_b), color='red', fontweight='bold', horizontalalignment='right',
                    fontsize=8)
            ax.text(9.75, 3.9, '{:.0f}'.format(dd_dur_b), fontweight='bold', horizontalalignment='right', fontsize=8)

            ax.text(0.25, 1.9, 'Beta', fontsize=8)
            ax.text(7.50, 1.9, '{:.1f}'.format(beta), fontweight='bold', horizontalalignment='right', fontsize=8)

            ax.text(0.25, 0.9, 'Beta', fontsize=8)
            ax.text(7.50, 0.9, '{:.2f} %'.format(100 * stats['alpha']), fontweight='bold', horizontalalignment='right', fontsize=8)
            ax.set_title('Curve vs. Benchmark', fontweight='bold')

        ax.grid(False)
        ax.spines['top'].set_linewidth(2.0)
        ax.spines['bottom'].set_linewidth(2.0)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.get_yaxis().set_visible(False)
        ax.get_xaxis().set_visible(False)
        ax.set_ylabel('')
        ax.set_xlabel('')

        ax.axis([0, 10, 0, 12])
        return ax

    def _plot_txt_trade(self, stats, ax=None, **kwargs):
        """
		Outputs the statistics for the trades.
		"""

        def format_perc(x, pos):
            return '%.0f%%' % x

        if ax is None:
            ax = plt.gca()

        if 'positions' not in stats:
            num_trades = 0
            win_pct = "N/A"
            win_pct_str = "N/A"
            avg_trd_pct = "N/A"
            avg_win_pct = "N/A"
            avg_loss_pct = "N/A"
            max_win_pct = "N/A"
            max_loss_pct = "N/A"
        else:
            pos = stats['positions']
            num_trades = pos.shape[0]
            win_pct = pos[pos["trade_pct"] > 0].shape[0] / float(num_trades)
            win_pct_str = '{:.0%}'.format(win_pct)
            avg_trd_pct = '{:.2%}'.format(np.mean(pos["trade_pct"]))
            avg_win_pct = '{:.2%}'.format(np.mean(pos[pos["trade_pct"] > 0]["trade_pct"]))
            avg_loss_pct = '{:.2%}'.format(np.mean(pos[pos["trade_pct"] <= 0]["trade_pct"]))
            max_win_pct = '{:.2%}'.format(np.max(pos["trade_pct"]))
            max_loss_pct = '{:.2%}'.format(np.min(pos["trade_pct"]))

        y_axis_formatter = FuncFormatter(format_perc)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))

        # TODO: Position class needs entry date
        max_loss_dt = 'TBD'  # pos[pos["trade_pct"] == np.min(pos["trade_pct"])].entry_date.values[0]
        avg_dit = '0.0'  # = '{:.2f}'.format(np.mean(pos.time_in_pos))

        ax.text(0.5, 8.9, 'Trade Winning %', fontsize=8)
        ax.text(9.5, 8.9, win_pct_str, fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.text(0.5, 7.9, 'Average Trade %', fontsize=8)
        ax.text(9.5, 7.9, avg_trd_pct, fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.text(0.5, 6.9, 'Average Win %', fontsize=8)
        ax.text(9.5, 6.9, avg_win_pct, fontsize=8, fontweight='bold', color='green', horizontalalignment='right')

        ax.text(0.5, 5.9, 'Average Loss %', fontsize=8)
        ax.text(9.5, 5.9, avg_loss_pct, fontsize=8, fontweight='bold', color='red', horizontalalignment='right')

        ax.text(0.5, 4.9, 'Best Trade %', fontsize=8)
        ax.text(9.5, 4.9, max_win_pct, fontsize=8, fontweight='bold', color='green', horizontalalignment='right')

        ax.text(0.5, 3.9, 'Worst Trade %', fontsize=8)
        ax.text(9.5, 3.9, max_loss_pct, color='red', fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.text(0.5, 2.9, 'Worst Trade Date', fontsize=8)
        ax.text(9.5, 2.9, max_loss_dt, fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.text(0.5, 1.9, 'Avg Days in Trade', fontsize=8)
        ax.text(9.5, 1.9, avg_dit, fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.text(0.5, 0.9, 'Trades', fontsize=8)
        ax.text(9.5, 0.9, num_trades, fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.set_title('Trade', fontweight='bold')
        ax.grid(False)
        ax.spines['top'].set_linewidth(2.0)
        ax.spines['bottom'].set_linewidth(2.0)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.get_yaxis().set_visible(False)
        ax.get_xaxis().set_visible(False)
        ax.set_ylabel('')
        ax.set_xlabel('')

        ax.axis([0, 10, 0, 10])

        return ax

    def _plot_txt_time(self, stats, ax=None, **kwargs):
        """
		Outputs the statistics for various time frames.
		"""

        def format_perc(x, pos):
            return '%.0f%%' % x

        returns = stats['return']

        if ax is None:
            ax = plt.gca()

        y_axis_formatter = FuncFormatter(format_perc)
        ax.yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))

        mly_ret = cstat.aggregate_returns(returns, 'monthly')
        yly_ret = cstat.aggregate_returns(returns, 'yearly')

        mly_pct = mly_ret[mly_ret >= 0].shape[0] / float(mly_ret.shape[0])
        mly_avg_win_pct = np.mean(mly_ret[mly_ret >= 0])
        mly_avg_loss_pct = np.mean(mly_ret[mly_ret < 0])
        mly_max_win_pct = np.max(mly_ret)
        mly_max_loss_pct = np.min(mly_ret)
        yly_pct = yly_ret[yly_ret >= 0].shape[0] / float(yly_ret.shape[0])
        yly_max_win_pct = np.max(yly_ret)
        yly_max_loss_pct = np.min(yly_ret)

        ax.text(0.5, 8.9, 'Winning Months %', fontsize=8)
        ax.text(9.5, 8.9, '{:.0%}'.format(mly_pct), fontsize=8, fontweight='bold',
                horizontalalignment='right')

        ax.text(0.5, 7.9, 'Average Winning Month %', fontsize=8)
        ax.text(9.5, 7.9, '{:.2%}'.format(mly_avg_win_pct), fontsize=8, fontweight='bold',
                color='red' if mly_avg_win_pct < 0 else 'green',
                horizontalalignment='right')

        ax.text(0.5, 6.9, 'Average Losing Month %', fontsize=8)
        ax.text(9.5, 6.9, '{:.2%}'.format(mly_avg_loss_pct), fontsize=8, fontweight='bold',
                color='red' if mly_avg_loss_pct < 0 else 'green',
                horizontalalignment='right')

        ax.text(0.5, 5.9, 'Best Month %', fontsize=8)
        ax.text(9.5, 5.9, '{:.2%}'.format(mly_max_win_pct), fontsize=8, fontweight='bold',
                color='red' if mly_max_win_pct < 0 else 'green',
                horizontalalignment='right')

        ax.text(0.5, 4.9, 'Worst Month %', fontsize=8)
        ax.text(9.5, 4.9, '{:.2%}'.format(mly_max_loss_pct), fontsize=8, fontweight='bold',
                color='red' if mly_max_loss_pct < 0 else 'green',
                horizontalalignment='right')

        ax.text(0.5, 3.9, 'Winning Years %', fontsize=8)
        ax.text(9.5, 3.9, '{:.0%}'.format(yly_pct), fontsize=8, fontweight='bold',
                horizontalalignment='right')

        ax.text(0.5, 2.9, 'Best Year %', fontsize=8)
        ax.text(9.5, 2.9, '{:.2%}'.format(yly_max_win_pct), fontsize=8,
                fontweight='bold', color='red' if yly_max_win_pct < 0 else 'green',
                horizontalalignment='right')

        ax.text(0.5, 1.9, 'Worst Year %', fontsize=8)
        ax.text(9.5, 1.9, '{:.2%}'.format(yly_max_loss_pct), fontsize=8,
                fontweight='bold', color='red' if yly_max_loss_pct < 0 else 'green',
                horizontalalignment='right')

        # ax.text(0.5, 0.9, 'Positive 12 Month Periods', fontsize=8)
        # ax.text(9.5, 0.9, num_trades, fontsize=8, fontweight='bold', horizontalalignment='right')

        ax.set_title('Time', fontweight='bold')
        ax.grid(False)
        ax.spines['top'].set_linewidth(2.0)
        ax.spines['bottom'].set_linewidth(2.0)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.get_yaxis().set_visible(False)
        ax.get_xaxis().set_visible(False)
        ax.set_ylabel('')
        ax.set_xlabel('')

        ax.axis([0, 10, 0, 10])
        return ax

    def plot_results(self):
        """
        Plot the Statistics sheets
        """
        rc = {
            'lines.linewidth': 1.0,
            'axes.facecolor': '0.995',
            'figure.facecolor': '0.97',
            'font.family': 'serif',
            'font.serif': 'Ubuntu',
            'font.monospace': 'Ubuntu Mono',
            'font.size': 10,
            'axes.labelsize': 10,
            'axes.labelweight': 'bold',
            'axes.titlesize': 10,
            'xtick.labelsize': 8,
            'ytick.labelsize': 8,
            'legend.fontsize': 10,
            'figure.titlesize': 12
        }
        sns.set_context(rc)
        sns.set_style("whitegrid")
        sns.set_palette("deep", desat=.6)

        if self.rolling_sharpe:
            offset_index = 1
        else:
            offset_index = 0

        vertical_sections = 7 + offset_index
        fig = plt.figure(figsize=(15, vertical_sections * 4))
        fig.suptitle(self.title, y=0.94, weight='bold')
        gs = gridspec.GridSpec(vertical_sections, 3, wspace=0.25, hspace=0.5)

        stats = self.get_results()
        ax_description = plt.subplot(gs[:1, :])
        ax_equity = plt.subplot(gs[1:3, :])
        if self.rolling_sharpe:
            ax_sharpe = plt.subplot(gs[3, :])
        ax_drawdown = plt.subplot(gs[3 + offset_index, :])
        ax_monthly_returns = plt.subplot(gs[4 + offset_index: 6 + offset_index, :2])
        ax_yearly_returns = plt.subplot(gs[4 + offset_index: 6 + offset_index, 2])
        ax_txt_curve = plt.subplot(gs[6 + offset_index:, 0])
        ax_txt_trade = plt.subplot(gs[6 + offset_index:, 1])
        ax_txt_time = plt.subplot(gs[6 + offset_index: , 2])

        self._plot_strategy_description(ax=ax_description)
        self._plot_equity(stats, ax=ax_equity)
        if self.rolling_sharpe:
            self._plot_rolling_sharpe(stats, ax=ax_sharpe)
        self._plot_drawdown(stats, ax=ax_drawdown)
        self._plot_monthly_returns(stats, ax=ax_monthly_returns)
        self._plot_yearly_returns(stats, ax=ax_yearly_returns)
        self._plot_txt_curve(stats, ax=ax_txt_curve)
        self._plot_txt_trade(stats, ax=ax_txt_trade)
        self._plot_txt_time(stats, ax=ax_txt_time)

        # Plot the figure
        # fig = plt.gcf()
        fig.set_size_inches(15, 20)
        fig.savefig(self.title + '.png', dpi=500)
        # plt.show()
