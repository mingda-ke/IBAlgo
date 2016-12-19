__author__ = 'Mingda Ke'


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick2_ochl
from datetime import timedelta, datetime
from strategies.bollinger1 import initialize, bollinger_bands_1

# SET THE FOLLOWING CONFIGURATIONS BEFORE RUNNING THE MAIN SCRIPT

# folder path where data is saved
PATH_FX = '/Users/Mingda/Desktop/PropTrading/FX/'

# key: columns in data, value: variable names in signals
COLUMNS_FX = {'Open': 'OPEN',
           'Close': 'CLOSE',
           'High': 'HIGH',
           'Low': 'LOW',
           'Volume': 'VOLUME',
           'BidOpen': 'BIDOPEN',
           'OfferOpen': 'OFROPEN',
           'BidClose': 'BIDCLOSE',
           'OfferClose': 'OFRCLOSE'}


class BackTester(object):
    """
    Back Test intraday strategies (vectorized or event-driven)
    """
    def __init__(self, data=None, commission=2E-5, start='20160101', end='20161001', strategy_params=None):
        """
        set parameters and feed data
        :param data: pandas.DataFrame, bar data
        :param start: str, start date
        :param end: str, end date
        :param commission: commission fee
        """
        # pass back testing parameters
        data.rename(columns=COLUMNS_FX, inplace=True)
        self.__commission = commission
        self.__start = datetime.strptime(start, '%Y%m%d')
        self.__end = datetime.strptime(end, '%Y%m%d')
        self.__data = data[(data.index >= start) & (data.index <= self.__end)]
        self.__strategy_params = {} if strategy_params is None else strategy_params
        self.__orders = None

        # event-driven back tester live parameter
        self.__num = 0
        self.time = None
        self.position = 0
        self.trades = pd.Series(0, index=self.__data.index)
        self.limit_orders = []
        self.__cash = 1000000.0
        self.__last_deal = 0.0
        self.px_change = 0.0
        self.__total_value = pd.Series(np.nan, index=self.__data.index)
        self.__plots = []

        # strategy performance
        self.pnl = None
        self.curve = None
        self.sharpe = None
        self.max_dd = None

    def run_vector(self, strategy):
        """
        run back testing in vectorization (mainly for signal trading strategy), save performance statistics
        :param strategy: function, must have argument 'data', return 'orders'
            (pandas.Series, index: datetime, value: zero for hold, positive number for buy, negative number for sell)
        :return: None
        """
        # get orders and close position at EOD
        self.__orders = strategy(data=self.__data, **self.__strategy_params)
        self.__orders = self.__orders.reindex(index=self.__data.index)
        self.__orders.fillna(0, inplace=True)
        self.__orders.name = 'ORDER'
        self.__orders = pd.concat([self.__data['DATE'], self.__orders], axis=1)
        daily_orders = self.__orders.groupby(by='DATE').sum()
        close_orders = -daily_orders
        close_orders.index += timedelta(hours=16)
        close_orders = close_orders.reindex(index=self.__orders.index)
        close_orders.fillna(0, inplace=True)
        self.__orders += close_orders

        # conclude trading result (daily P&L, Sharpe, trading frequency ...)
        direction = np.sign(self.__orders['ORDER'])
        cash_flow = (-0.5 * self.__data['BIDOPEN'] * (1 - direction * (1 - self.__commission))
                     - 0.5 * self.__data['OFROPEN'] * (1 + direction * (1 + self.__commission))) * self.__orders['ORDER']
        cash_flow = pd.concat([cash_flow, self.__data['DATE']], axis=1)
        self.pnl = cash_flow.groupby(by='DATE').sum()
        self.curve = self.pnl.cumsum()
        self.sharpe = self.pnl.mean() / self.pnl.std() * np.sqrt(252)
        tracking_max = np.maximum.accumulate(self.curve)
        self.max_dd = ((tracking_max - self.curve) / tracking_max).max()

    def run_event(self, init, strategy):
        """
        run back testing in event-driven mode, for more flexible strategies
        use close price (bid/ask) of each bar to trade
        :param init: initialize function for strategy
        :param strategy: function, BackTester object as parameter, in the function can call
        history() and order() methods.
        :return: None
        """
        init(self)
        for i in range(200, len(self.__data) - 100):
            # pre-execute, calculate indicators, fill limit orders
            self.__num = i
            self.time = self.__data.index[i]
            print self.time
            for lmt in self.limit_orders:
                is_filled = False
                if (lmt['quantity'] > 0 and self.__data['LOW'].iloc[i] < lmt['price']) or \
                        (lmt['quantity'] < 0 and self.__data['HIGH'].iloc[i] > lmt['price']):
                    print "ORDER FILLED"
                    is_filled = True
                if is_filled:
                    lmt['order_type'] = 'MKT'
                    self.order(**lmt)
                elif lmt['order_type'] == 'LMM':
                    lmt['order_type'] = 'MKT'
                    lmt['price'] = None
                    self.order(**lmt)
                elif lmt['order_type'] == 'LMC':
                    pass
                else:
                    raise ValueError(lmt['order_type'] + ' is not a valid parameter.')
            self.limit_orders = []

            current_px = self.__data['CLOSE'].iloc[i]
            self.px_change = self.position / (abs(self.position) + 1E-9) * (current_px / self.__last_deal - 1)

            # execute strategy
            strategy(context=self, **self.__strategy_params)

            # post-execute, update current P&L
            self.__total_value.iloc[i] = self.__cash + self.position * current_px

        # post-trade analysis
        # self.__plots = pd.DataFrame(self.__plots)
        # self.__plots.set_index('DATETIME', inplace=True)
        self.trades = self.trades.groupby(by=self.trades.index.date).sum()
        self.__total_value.ffill(inplace=True)
        self.curve = self.__total_value.groupby(by=self.__total_value.index.date).last()
        self.pnl = self.curve.diff(1).fillna(0.0)
        self.sharpe = self.pnl.mean() / (self.pnl.std() + 1E-9) * np.sqrt(252)
        tracking_max = np.maximum.accumulate(self.curve)
        self.max_dd = ((tracking_max - self.curve) / tracking_max).max()

    def history(self, item='LAST', bars=10):
        """
        retrieve historical data, API function for event-driven back tester
        :param item: str or list or None, column name(s) to retrieve
        :param bars: int, number of bars to retrieve
        :return: pandas.DataFrame or pandas.Series
        """
        if item is None:
            return self.__data.iloc[(self.__num - bars + 1): (self.__num + 1), :]
        else:
            return self.__data[item].iloc[(self.__num - bars + 1): (self.__num + 1)]

    def order(self, quantity=1, order_type='MKT', price=None):
        """
        place order, API function for event-driven back tester
        :param quantity: int, order quantity, positive for buy and negative for sell
        :param order_type: str, order type, can only be 'MKT' (MARKET), 'LMC' (LIMIT CANCEL), 'LMM' (LIMIT MARKET)
        LIMIT: place limit order but if not filled in next bar, cancel (LMC) or place market order (LMM)
        :return: None
        """
        if order_type == 'MKT':
            self.position += quantity
            self.trades.iloc[self.__num] = 1
            if quantity > 0:
                self.__last_deal = price if price is not None else self.__data['OFRCLOSE'].iloc[self.__num]
                self.__cash -= self.__last_deal * quantity * (1 + self.__commission)
            if quantity < 0:
                self.__last_deal = price if price is not None else self.__data['BIDCLOSE'].iloc[self.__num]
                self.__cash -= self.__last_deal * quantity * (1 - self.__commission)
        elif order_type == 'LMI':
            self.position += quantity
            self.trades.iloc[self.__num] = 1
            if quantity > 0:
                self.__last_deal = price if price is not None else self.__data['BIDCLOSE'].iloc[self.__num]
                self.__cash -= self.__last_deal * quantity * (1 + self.__commission)
            if quantity < 0:
                self.__last_deal = price if price is not None else self.__data['OFRCLOSE'].iloc[self.__num]
                self.__cash -= self.__last_deal * quantity * (1 - self.__commission)
        else:
            print 'LMT ORDER'
            if quantity < 0:
                px = self.__data['OFRCLOSE'].iloc[self.__num]
            else:
                px = self.__data['BIDCLOSE'].iloc[self.__num]
            self.limit_orders.append({'order_type': order_type, 'quantity': quantity,
                                      'price': px})

    def plot(self, plot_data):
        """
        plot curve on top of price change, API function for event-driven back tester
        :param plot_data: dict, data to plot
        :return: None
        """
        plot_data.update({'DATETIME': self.time})
        self.__plots.append(plot_data)

    def show(self, start, end):
        """
        show plotted graph (Candlestick + Strategy Plot + P&L) between start and end
        :param start: str, 'YYYYMMDD HH:MM:SS'
        :param end: str, 'YYYYMMDD HH:MM:SS'
        :return: None
        """
        start = datetime.strptime(start, '%Y%m%d %H:%M:%S')
        end = datetime.strptime(end, '%Y%m%d %H:%M:%S')
        data_part = self.__data[(self.__data.index >= start) & (self.__data.index <= end)]
        total_value_part = self.__total_value[(self.__total_value.index >= start) & (self.__total_value.index <= end)]
        if len(self.__plots) > 0:
            plots_part = self.__plots[(self.__plots.index >= start) & (self.__plots.index <= end)]
        else:
            plots_part = pd.Series(np.nan, index=data_part.index)
        plt.figure(1)
        ax1 = plt.subplot(211)
        candlestick2_ochl(ax1, opens=data_part['OPEN'], closes=data_part['CLOSE'],
                          highs=data_part['HIGH'], lows=data_part['LOW'])
        plt.plot(plots_part)

        plt.subplot(212)
        plt.plot(total_value_part)
        plt.show()


if __name__ == '__main__':
    fx_data = pd.read_csv(PATH_FX + 'EURUSD-1Min.csv')
    fx_data.index = pd.to_datetime(fx_data['Date'].astype('str') + ' ' + fx_data['Timestamp'])
    fx_data['BidOpen'] = fx_data['Close']
    fx_data['OfferOpen'] = fx_data['Close']
    fx_data['BidClose'] = fx_data['Close']
    fx_data['OfferClose'] = fx_data['Close']
    back_tester = BackTester(data=fx_data, commission=0E-5, start='20150101', end='20150401')
    back_tester.run_event(initialize, bollinger_bands_1)
    print back_tester.curve
    back_tester.curve.plot()
    # print plt.isinteractive()
    # back_tester.show(start='20160928 9:00:00', end='20160928 11:00:00')

