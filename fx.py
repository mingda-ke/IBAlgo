__author__ = 'Mingda'

from ib.opt import Connection, message
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from time import sleep
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from market_making import market_making

# minimum price variation for each currency (under paper trading environment)
MIN_PRICE = {'EUR': 0.00005}


class FXTrader(object):
    """
    wrapper of IB API function to trade FX
    """
    def __init__(self, currency, strategy=None, frequency=60):
        """
        initialize function
        :param currency: str, fx pair to trade, e.g. 'EUR'
        :param strategy: function(parameter contains self), execution strategy
        :param frequency: int, how many seconds to run strategy
        """
        # store parameters
        self.currency = currency
        self.strategy = strategy
        self.frequency = frequency

        # create contract
        self.__contract = Contract()
        self.__contract.m_symbol = currency
        self.__contract.m_secType = 'CASH'
        self.__contract.m_exchange = 'IDEALPRO'
        self.__contract.m_primaryExch = 'IDEALPRO'
        self.__contract.m_currency = 'USD'

        # create empty order
        self.__order = Order()

        # connect to IB and register callback function
        self.conn = Connection.create(port=7497, clientId=1994)
        self.conn.connect()
        # self.conn.registerAll(self.reply_handler)
        self.conn.register(self.error_handler, message.Error)
        self.conn.register(self.bar_handler, message.realtimeBar)
        self.conn.register(self.snapshot_handler, message.tickPrice)
        self.conn.register(self.valid_id_handler, message.nextValidId)
        self.conn.register(self.time_handler, message.currentTime)
        self.conn.register(self.open_order_handler, message.orderStatus)
        self.conn.register(self.position_handler, message.position)
        self.conn.register(self.market_depth_handler, message.updateMktDepth)

        # get valid request ID and order ID
        self.__req_id = 1
        self.__order_id = 1
        self.conn.reqIds(1)

        # initialise other variables
        self.debug = False
        self.stop_ind = False
        self.order_time = None
        self.current_time = None
        self.position = 0
        self.errors = []
        self.bar_data = pd.DataFrame(columns=['DATETIME', 'OPEN', 'CLOSE', 'HIGH', 'LOW'])
        self.bid_price = None
        self.ask_price = None
        self.open_orders = {}
        self.min_price = MIN_PRICE[currency]

        # request real time data
        self.conn.reqRealTimeBars(tickerId=self.__req_id, contract=self.__contract, barSize=5, whatToShow='MIDPOINT',
                                  useRTH=1)
        self.__req_id += 1
        self.conn.reqMktDepth(tickerId=self.__req_id, contract=self.__contract, numRows=1)
        self.__req_id += 1

    def error_handler(self, msg):
        """
        handle error information
        :param msg: message
        :return: None
        """
        print msg
        self.errors.append({'errorCode': msg.errorCode, 'errorMsg': msg.errorMsg})

    def bar_handler(self, msg):
        """
        real time bars handler
        :param msg: message
        :return: None
        """
        self.current_time = datetime(1970, 1, 1) + timedelta(seconds=msg.time)
        self.bar_data = self.bar_data.append({'DATETIME': pd.Timestamp(self.current_time), 'OPEN': msg.open,
                                              'HIGH': msg.high, 'LOW': msg.low, 'CLOSE': msg.close}, ignore_index=True)
        self.run(self.strategy)

    def snapshot_handler(self, msg):
        """
        market snapshot handler
        :param msg: message
        :return: None
        """
        print msg
        if msg.field == 1:
            self.bid_price = msg.price
        if msg.field == 2:
            self.ask_price = msg.price

    def open_order_handler(self, msg):
        """
        open order handler
        :param msg: message
        :return: None
        """
        print msg
        if msg.orderId in self.open_orders and msg.status != 'Cancelled':
            self.open_orders[msg.orderId]['remaining'] = msg.remaining

    def valid_id_handler(self, msg):
        """
        next valid ID handler
        :param msg: message
        :return: None
        """
        self.__order_id = msg.orderId
        self.__req_id = msg.orderId

    def time_handler(self, msg):
        """
        current time handler
        :param msg: message
        :return: None
        """
        print msg
        self.order_time = datetime(1970, 1, 1) + timedelta(seconds=msg.time)

    def position_handler(self, msg):
        """
        position handler
        :param msg: message
        :return: None
        """
        self.position = msg.pos

    def market_depth_handler(self, msg):
        """
        market depth handler
        :param msg: message
        :return: None
        """
        if msg.side == 0:
            self.ask_price = msg.price
        if msg.side == 1:
            self.bid_price = msg.price

    @staticmethod
    def reply_handler(msg):
        """
        request debug
        :param msg: message
        :return: None
        """
        print msg

    def history(self, item=None, bars=None):
        """
        retrieve historical data
        :param item: str, item to retrieve, can be 'OPEN', 'HIGH', 'LOW', 'CLOSE'
        :param bars: int, number of bars
        :return: pandas.DataFrame
        """
        return self.bar_data[item].iloc[-bars:]

    def order(self, order_type=None, quantity=0, period=60, lmt_price=None):
        """
        place order, API function
        :param order_type: str, order type, like 'LMC', 'LMM', 'MKT'
        :param quantity: int, order quantity
        :param period: int, when order_type is 'LMC' or 'LMM', cancel or execute limit order after how many seconds
        :return: None
        """
        self.__order.m_action = 'BUY' if quantity >= 0 else 'SELL'
        self.__order.m_totalQuantity = abs(quantity)
        self.__order.m_orderType = 'MKT' if order_type == 'MKT' else 'LMT'
        if order_type == 'MKT':
            self.conn.placeOrder(self.__order_id, self.__contract, self.__order)
        else:
            # call self.bid_price and self.ask_price immediately after reqMktData is risky,
            # consider using threading.Event
            self.__order.m_lmtPrice = np.floor(self.bid_price / self.min_price) * self.min_price if quantity >= 0 \
                else np.ceil(self.ask_price / self.min_price) * self.min_price
            if lmt_price is not None:
                self.__order.m_lmtPrice = lmt_price
            self.conn.placeOrder(self.__order_id, self.__contract, self.__order)

            self.open_orders.update({self.__order_id: {'expiration': self.current_time + timedelta(seconds=period),
                                                       'action': order_type[-1],
                                                       'remaining': abs(quantity),
                                                       'direction': quantity / abs(quantity)}})
        self.__order_id += 1

    def run(self, strategy=None):
        """
        run strategy, API function
        :return: None
        """
        # stop running
        if self.stop_ind:
            return

        # run strategy
        print 'Run strategy'
        self.conn.reqPositions()
        current_secs = 3600 * self.current_time.hour + 60 * self.current_time.minute + self.current_time.second
        print current_secs
        if current_secs % self.frequency == 0:
            if strategy is not None:
                strategy(context=self)

        # deal with open orders
        for order_id in self.open_orders.iterkeys():
            if self.open_orders[order_id]['expiration'] <= self.current_time:
                if self.open_orders[order_id]['remaining'] > 0:
                    self.conn.cancelOrder(order_id)
                    if self.open_orders[order_id]['action'] == 'M':
                        self.order(order_type='MKT', quantity=self.open_orders[order_id]['remaining'] * self.
                                   open_orders[order_id]['direction'])
                    self.open_orders[order_id]['remaining'] = 0

    def stop(self):
        """
        stop running strategy
        :return:
        """
        self.stop_ind = True

    def resume(self):
        """
        resume running strategy
        :return:
        """
        self.stop_ind = False

if __name__ == '__main__':
    fx_trader = FXTrader(currency='EUR', frequency=20, strategy=market_making)


