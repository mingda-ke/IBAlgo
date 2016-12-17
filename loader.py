__author__ = 'Mingda'


from ib.opt import Connection
from ib.ext.Contract import Contract
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from pandas.tseries.offsets import BDay


EXPORT_PATH = '/Users/Mingda/Desktop/PropTrading/FX/'
PORT = 7497
ID = 1994


# key: bar size, value: duration
LIMITS = {
    '1 secs': '1800 S',
    '5 secs': '7200 S',
    '10 secs': '14400 S',
    '15 secs': '14400 S',
    '30 secs': '28800 S',
    '1 min': '1 D',
    '5 mins': '1 W',
    '1 day': '1 Y'
}


class Loader(object):
    """
    data loader class, download data from IB (Interactive Broker) API
    """
    def __init__(self, symbol, exch, start, end, sec_type='STK', expiration=None, bar_size='5 secs', manual=False):
        """
        initialize function, set parameters
        :param symbol: contract symbol
        :param expiration: expiration
        :param sec_type:  security type
        :param exch: primary exchange
        :param bar_size: bar size, 1 secs, 5 secs, 15 secs, 30 secs, 1 min, 5 mins, 1 day
        :param start: start date, YYYYMMDD
        :param end: end date, YYYYMMDD
        :param manual: manually change port, request() return result after 60 request.
        :return: None
        """
        self.__conn = Connection.create(port=PORT, clientId=ID)
        self.__conn.connect()
        self.__conn.register(Loader.error_handler, 'Error')
        self.__conn.register(self.msg_parser, 'HistoricalData')
        self.__contract = Loader.make_contract(symbol=symbol, expiration=expiration, sec_type=sec_type, prime_exchange=exch, curr='USD')
        self.__bar_size = bar_size
        self.__bus_days = Loader.bus_days(start, end)
        self.__manual = manual
        self.__end_time = None
        self.__duration = LIMITS.get(bar_size, None)
        if self.__duration is None:
            raise ValueError("bar size {} is not valid!".format(bar_size))
        self.__start_time = datetime.strptime(start, '%Y%m%d')
        self.__end_time = datetime.strptime(end + ' 23:59:59', '%Y%m%d %H:%M:%S')

        self.__req_id = 0
        self.__finish = False
        self.data = []  # appending to list is faster than DataFrame
        self.datetimes = []

    @staticmethod
    def make_contract(symbol, expiration, sec_type, prime_exchange, curr):
        """
        create a Contract object
        :param symbol: symbol of underlying asset
        :param expiration: expiration of symbol
        :param sec_type: security type (STK, OPT, FUT, IND, FOP, CASH, BAG, NEWS)
        :param prime_exchange: primary exchage
        :param curr: currency
        :return: Contract object
        """
        cont = Contract()
        cont.m_symbol = symbol
        cont.m_expiry = expiration
        cont.m_secType = sec_type
        cont.m_exchange = 'SMART'
        cont.m_primaryExch = prime_exchange
        cont.m_currency = curr
        return cont

    @staticmethod
    def bus_days(start, end):
        """
        return all business days
        :param start: start date, YYYYMMDD
        :param end: end date, YYYYMMDD
        :return: a list of date string, YYYYMMDD
        """
        dates = []
        start = datetime.strptime(start, '%Y%m%d')
        end = datetime.strptime(end, '%Y%m%d')
        diff = 1
        while 1:
            dt = end - BDay(diff)
            if dt < start:
                break
            dates.append(dt.strftime('%Y%m%d'))
            diff += 1
        return dates

    @staticmethod
    def error_handler(msg):
        """
        print error message
        :return: None
        """
        print "Server Error: %s" % msg

    def msg_parser(self, msg):
        if msg.open != -1:
            new_data = {'DATETIME': pd.Timestamp(msg.date), 'OPEN': msg.open, 'HIGH': msg.high,
                        'LOW': msg.low, 'CLOSE': msg.close, 'VWAP': msg.WAP, 'VOLUME': msg.volume,
                        'NUMTRADE': msg.count}
            self.data.append(new_data)
            self.datetimes.append(new_data['DATETIME'])
        else:
            print 'finish!'
            self.__finish = True

    def request(self, item='TRADES'):
        """
        request historical data and save in self.__data
        :param item: str, 'TRADES', 'BID', 'ASK'
        :return: None
        """
        count = 0
        tm = self.__end_time
        while tm > self.__start_time:
            print "Timestamp:{} Data Length: {}".format(tm, len(self.data))
            self.__finish = False
            # setting useRTH=0 will automatically neglect market closed time.
            self.__conn.reqHistoricalData(tickerId=self.__req_id, contract=self.__contract
                                          , endDateTime=tm.strftime('%Y%m%d %H:%M:%S EST'), durationStr=self.__duration
                                          , barSizeSetting=self.__bar_size, whatToShow=item, useRTH=0, formatDate=1)
            self.__req_id += 1
            count += 1
            if count >= 60:     # pacing violation: can't request more than 60 times in 10 minutes
                if self.__manual:
                    break
                print "Waiting for 10 minutes..."
                sleep(610)
                count = 0
            sleep(1)

            while not self.__finish:    # wait callback function to finish appending.
                pass
            tm = min(self.datetimes)

        # convert data to data frame format.
        self.data = pd.DataFrame(self.data)
        self.data.drop_duplicates(inplace=True)
        if len(self.data) > 0:
            self.data.set_index(keys='DATETIME', inplace=True)
        self.__conn.disconnect()


if __name__ == "__main__":
    loader = Loader(symbol='EUR', sec_type='CASH', exch='IDEALPRO', start='20161001', end='20161020',
                    bar_size='1 min', manual=False)
    print 'request'
    loader.request('BID')
    loader.data.to_excel(EXPORT_PATH + 'EURUSD_ask.xlsx')
