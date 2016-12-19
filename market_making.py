__author__ = 'Mingda'


# toy strategy for displaying execution module

from time import sleep


def market_making(context):
    """
    simple market making strategy, post quotes on bid/ask side
    :param context:
    :return:
    """
    print "market making..."
    if context.position <= 0:
        context.order(order_type='LMC', quantity=20000, period=60)
    if context.position >= 0:
        context.order(order_type='LMC', quantity=-20000, period=60)

