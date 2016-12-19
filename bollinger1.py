__author__ = 'Mingda'


# FX Strategy, Event-driven Back Test, Based on Bollinger Bands, Variations
# Bollinger Bands Strategy Version 1
# Entry Signal: LAST goes out of bands and comes back across bands
# Exit Signal: LAST comes back to EMA/SMA or exit_std on the other side
# Stop Loss: order unrealized return or maximum drawdown is smaller than -stop_loss or ratio of std


def initialize(context):
    """
    initialize function for the strategy
    :param context:  BackTester object
    :return: None
    """
    # initialize indicators
    context.is_out = 0  # whether close is out of the band, -1 for out of lower band, 1 for out of upper band
    context.is_out_track = [0, 0, 0]   # track last three is_out indicator
    context.is_trending = 0  # whether current price is trending


def bollinger_bands_1(context, window_len=20, entry_std=2, exit_std=0, margin=25E-5, stop_loss=25E-5):
    """
    Bollinger Bands Strategy Version 1
    :param context: BackTester object
    :param window_len: int, length of window to calculate MA and STD
    :param entry_std: float, band width, used as entry signal
    :param exit_std: float, band width, used as exit signal
    :param margin: float, only when price falls between entry line and margin line, open position
    :param stop_loss: float, maximum loss of each order
    :return:
    """
    # retrieve historical data
    rolling_close = context.history(item='CLOSE', bars=window_len)
    curr_close = rolling_close.iloc[-1]
    curr_pos = context.position
    moving_avg = rolling_close.mean()
    moving_std = rolling_close.diff(1).std()

    # track indicators
    upper_entry = moving_avg + entry_std * moving_std   # price threshold
    lower_entry = moving_avg - entry_std * moving_std
    upper_exit = moving_avg + exit_std * moving_std
    lower_exit = moving_avg - exit_std * moving_std
    upper_margin = moving_avg + margin
    lower_margin = moving_avg - margin

    if curr_close < lower_entry:    # is_out indicator
        context.is_out = -1
    if curr_close > upper_entry:
        context.is_out = 1
    context.is_out_track = context.is_out_track[1:] + [context.is_out]

    if len(set(context.is_out_track)) == 1:     # is_trending indicator
        context.is_trending = context.is_out_track[0]
    if context.is_trending == 1 and curr_close < moving_avg:
        context.is_trending = 0
    if context.is_trending == -1 and curr_close > moving_avg:
        context.is_trending = 0

    # open position
    if curr_pos == 0:
        if context.is_out == -1 and lower_entry < curr_close < lower_margin and (not context.is_trending):
            context.order(quantity=1000000, order_type='MKT')
            context.is_out = 0
        if context.is_out == 1 and upper_entry > curr_close > upper_margin and (not context.is_trending):
            context.order(quantity=-1000000, order_type='MKT')
            context.is_out = 0

    # close position
    if curr_pos > 0 and curr_close < lower_exit:
        context.order(quantity=-curr_pos, order_type='MKT')
    if curr_pos < 0 and curr_close > upper_exit:
        context.order(quantity=-curr_pos, order_type='MKT')

    # stop loss
    if context.px_change < -stop_loss:
        if curr_pos > 0:
            context.order(quantity=-curr_pos, order_type='MKT')
        if curr_pos < 0:
            context.order(quantity=curr_pos, order_type='MKT')





