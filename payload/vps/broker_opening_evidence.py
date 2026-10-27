"""Bounded, read-only projection of an already obtained broker order record.

This module never calls MT5, performs I/O or changes an order. Missing evidence
is diagnostic only and must not change the outcome of a trading operation.
"""
import math
import re


def _number(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError, TypeError):
        return None
    return result if math.isfinite(result) and (result > 0 if positive else result >= 0) else None


def _integer(value):
    number = _number(value)
    return int(number) if number is not None and number.is_integer() and number <= 2**53-1 else None


def opening_order_evidence(order, opening_ticket, symbol, *, submitted_price=None, fill_price=None):
    """Return exact-ticket broker fields; absence never proves price compliance.

    Keep native order type/state as integers. An order's final record does not
    provide its complete amendment history, and a market order price is not a
    guaranteed execution price. Callers retain all existing ownership guards.
    """
    try:
        ticket = str(opening_ticket)
        if (not re.fullmatch(r'[1-9][0-9]{0,19}', ticket)
                or not isinstance(symbol, str) or not 1 <= len(symbol) <= 128
                or str(order.ticket) != ticket or order.symbol != symbol):
            return None
        native_type = _integer(order.type)
        order_price = _number(order.price_open, positive=True)
        if native_type is None or native_type > 255 or order_price is None:
            return None
        result = dict(schema='broker-opening-order-v1', openingTicket=ticket, symbol=symbol,
                      orderType=native_type, acceptedOrderPrice=order_price,
                      amendmentHistoryAvailable=False)
        for attribute, name in (('state', 'orderState'), ('type_filling', 'fillingType'),
                                ('time_setup_msc', 'setupTimeMsc'), ('time_done_msc', 'doneTimeMsc'),
                                ('time_expiration', 'expirationTimeSeconds')):
            value = _integer(getattr(order, attribute, None))
            if value is not None:
                result[name] = value
        for attribute, name in (('volume_initial', 'initialVolume'), ('volume_current', 'remainingVolume'),
                                ('sl', 'sl'), ('tp', 'tp')):
            value = _number(getattr(order, attribute, None))
            if value is not None:
                result[name] = value
        submitted = _number(submitted_price, positive=True)
        fill = _number(fill_price, positive=True)
        if submitted is not None:
            result.update(submittedPrice=submitted,
                          requestToOrderPriceDifference=round(order_price-submitted, 10))
        if fill is not None:
            result.update(fillPrice=fill, orderToFillPriceDifference=round(fill-order_price, 10))
        return result
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None
