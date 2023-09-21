from enum import IntEnum

class MoexCandlePeriods(IntEnum):
    """Enum of candle periods.
    """

    Period1Min = 1,
    """Period for 1 minute candles.
    """
    Period10Min = 10,
    """Period for 10 minutes candles.
    """
    Period1Hour = 60,
    """Period for 1 hour candles.
    """
    Period1Day = 24,
    """Period for 1 day candles.
    """
    Period1Week = 7,
    """Period for 1 week candles.
    """
    Period1Month = 31,
    """Period for 1 month candles.
    """
    Period1Quarter = 4,
    """Period for 1 quarter candles.
    """