import sys
import pandas as pd
from datetime import datetime
from .MoexImporter import MoexImporter
from .MoexSessions import MoexSessions
from .MoexCandlePeriods import MoexCandlePeriods

class MoexSecurity:
    """Class MoexSecurity implements methods to
    store and request security-specific information.
    
    Instance of MoexImporter should be created
    before.
    """
    def __init__(self, seccode, mi):
        """Class constructor initializes base variables
        and loads security-specific information from
        MOEX ISS.
        
        Parameters
        ----------
        seccode: str
            Security ticker from MOEX.
        mi: MoexImporter
            The object of MoexImporter that was
            created before. You can't use the class
            without this object.
        """

        self.seccode = seccode
        """Ticker of the security.
        """
        self.shortname = None
        """Shortname of the security.
        """
        self.mainboard = None
        """Main trading board of the security.
        """     
        self.facecurrency = None
        """Facecurrency of the security.
        """  
        self.facevalue = None
        """Facevalue of the security.
        """
        self.mi = mi
        """MoexImporter object.
        """
        self.boards = {}
        """Boards for the security.
        """
        if isinstance(mi, MoexImporter):
            _tmp = mi.getSecurity(seccode)
            for _ti in _tmp:
                if 'description' in _ti:
                    _sd = _ti['description']
                    for _si in _sd:
                        if _si['name'] == 'SHORTNAME':
                            self.shortname = _si['value']
                        if _si['name'] == 'FACEVALUE':
                            self.facevalue = _si['value']
                        if _si['name'] == 'FACEUNIT':
                            self.facecurrency = _si['value']
                if 'boards' in _ti:
                    _sb = _ti['boards']
                    for _bi in _sb:
                        _dtf = None
                        if _bi['history_from']:
                            _dtf = datetime.strptime(_bi['history_from'], '%Y-%m-%d').date()
                        _dtt = None
                        if _bi['history_till']:
                            _dtt = datetime.strptime(_bi['history_till'], '%Y-%m-%d').date()
                        self.boards[_bi['boardid']] = {
                            'dtfrom': _dtf,
                            'dttill': _dtt,
                            'engine': _bi['engine'],
                            'market': _bi['market'],
                            'title': _bi['title']
                        }
                        if _bi['is_primary'] == 1:
                            self.mainboard = _bi['boardid']
        else:
            print('MoexSecurity::__init__(): must be initialized with MoexImporter object.', file=sys.stderr)
            
    def getHistoryQuotesAsDataFrame(self, dtfrom, dttill, board = None, ts = MoexSessions.MainSession):
        """Returns quotes for the security as a pandas dataframe.
        
        Parameters
        ----------
        dtfrom: date
            The left bound of the range to request quotes.
        dttill: date
            The right bound of the range to request quotes.
        board: str, optional
            Request quotes for the specific board. The primary board
            is used if the parameter is ommited. You can check
            class attribute `boards` to get all available boards.
        ts: MoexSessions, optional
            Request quotes for the specific session. The main session
            is used if the parameter is ommited.

        Returns
        --------
        pd.DataFrame
            Quotes as pandas dataframe.  
            Columns:  
            'TRADEDATE' - date of the quote,  
            'OPEN' - open price,  
            'HIGH' - high price,  
            'LOW' - low price,  
            'CLOSE' - last price,  
            'YIELD' - yield to maturity, may be None for non-bonds,  
            'DURATION' - duration in days, may be None for non-bonds,  
            'VALUE' - trading value in rubles,  
            'QUANTITY' - trading value in securities.  
        """
        _res = None
        try:
            _tmp = self.getHistoryQuotesAsArray(dtfrom=dtfrom, dttill=dttill, board=board, ts=ts)
            _res = pd.DataFrame.from_dict(data=_tmp, )
            _res.set_index(['TRADEDATE',], inplace=True)
            _res.sort_index(inplace=True)
        except Exception as e:
            print('MoexSecurity::getHistoryQuotesAsDataFrame(): ', e, file=sys.stderr)
        return _res
    
    def getHistoryQuotesAsArray(self, dtfrom, dttill, board = None, ts = MoexSessions.MainSession):
        """Returns quotes for the security as an array of dicts.
        
        Parameters
        ----------
        dtfrom: date
            The left bound of the range to request quotes.
        dttill: date
            The right bound of the range to request quotes.
        board: str, optional
            Request quotes for the specific board. The primary board
            is used if the parameter is ommited. You can check
            class attribute `boards` to get all available boards.
        ts: MoexSessions, optional
            Request quotes for the specific session. The main session
            is used if the parameter is ommited.

        Returns
        --------
        array_like
            Quotes as an array of dicts.  
            Dict keys:  
            'TRADEDATE' - date of the quote,  
            'OPEN' - open price,  
            'HIGH' - high price,  
            'LOW' - low price,  
            'CLOSE' - last price,  
            'YIELD' - yield to maturity, may be None for non-bonds,  
            'DURATION' - duration in days, may be None for non-bonds,  
            'VALUE' - trading value in rubles,  
            'QUANTITY' - trading value in securities.  
        """
        _res = []
        if isinstance(self.mi, MoexImporter):
            _tb = self.mainboard
            if board:
                _tb = board
            _rdf = max(dtfrom, self.boards[_tb]['dtfrom'])
            _rdt = min(dttill, self.boards[_tb]['dttill'])
            _st = 0
            _isNext = True
            try:
                while _isNext:
                    _isNext = False
                    _tmp = self.mi.getHistoryQuotes(
                        engine = self.boards[_tb]['engine'],
                        market = self.boards[_tb]['market'],
                        board = _tb,
                        seccode = self.seccode,
                        dtfrom = _rdf,
                        dttill = _rdt,
                        tsession = ts,
                        start = _st,
                    )

                    for _ti in _tmp:
                        if 'history' in _ti:
#                            if _ti['history']:
                            _thq = [
                                {
                                    ('VALUE' if _k =='VOLRUR' else 'QUANTITY' if _k == 'VOLUME' else 'YIELD' if _k == 'YIELDCLOSE' else _k): (datetime.strptime(_sq[_k], '%Y-%m-%d').date() if _k == 'TRADEDATE' else _sq[_k])
                                    for _k in _sq
                                    if _k in ['TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'YIELD', 'DURATION', 'YIELDCLOSE', 'VOLUME', 'VALUE', 'WAPRICE', 'VOLRUR']
                                } for _sq in _ti['history']
                            ]
                            _st += self.mi.limit
                            _res += _thq
                            if len(_thq) == self.mi.limit:
                                _isNext = True
            except Exception as e:
                print('MoexSecurity::getHistoryQuotesAsArray(): ', e, file=sys.stderr)
        return _res
    
    def getCandleQuotesAsDataFrame(self, dtfrom, dttill, board = None, interval = MoexCandlePeriods.Period1Day):
        """Returns candles for the security as a pandas dataframe.
        
        Parameters
        ----------
        dtfrom: date
            The left bound of the range to request quotes.
        dttill: date
            The right bound of the range to request quotes.
        board: str, optional
            Request quotes for the specific board. The primary board
            is used if the parameter is ommited. You can check
            class attribute `boards` to get all available boards.
        interval: MoexCandlePeriods, optional
            Request candles for the specified period. Default is 1 day.

        Returns
        --------
        pd.DataFrame
            Candles as pandas dataframe.
            Columns:
            'begin' - begin time of the candle,
            'end' - end time of the candle,
            'open' - open price,
            'high' - high price,
            'low' - low price,
            'close' - last price,
            'value' - trading value in rubles,
            'quantity' - trading value in securities.
        """
        _res = None
        try:
            if isinstance(interval, MoexCandlePeriods):
                _tmp = self.getCandleQuotesAsArray(dtfrom=dtfrom, dttill=dttill, board=board, interval=interval)
                _res = pd.DataFrame.from_dict(data=_tmp, )
                _res.set_index(['begin',], inplace=True)
                _res.sort_index(inplace=True)
        except Exception as e:
            print('MoexSecurity::getCandleQuotesAsDataFrame(): ', e, file=sys.stderr)
        return _res
    
    def getCandleQuotesAsArray(self, dtfrom, dttill, board = None, interval = MoexCandlePeriods.Period1Day):
        """Returns quotes for the security as an array of dicts.
        
        Parameters
        ----------
        dtfrom: date
            The left bound of the range to request quotes.
        dttill: date
            The right bound of the range to request quotes.
        board: str, optional
            Request quotes for the specific board. The primary board
            is used if the parameter is ommited. You can check
            class attribute `boards` to get all available boards.
        interval: MoexCandlePeriods, optional
            Request candles for the specified period. Default is 1 day.

        Returns
        --------
        array_like
            Candles as an array of dicts.
            Dict keys:
            'begin' - begin time of the candle,
            'end' - end time of the candle,
            'open' - open price,
            'high' - high price,
            'low' - low price,
            'close' - last price,
            'value' - trading value in rubles,
            'quantity' - trading value in securities.
        """
        _res = []
        if isinstance(self.mi, MoexImporter) and isinstance(interval, MoexCandlePeriods):
            _tb = self.mainboard
            if board:
                _tb = board
            _rdf = max(dtfrom, self.boards[_tb]['dtfrom'])
            _rdt = min(dttill, self.boards[_tb]['dttill'])
            _st = 0
            _isNext = True
            try:
                while _isNext:
                    _isNext = False
                    _tmp = self.mi.getCandles(
                        engine = self.boards[_tb]['engine'],
                        market = self.boards[_tb]['market'],
                        board = _tb,
                        seccode = self.seccode,
                        dtfrom = _rdf,
                        dttill = _rdt,
                        candleperiod = interval,
                        start = _st,
                    )

                    for _ti in _tmp:
                        if 'candles' in _ti:
                            _thq = [
                                {
                                    ('quantity' if _k == 'volume' else _k): (datetime.strptime(_sq[_k], '%Y-%m-%d %H:%M:%S') if _k == 'begin' else datetime.strptime(_sq[_k], '%Y-%m-%d %H:%M:%S') if _k == 'end' else _sq[_k])
                                    for _k in _sq
                                    if _k in ['open', 'close', 'low', 'high', 'value', 'volume', 'begin', 'end',]
                                } for _sq in _ti['candles']
                            ]
                            _st += self.mi.limit
                            _res += _thq
                            if len(_thq) == self.mi.limit:
                                _isNext = True
            except Exception as e:
                print('MoexSecurity::getCandleQuotesAsArray(): ', e, file=sys.stderr)
        return _res
            
    def __str__(self):
        _res = f'''
Security {self.seccode:s}
Main board: {self.mainboard:s} ({self.boards[self.mainboard]["title"]})
Engine: {self.boards[self.mainboard]["engine"]}
Market: {self.boards[self.mainboard]["market"]}
History for the main board is available from {self.boards[self.mainboard]["dtfrom"]} till {self.boards[self.mainboard]["dttill"]}
'''
        return _res