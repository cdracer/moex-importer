import sys
import pandas as pd
from datetime import datetime
from .MoexImporter import MoexImporter
from .MoexSessions import MoexSessions

class MoexSecurity:
    """Class MoexSecurity implements methods to
    store and request security-specific information.
    
    Instance of MoexImporter should be created
    before.
    """
    def __init__(self, _seccode, _mi):
        """Class constructor initializes base variables
        and loads security-specific information from
        MOEX ISS.
        
        Parameters
        ----------
            _seccode: str
                Security ticker from MOEX.
            _mi: MoexImporter
                The object of MoexImporter that was
                created before. You can't use the class
                without this object.
        """
        self.seccode = _seccode
        self.shortname = None
        self.mainboard = None
        self.facecurrency = None
        self.facevalue = None
        self.mi = _mi
        self.boards = {}
        if isinstance(_mi, MoexImporter):
            _tmp = _mi.getSecurity(_seccode)
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
            
    def getHistoryQuotesAsDataFrame(self, _dtf, _dtt, _board = None, _ts = MoexSessions.MainSession):
        """Returns quotes for the security as a pandas dataframe.
        
        Parameters
        ----------
            _dtf: date
                The left bound of the range to request quotes.
            _dtt: date
                The right bound of the range to request quotes.
            _board: str, optional
                Request quotes for the specific board. The primary board
                is used if the parameter is ommited. You can check
                class attribute `boards` to get all available boards.
            _ts: MoexSessions, optional
                Request quotes for the specific session. The main session
                is used if the parameter is ommited.

        Returns
        --------
            pd.DataFrame:
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
            _tmp = self.getHistoryQuotesAsArray(_dtf=_dtf, _dtt=_dtt, _board=_board, _ts=_ts)
            _res = pd.DataFrame.from_dict(data=_tmp, )
            _res.set_index(['TRADEDATE',], inplace=True)
            _res.sort_index(inplace=True)
        except Exception as e:
            print('MoexSecurity::getHistoryQuotesAsDataFrame(): ', e, file=sys.stderr)
        return _res
    
    def getHistoryQuotesAsArray(self, _dtf, _dtt, _board = None, _ts = MoexSessions.MainSession):
        """Returns quotes for the security as an array of dicts.
        
        Parameters
        ----------
            _dtf: date
                The left bound of the range to request quotes.
            _dtt: date
                The right bound of the range to request quotes.
            _board: str, optional
                Request quotes for the specific board. The primary board
                is used if the parameter is ommited. You can check
                class attribute `boards` to get all available boards.
            _ts: MoexSessions, optional
                Request quotes for the specific session. The main session
                is used if the parameter is ommited.

        Returns
        --------
            array_like:
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
            if _board:
                _tb = _board
            _rdf = max(_dtf, self.boards[_tb]['dtfrom'])
            _rdt = min(_dtt, self.boards[_tb]['dttill'])
            _st = 0
            _isNext = True
            try:
                while _isNext:
                    _isNext = False
                    _tmp = self.mi.getHistoryQuotes(
                        _engine = self.boards[_tb]['engine'],
                        _market = self.boards[_tb]['market'],
                        _board = _tb,
                        _seccode = self.seccode,
                        _from = _rdf,
                        _till = _rdt,
                        _tsession = _ts,
                        _start = _st,
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
                print('MoexSecurity::getHistoryQuotes(): ', e, file=sys.stderr)
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