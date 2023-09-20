import urllib.error
import urllib.parse
import urllib.request
import json
import ssl
import sys
import pandas as pd
from datetime import datetime

from .MoexRequests import MoexRequests
from .MoexSessions import MoexSessions

class MoexImporter:
    def __init__(
            self,
            _header = {
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
            },
            _loadinfo = False
        ):
        self.base_url = 'https://iss.moex.com/iss'
        self.base_header = _header
        self.base_values = {
            'iss.meta': 'off',
            'iss.json': 'extended',
        }
        self.requests_dictionary = {
            MoexRequests.GetEngines: {
                'postfix': '/engines.json',
                'postfix_params': [],
                'params': {},
            },
            MoexRequests.GetMarkets: {
                'postfix': '/engines/__ENGINE__/markets.json',
                'postfix_params': [
                    '__ENGINE__',
                ],
                'params': {},
            },
            MoexRequests.GetSecurity: {
                'postfix': '/securities/__SECCODE__.json',
                'postfix_params': [
                    '__SECCODE__',
                ],
                'params': {},
            },
            MoexRequests.GetHistoryQuotes: {
                'postfix': '/history/engines/__ENGINE__/markets/__MARKET__/boards/__BOARD__/securities/__SECCODE__.json',
                #'postfix': '/history/engines/__ENGINE__/markets/__MARKET__/securities/__SECCODE__.json',
                'postfix_params': [
                    '__ENGINE__',
                    '__MARKET__',
                    '__SECCODE__',
                    '__BOARD__',
                ],
                'params': {
                    'from': '%Y-%m-%d',
                    'till': '%Y-%m-%d',
                    'start': 'd',
                    'tradingsession': 'd',
                    'limit': 'd',
                },
            },
        }
        self.method = 'GET'
        self.engines = []
        self.markets = {}
        if _loadinfo:
            self.engines = self.getEngines()
            if self.engines:
                for _en in self.engines:
                    self.markets[_en['name']] = self.getMarkets(_en['name'])
                       
    def _MoexRequest(self, _type, _pparams = None, _params = None):
        _res = None
        _values = self.base_values
        try:
            _mr = self.requests_dictionary[_type]
            _url = self.base_url + _mr['postfix']
            _rpp = _mr['postfix_params']
            if _pparams:
                for _ppkey in _pparams:
                    if _ppkey in _rpp:
                        _url = _url.replace(_ppkey, _pparams[_ppkey]) 
            _rp = _mr['params']
            if _params:
                for _pp in _params:
                    if _pp in _rp:
                        _values[_pp] = f'{_params[_pp]:{_rp[_pp]}}'
            _data = urllib.parse.urlencode(_values)
            _req = urllib.request.Request(_url+f'?{_data:s}', headers=self.base_header, method = self.method)
            _resp = None
            try:
                _resp = urllib.request.urlopen(_req, context=ssl._create_unverified_context())
            except urllib.error.HTTPError as e:
                print('_MoexRequest(): HTTP Error ', e.code)
            except urllib.error.URLError as e:
                print('_MoexRequest(): Error ', e.reason)
            else:
                _res = json.load(_resp)
            if _resp is not None:
                _resp.close()
        except Exception as e:
            print('MoexImporter::_MoexRequest(): ', e, file=sys.stderr)
        return _res
    
    def getEngines(self):
        _res = None
        try:
            _tmp = self._MoexRequest(MoexRequests.GetEngines)
            if isinstance(_tmp, list):
                for _ti in _tmp:
                    if isinstance(_ti, dict):
                        if 'engines' in _ti.keys():
                            _res = _ti['engines']
        except Exception as e:
            print('MoexImporter::getEngines(): ', e, file=sys.stderr)
        return _res
    
    def getMarkets(self, _engine):
        _res = None
        try:
            _tmp = self._MoexRequest(
                MoexRequests.GetMarkets,
                _pparams = {
                    '__ENGINE__': _engine,
                },
            )
            if isinstance(_tmp, list):
                for _ti in _tmp:
                    if isinstance(_ti, dict):
                        if 'markets' in _ti.keys():
                            _res = _ti['markets']
        except Exception as e:
            print('MoexImporter::getMarkets(): ', e, file=sys.stderr)
        return _res
    
    def getSecurity(self, _seccode):
        _res = None
        try:
            _res = self._MoexRequest(
                MoexRequests.GetSecurity,
                _pparams = {
                    '__SECCODE__': _seccode,
                }
            )
        except Exception as e:
            print('MoexImporter::getSecurity(): ', e, file=sys.stderr)
        return _res
    
    def getHistoryQuotes(self, _engine, _market, _board, _seccode, _from, _till, _tsession, _start):
        _res = None
        try:
            _res = self._MoexRequest(
                MoexRequests.GetHistoryQuotes,
                _pparams = {
                    '__SECCODE__': _seccode,
                    '__ENGINE__': _engine,
                    '__MARKET__': _market,
                    '__BOARD__': _board,
                },
                _params = {
                    'from': _from,
                    'till': _till,
                    'tradingsession': _tsession,
                    'start': _start,
                    'limit': 100,
                }
            )
        except Exception as e:
            print('MoexImporter::getHistoryQuotes(): ', e, file=sys.stderr)
        return _res

class MoexSecurity:
    def __init__(self, _seccode, _mi):
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
                            _st += 100
                            _res += _thq
                            if len(_thq) == 100:
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
History from {self.boards[self.mainboard]["dtfrom"]} till {self.boards[self.mainboard]["dttill"]}
'''
        return _res
