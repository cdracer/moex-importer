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
        self.limit = 100
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
            MoexRequests.GetSecuritiesAll: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                },
            },
            MoexRequests.GetSecuritiesForEngine: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                    'engine': 's',
                },
            },
            MoexRequests.GetSecuritiesForMarket: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                    'engine': 's',
                    'market': 's',
                },
            },
            MoexRequests.GetSecuritiesSearch: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                    'q': 's',
                },
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
            if _resp:
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
    
    def _getSecurities(self, _is_trading='', _engine=None, _market=None, _query = None):
        _res = None
        _params = {
            'start': 0,
            'is_trading': _is_trading,
        }
        _type = MoexRequests.GetSecuritiesAll
        if _engine:
            _type = MoexRequests.GetSecuritiesForEngine
            _params['engine'] = _engine
            if _market:
                _type = MoexRequests.GetSecuritiesForMarket
                _params['market'] = _market
        if _query:
            _type = MoexRequests.GetSecuritiesSearch
            _params['q'] = _query
        try:
            isNext = True
            while isNext:
                isNext = False
                _tmp = self._MoexRequest(
                    _type,
                    _params = _params
                )
                for _ti in _tmp:
                    if 'securities' in _ti:
                        if _ti['securities']:
                            if not _res:
                                _res = []
                                
                            _res += [
                                {
                                    _k:_sq[_k]
                                    for _k in _sq
                                    if _k in ['secid', 'shortname', 'name', 'regnumber', 'isin', 'is_traded', 'emitent_id', 'emitent_title', 'emitent_inn', 'gosreg', 'primary_boardid']
                                } for _sq in _ti['securities']
                            ]
                            _params['start'] += self.limit
                            if len(_ti['securities']) == self.limit:
                                isNext = True
                    
        except Exception as e:
            print('MoexImporter::_getSecurities(): ', e, file=sys.stderr)
        return _res

    def searchForSecurity(self, _secpart):
        _res = None
        try:
            if isinstance(_secpart, str):
                _res = self._getSecurities(_query=_secpart)
            else:
                print('MoexImporter::searchForSecurity(): _secpart should be str', file=sys.stderr)
        except Exception as e:
            print('MoexImporter::searchForSecurity(): ', e, file=sys.stderr)
        return _res
    
    def searchForSecurityTraded(self, _secpart):
        _res = None
        try:
            if isinstance(_secpart, str):
                _res = self._getSecurities(_is_trading='1', _query=_secpart)
            else:
                print('MoexImporter::searchForSecurityTraded(): _secpart should be str', file=sys.stderr)
        except Exception as e:
            print('MoexImporter::searchForSecurityTraded(): ', e, file=sys.stderr)
        return _res
    
    def searchForSecurityNonTraded(self, _secpart):
        _res = None
        try:
            if isinstance(_secpart, str):
                _res = self._getSecurities(_is_trading='0', _query=_secpart)
            else:
                print('MoexImporter::searchForSecurityNonTraded(): _secpart should be str', file=sys.stderr)
        except Exception as e:
            print('MoexImporter::searchForSecurityNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getSecuritiesAll(self):
        _res = None
        try:
            _res = self._getSecurities()
        except Exception as e:
            print('MoexImporter::getSecuritiesAll(): ', e, file=sys.stderr)
        return _res
    
    def getSecuritiesAllTraded(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='1')
        except Exception as e:
            print('MoexImporter::getSecuritiesAllTraded(): ', e, file=sys.stderr)
        return _res
    
    def getSecuritiesAllNonTraded(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='0')
        except Exception as e:
            print('MoexImporter::getSecuritiesAllNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getBondsAll(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='', _engine='stock', _market = 'bonds')
            #_res += self._getSecurities(_is_trading='', _engine='state', _market = 'bonds')
        except Exception as e:
            print('MoexImporter::getAllBonds(): ', e, file=sys.stderr)
        return _res
    
    def getBondsAllTraded(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='1', _engine='stock', _market = 'bonds')
            #_res += self._getSecurities(_is_trading='1', _engine='state', _market = 'bonds')
        except Exception as e:
            print('MoexImporter::getAllBondsTraded(): ', e, file=sys.stderr)
        return _res

    def getBondsAllNonTraded(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='0', _engine='stock', _market = 'bonds')
            #_res += self._getSecurities(_is_trading='0', _engine='state', _market = 'bonds')
        except Exception as e:
            print('MoexImporter::getAllBondsNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getSharesAll(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='', _engine='stock', _market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllShares(): ', e, file=sys.stderr)
        return _res
    
    def getSharesAllTraded(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='1', _engine='stock', _market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllSharesTraded(): ', e, file=sys.stderr)
        return _res

    def getSharesAllNonTraded(self):
        _res = None
        try:
            _res = self._getSecurities(_is_trading='0', _engine='stock', _market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllSharesNonTraded(): ', e, file=sys.stderr)
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
                    'limit': self.limit,
                }
            )
        except Exception as e:
            print('MoexImporter::getHistoryQuotes(): ', e, file=sys.stderr)
        return _res