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
    """Class MoexImporter implements https-queries to MOEX ISS API.
    You should to create at least one instance to work wirh other objects in the package.
    
    Base methods allow to get generic information about available engines and markets,
    request securities lists and quotes.
    
    Quotes requests are wrapped in the MoexSecurity class to improve convenience.
    """
    def __init__(
            self,
            _header = {
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
            },
            _loadinfo = False
        ):
        """Class constructor initializes base variables and load information about
        engines and markets if flag `_loadinfo` is `True`
        
        Parameters
        ----------
        _header : dict, optional
            HTTP-header for all requests to MOEX ISS API.
        _loadinfo: boolean, optional
            If True, engines and markets lists are requested from MOEX ISS. You may get
            this information later with methods getEngines and getMarkets.
        
        """
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
        """Internal method for standartize https-queries to MOEX ISS.
        
        You don't need to call this method directly, but you may use it for
        more specific queries.
        
        This method is wrapped into more direct methods to request data.
        
        Parameters
        ----------
        _type: MoexRequests
            Determines the type of the request. Is used to generate an url with
            required parameters. Algorithms to generate the url are implemented
            in __init__ method of the class.
        _pparams: dict, optional
            Dictionary of pairs id:value where id is the string id of the
            parameter and value is its string value. These parameters are
            used in the url string and should be replace with values.
            Example:
            in the url template
            https://iss.moex.com/iss/securities.json
            parameter __SECCODE__ should be replaced with the actual security
            ticker.  {'__SECCODE___':'GAZP'} is passed to the method.
        _params: dict, optional
            Dictionary of pairs id:value where id is a string id of the
            parameter and value is its value. The value type depends on the
            request and automatically converted for the url in this method.
            Example:
            request
            https://iss.moex.com/iss/securities.json
            may use 2 parameters in some cases: start and is_trading
            https://iss.moex.com/iss/securities.json?start=0&is_trading=1
            the argument should be {'start': 0, 'is_trading': '1'}
                
        Returns
        -------
        array_like
            Returns the array of dictionaries from the MOEX ISS reply.
            This array should be parsed and converted for usage.
            More information you can find on https://iss.moex.com/iss/reference/
        """
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
        """Returns the list of engines.

        Returns
        -------
        array_like
            List of engines.
        """
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
        """Returns the list of markets.
        
        Parameters
        ----------
        _engine: str
            The engine string identifier to get markets list.

        Returns
        -------
        array_like
            List of markets for the specific _engine.
        """
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
        """Returns the specific data for the security.
        
        Parameters
        ----------
        _seccode: str
            Security ticker.

        Returns
        -------
        array_like
            List of specific data for `_seccode`.
        """
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
        """Internal method to request security list.
        
        Parameters
        ----------
        _is_trading: str
            Select traded, non-traded or all securities:
            '' – all securities,
            '0' - non-traded securities,
            '1' - traded securities.
        _engine: str
            Securities for the specific engine.
        _market: str
            Securities for the specific market.
        _query: str
            Search security the part of its name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
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
        """Returns the list of all securities with the specific `_secpart` of the name or ticker.
        
        Parameters
        ----------
        _secpart: str
            Part of the security name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
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
        """Returns the list of traded securities with the specific `_secpart` of the name or ticker.
        
        Parameters
        ----------
        _secpart: str
            Part of the security name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
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
        """Returns the list of non-traded securities with the specific `_secpart` of the name or ticker.
        
        Parameters
        ----------
        _secpart: str
            Part of the security name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
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
        """Returns the list of all securities.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities()
        except Exception as e:
            print('MoexImporter::getSecuritiesAll(): ', e, file=sys.stderr)
        return _res
    
    def getSecuritiesAllTraded(self):
        """Returns the list of traded securities.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='1')
        except Exception as e:
            print('MoexImporter::getSecuritiesAllTraded(): ', e, file=sys.stderr)
        return _res
    
    def getSecuritiesAllNonTraded(self):
        """Returns the list of non-traded securities.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='0')
        except Exception as e:
            print('MoexImporter::getSecuritiesAllNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getBondsAll(self):
        """Returns the list of all local bonds.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='', _engine='stock', _market = 'bonds')
            #_res += self._getSecurities(_is_trading='', _engine='state', _market = 'bonds')
        except Exception as e:
            print('MoexImporter::getAllBonds(): ', e, file=sys.stderr)
        return _res
    
    def getBondsAllTraded(self):
        """Returns the list of all traded local bonds.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='1', _engine='stock', _market = 'bonds')
            #_res += self._getSecurities(_is_trading='1', _engine='state', _market = 'bonds')
        except Exception as e:
            print('MoexImporter::getAllBondsTraded(): ', e, file=sys.stderr)
        return _res

    def getBondsAllNonTraded(self):
        """Returns the list of all non-traded local bonds.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='0', _engine='stock', _market = 'bonds')
            #_res += self._getSecurities(_is_trading='0', _engine='state', _market = 'bonds')
        except Exception as e:
            print('MoexImporter::getAllBondsNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getSharesAll(self):
        """Returns the list of all local shares.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='', _engine='stock', _market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllShares(): ', e, file=sys.stderr)
        return _res
    
    def getSharesAllTraded(self):
        """Returns the list of all traded local shares.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='1', _engine='stock', _market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllSharesTraded(): ', e, file=sys.stderr)
        return _res

    def getSharesAllNonTraded(self):
        """Returns the list of all non-traded local shares.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            _res = self._getSecurities(_is_trading='0', _engine='stock', _market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllSharesNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getHistoryQuotes(self, _engine, _market, _board, _seccode, _from, _till, _tsession, _start):
        """Returns quotes for the specific security.
        
        Parameters
        ----------
        _engine: str
            Specify engine for quotes.
        _market: str
            Specify market for quotes.
        _board: str
            Specify board for quotes.
        _seccode: str
            Security ticker.
        _from: date
            Left bound of daterange for quotes.
        _till: date
            Right bound of daterange for quotes.
        _tsession: MoexSessions
            Specify trading session for quotes.
        _start: int
            Specify cursor for query. MOEX ISS returns only
            limited number of quotes per request. You have to
            shift the cursor to get the next portion.

        Returns
        -------
        array_like
            List of quotes.
        """
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