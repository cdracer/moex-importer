import urllib.error
import urllib.parse
import urllib.request
import json
import ssl
import sys

from ._MoexRequests import _MoexRequests

class MoexImporter:
    """Class MoexImporter implements https-queries to MOEX ISS API.
    You should to create at least one instance to work with other objects in the package. \ \
    
    Base methods allow to get generic information about available engines and markets,
    request securities lists and quotes. \ \
    
    Quotes requests are wrapped in the MoexSecurity class to improve convenience.
    """
    def __init__(
            self,
            header = {
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
            },
            loadinfo = False
        ):
        """Class constructor initializes base variables and load information about
        engines and markets if flag `_loadinfo` is `True`
        
        Parameters
        ----------
        header : dict, optional
            HTTP-header for all requests to MOEX ISS API.
        loadinfo: boolean, optional
            If `True`, engines and markets lists are requested from MOEX ISS. You may get
            this information later with methods getEngines and getMarkets.
        
        """

        self.base_url = 'https://iss.moex.com/iss'
        """Base url for MOEX ISS API.
        """
        self.base_header = header
        """Header.
        """
        self.limit = 100
        """Limit for maximum lines per request.
        """
        self.base_values = {
            'iss.meta': 'off',
            'iss.json': 'extended',
        }
        """Standard request parameters.
        """
        self.requests_dictionary = {
            _MoexRequests.GetEngines: {
                'postfix': '/engines.json',
                'postfix_params': [],
                'params': {},
            },
            _MoexRequests.GetMarkets: {
                'postfix': '/engines/__ENGINE__/markets.json',
                'postfix_params': [
                    '__ENGINE__',
                ],
                'params': {},
            },
            _MoexRequests.GetSecuritiesAll: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                },
            },
            _MoexRequests.GetSecuritiesForEngine: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                    'engine': 's',
                },
            },
            _MoexRequests.GetSecuritiesForMarket: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                    'engine': 's',
                    'market': 's',
                },
            },
            _MoexRequests.GetSecuritiesSearch: {
                'postfix': '/securities.json',
                'postfix_params': [],
                'params': {
                    'start': 'd',
                    'is_trading': 's',
                    'q': 's',
                },
            },
            _MoexRequests.GetSecurity: {
                'postfix': '/securities/__SECCODE__.json',
                'postfix_params': [
                    '__SECCODE__',
                ],
                'params': {},
            },
            _MoexRequests.GetHistoryQuotes: {
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
            _MoexRequests.GetCandleQuotes: {
                'postfix': '/engines/__ENGINE__/markets/__MARKET__/boards/__BOARD__/securities/__SECCODE__/candles.json',
                'postfix_params': [
                    '__ENGINE__',
                    '__MARKET__',
                    '__SECCODE__',
                    '__BOARD__',
                ],
                'params': {
                    'from': '%Y-%m-%d',
                    'till': '%Y-%m-%d',
                    'interval': 'd',
                    'start': 'd',
                },
            },
        }
        """Requests library.
        """
        self.method = 'GET'
        """Request method.
        """
        self.engines = []
        """Engines.
        """
        self.markets = {}
        """Markets.
        """
        if loadinfo:
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
        _type: _MoexRequests
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
        _values = self.base_values.copy()
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
                        _values[_pp] = f'{_params[_pp]:{_rp[_pp]:s}}'
            _data = urllib.parse.urlencode(_values)
            _url += f'?{_data:s}'
            _req = urllib.request.Request(_url, headers=self.base_header, method = self.method)
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
            _tmp = self._MoexRequest(_MoexRequests.GetEngines)
            if isinstance(_tmp, list):
                for _ti in _tmp:
                    if isinstance(_ti, dict):
                        if 'engines' in _ti.keys():
                            _res = _ti['engines']
        except Exception as e:
            print('MoexImporter::getEngines(): ', e, file=sys.stderr)
        return _res
    
    def getMarkets(self, engine):
        """Returns the list of markets.
        
        Parameters
        ----------
        engine: str
            The engine string identifier to get markets list.

        Returns
        -------
        array_like
            List of markets for the specific `engine`.
        """
        _res = None
        try:
            _tmp = self._MoexRequest(
                _MoexRequests.GetMarkets,
                _pparams = {
                    '__ENGINE__': engine,
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
    
    def getSecurity(self, seccode):
        """Returns the specific data for the security.
        
        Parameters
        ----------
        seccode: str
            Security ticker.

        Returns
        -------
        array_like
            List of specific data for `seccode`.
        """
        _res = None
        try:
            _res = self._MoexRequest(
                _MoexRequests.GetSecurity,
                _pparams = {
                    '__SECCODE__': seccode,
                }
            )
        except Exception as e:
            print('MoexImporter::getSecurity(): ', e, file=sys.stderr)
        return _res
    
    def _getSecurities(self, is_trading='', engine=None, market=None, query = None):
        """Internal method to request security list.
        
        Parameters
        ----------
        is_trading: str  
            Select traded, non-traded or all securities:  
            '' – all securities,  
            '0' - non-traded securities,  
            '1' - traded securities.  
        engine: str
            Securities for the specific _engine.
        market: str
            Securities for the specific market.
        query: str
            Search security the part of its name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        _params = {
            'start': 0,
            'is_trading': is_trading,
        }
        _type = _MoexRequests.GetSecuritiesAll
        if engine:
            _type = _MoexRequests.GetSecuritiesForEngine
            _params['engine'] = engine
            if market:
                _type = _MoexRequests.GetSecuritiesForMarket
                _params['market'] = market
        if query:
            _type = _MoexRequests.GetSecuritiesSearch
            _params['q'] = query
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

    def searchForSecurity(self, secpart):
        """Returns the list of all securities with the specific `secpart` of the name or ticker.
        
        Parameters
        ----------
        secpart: str
            Part of the security name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            if isinstance(secpart, str):
                _res = self._getSecurities(query=secpart)
            else:
                print('MoexImporter::searchForSecurity(): secpart should be str', file=sys.stderr)
        except Exception as e:
            print('MoexImporter::searchForSecurity(): ', e, file=sys.stderr)
        return _res
    
    def searchForSecurityTraded(self, secpart):
        """Returns the list of traded securities with the specific `secpart` of the name or ticker.
        
        Parameters
        ----------
        secpart: str
            Part of the security name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            if isinstance(secpart, str):
                _res = self._getSecurities(is_trading='1', query=secpart)
            else:
                print('MoexImporter::searchForSecurityTraded(): secpart should be str', file=sys.stderr)
        except Exception as e:
            print('MoexImporter::searchForSecurityTraded(): ', e, file=sys.stderr)
        return _res
    
    def searchForSecurityNonTraded(self, secpart):
        """Returns the list of non-traded securities with the specific `secpart` of the name or ticker.
        
        Parameters
        ----------
        secpart: str
            Part of the security name or ticker.

        Returns
        -------
        array_like
            List of securities.
        """
        _res = None
        try:
            if isinstance(secpart, str):
                _res = self._getSecurities(is_trading='0', query=secpart)
            else:
                print('MoexImporter::searchForSecurityNonTraded(): secpart should be str', file=sys.stderr)
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
            _res = self._getSecurities(is_trading='1')
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
            _res = self._getSecurities(is_trading='0')
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
            _res = self._getSecurities(is_trading='', engine='stock', market = 'bonds')
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
            _res = self._getSecurities(is_trading='1', engine='stock', market = 'bonds')
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
            _res = self._getSecurities(is_trading='0', engine='stock', market = 'bonds')
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
            _res = self._getSecurities(is_trading='', engine='stock', market = 'shares')
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
            _res = self._getSecurities(is_trading='1', engine='stock', market = 'shares')
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
            _res = self._getSecurities(is_trading='0', engine='stock', market = 'shares')
        except Exception as e:
            print('MoexImporter::getAllSharesNonTraded(): ', e, file=sys.stderr)
        return _res
    
    def getHistoryQuotes(self, engine, market, board, seccode, dtfrom, dttill, tsession, start):
        """Returns quotes for the specific security.
        
        Parameters
        ----------
        engine: str
            Specify engine for quotes.
        market: str
            Specify market for quotes.
        board: str
            Specify board for quotes.
        seccode: str
            Security ticker.
        dtfrom: date
            Left bound of daterange for quotes.
        dttill: date
            Right bound of daterange for quotes.
        tsession: MoexSessions
            Specify trading session for quotes.
        start: int
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
                _MoexRequests.GetHistoryQuotes,
                _pparams = {
                    '__SECCODE__': seccode,
                    '__ENGINE__': engine,
                    '__MARKET__': market,
                    '__BOARD__': board,
                },
                _params = {
                    'from': dtfrom,
                    'till': dttill,
                    'tradingsession': tsession,
                    'start': start,
                    'limit': self.limit,
                }
            )
        except Exception as e:
            print('MoexImporter::getHistoryQuotes(): ', e, file=sys.stderr)
        return _res
    
    def getCandles(self, engine, market, board, seccode, dtfrom, dttill, start, candleperiod):
        """Returns candles for the specific security.
        
        Parameters
        ----------
        engine: str
            Specify engine for quotes.
        market: str
            Specify market for quotes.
        board: str
            Specify board for quotes.
        seccode: str
            Security ticker.
        dtfrom: date
            Left bound of daterange for quotes.
        dttill: date
            Right bound of daterange for quotes.
        start: int
            Specify cursor for query. MOEX ISS returns only
            limited number of quotes per request. You have to
            shift the cursor to get the next portion.
        candleperiod: MoexCandlePeriods
            Period for candle.

        Returns
        -------
        array_like
            List of quotes.
        """
        _res = None
        try:
            _res = self._MoexRequest(
                _MoexRequests.GetCandleQuotes,
                _pparams = {
                    '__SECCODE__': seccode,
                    '__ENGINE__': engine,
                    '__MARKET__': market,
                    '__BOARD__': board,
                },
                _params = {
                    'from': dtfrom,
                    'till': dttill,
                    'interval': candleperiod,
                    'start': start,
                }
            )
        except Exception as e:
            print('MoexImporter::getCandles(): ', e, file=sys.stderr)
        return _res