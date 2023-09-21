from enum import IntEnum

class MoexRequests(IntEnum):
    GetEngines = 10,
    GetMarkets = 20,
    GetSecuritiesAll = 100,
    GetSecuritiesForEngine = 101,
    GetSecuritiesForMarket = 102,
    GetSecuritiesSearch = 103,
    GetSecurity = 150,
    GetHistoryQuotes = 200