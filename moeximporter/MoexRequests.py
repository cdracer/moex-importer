from enum import IntEnum

class MoexRequests(IntEnum):
    GetEngines = 10,
    GetMarkets = 20,
    GetSecurities = 100,
    GetSecurity = 101,
    GetHistoryQuotes = 200