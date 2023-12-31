# Project description

## Important
**! Arguments' naming has been changed since 0.1.x release. !**

## Disclaimer
This project is **not affilliated** with [MOEX](https://www.moex.com).

You should refer to MOEX's official documents for terms of use the market data.

## Description

The package allows to get quotes and data from [MOEX ISS](https://iss.moex.com/iss/reference/) interface over HTTPS.

[API docs](https://github.com/cdracer/moex-importer/wiki/Documentation) for moeximporter.

## Installation
Install `moeximporter`:

`$ pip install moeximporter`

## Requirements
- pandas

## Examples
### Importing modules
```
# Import required modules
from datetime import date
from moeximporter import MoexImporter, MoexSecurity, MoexCandlePeriods
```

### Initialization
Class `MoexImporter` is used for all https-requests to the ISS API. You should create at least one copy of the class to use for data-requests.

You can pass your own http-header to the class-constructor. `_loadinfo` flag allows to get additional data from the exchange during the class initialization. You may use this data to check available *engines* and *markets*. You do not need to set this flag to `True` if you often create copies of the class to speedup the code. Additional data isn't required for
```
# Create an object to access ISS API requests
mi = MoexImporter()

# Get all traded securities
seclist = mi.getSecuritiesAllTraded()

# Get all traded bonds
seclist = mi.getBondsAllTraded()

# Get all traded shares
seclist = mi.getSharesAllTraded()

# Search for traded security
seclist = mi.searchForSecurityTraded('ОФЗ')

```

### Working with securities
Class `MoexSecuirty` is used to get quotes and additional data for the security. You should pass an appropriate ticker and `MoexImporter` object that you have created before.

```
# Create an object to access sequirity data
sec = MoexSecurity('GAZP', mi)

# Print information about security
print(sec)

# Request quotes as a pandas DataFrame
quotes_df = sec.getHistoryQuotesAsDataFrame(date(2023, 5, 1), date(2023, 9, 20))

# Request quotes as an array of dicts
quotes_arr = sec.getHistoryQuotesAsArray(date(2023, 5, 1), date(2023, 9, 20))

# Request candles as a pandas DataFrame
candles_df = sec.getCandleQuotesAsDataFrame(date(2023, 5, 1), date(2023, 9, 20), interval=MoexCandlePeriods.Period1Hour)

# Request candles as an array of dicts
candles_arr = sec.getCandleQuotesAsArray(date(2023, 5, 1), date(2023, 9, 20), interval=MoexCandlePeriods.Period1Hour)

```
## Licensing

The package is distributed under MIT License. See details in LICENSE.txt file.
