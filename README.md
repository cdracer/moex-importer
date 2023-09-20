# Project desctiption

## Disclaimer
This project is **not affilliated** with [MOEX](https://www.moex.com).

You should refer to MOEX's official documents for terms of use the market data.

## Description

The package allows to get quotes and data from [MOEX ISS](https://iss.moex.com/iss/reference/) interface over HTTPS.

## Installation
Install `moeximporter`:

`$ pip install moex-importer`

## Requirements
- pandas

## Examples
### Importing modules
```
# Import required modules
from datetime import date
from moeximporter import MoexImporter, MoexSecurity
```

### Initialization
Class `MoexImporter` is used for all https-requests to the ISS API. You should create at least one copy of the class to use for data-requests.

You can pass your own http-header to the class-constructor. `_loadinfo` flag allows to get additional data from the exchange during the class initialization. You may use this data to check availabel *engines* and *markets*. You do not need to set this flag to `True` if you often creates copies of the class to speedup the code. Additional data isn't required for
```
# Create an object to access ISS API requests
mi = MoexImporter()
```

### Working with securities
Class `MoexSecuirty` is used to get quotes and additional data for the security. You should pass an appropriate ticker and `MoexImporter` object that you have created before.

```
# Create an object to access sequirity data
sec = MoexSecurity('GAZP', mi)

# Print information about security
print(sec)

# Request quotes as a pandas DataFrame
quotes_df = security.getHistoryQuotesAsDataFrame(date(2023, 5, 1), date(2023, 9, 20))

# Request quotes as an array of dicts
quotes_arr = security.getHistoryQuotesAsArray(date(2023, 5, 1), date(2023, 9, 20))

```
## Licensing

The package is distributed under MIT License. See details in LICENSE.txt file.
