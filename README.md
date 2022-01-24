A futures database backfilled from the Stevens Reference Futures[^1] data set, and updated daily with CME's published settlement files. `update.py` contains options for initializing the database, updating it daily with SRF, CME (or both), and cleaning up the source data. 

Included is a sample `update.sh` which will initialize the database using SRF and the latest CME data. Before initializing the database, add your Nasdaq data link API key to `config.json`. Afterward, edit this file once the SRF subscription is no longer necessary. The `cme_latest` argument is sufficient to continue daily updates.

### SCHEMA

```
    ohlc
        contract_id                       TEXT
        exchange                          TEXT
        contract                          TEXT
        name                              TEXT
        month                             TEXT
        year                              TEXT
        open                              REAL
        high                              REAL
        low                               REAL
        settle                            REAL
        previous day's volume             INTEGER
        previous day's open interest      INTEGER
        PRIMARY KEY(contract_id, date)


    metadata
        contract_id   TEXT PRIMARY KEY
        from_date     TEXT
        to_date       TEXT
```

### Extending the database

The CME daily data allows you to record more contracts than exist in SRF. To track a contract not already in `contracts.csv` add a line for the contract. The `globex` symbol is the only strictly necessary field. Note that this file also provides the mapping between SRF and Globex codes. The database itself will use the Globex codes for all symbols.

I have populated `contracts.csv` with all of the contracts from SRF, plus some of those from CME that interest me. Feel free to edit the list as you wish.

### Data quirks

- SRF contains data from ICE exchanges. I have disabled some of these contracts, but in some cases have kept the data enabled using Globex equivalents. For example, `ATW` (ICE) can be tracked with `MTF` (NYMEX), as these products share the same index, though they trade on different exchanges. In this case, the SRF ICE data will populate the database, and daily updates will be ingested using the NYMEX data. If you don't want this behavior, you can disable the contract entirely using the `enabled` column in `contracts.csv`, or set the Globex code to `""` to retain the initial ICE data from SRF without any future updates from NYMEX.

- `from_date` is the date of the earliest data point for that contract, not necessarily its listing date. `to_date` should be the `last trading date` for the contract. These fields can be used to calculate the number of days a contract has been trading, or the number of days to expiration.

- `volume` is, for data derived from Steven's Reference Futures, the volume of that record's trading date. For CME-derived data, `volume` is from the previous day. For both data sets, `open interest` is from the previous day.

- The CME periodically emends errors in its data, posting these to https://www.cmegroup.com/market-data/files/final-settlement-changes.xls. These changes are not yet incorporated into the database.

### Future plans

- VX contracts from CBOE
- A new table for fundamental data
- A new table for spot prices from local markets

[^1]: https://data.nasdaq.com/data/SRF-reference-futures