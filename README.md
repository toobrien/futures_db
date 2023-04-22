# NOTE

Development on this repository is no longer active! If you have been using it, you may want to follow the instructions to convert your database to the smaller, faster, and simpler version at [`futures_db_v2`](https://www.github.com/toobrien/futures_db_v2).


----------



A futures database backfilled from the Stevens Reference Futures[^1] data set, and updated daily with CME's published settlement files. `update.py` contains options for initializing the database, updating it daily, and cleaning up the source data.

Additionaly, VX settlements are provided from CBOE, and spot prices are available for select contracts.

# USAGE

Included are sample scripts, `init.sh` and `update.sh`. Both invoke `update.py` with the parameters that I use. Running `init.sh` will initialize the database with SRF, CBOE, and CME data. Before initializing the database, add your Nasdaq data link API key to `config.json`. Afterward, run `update.sh` to update the database daily, using only CBOE and CME data. This script runs much more quickly.

Although you can use SRF for daily updates, the sample update script does not, since CME offers almost all of the same data. The subscription is no longer needed.

### SCHEMA

```
    ohlc
        contract_id                     TEXT
        exchange                        TEXT
        name                            TEXT
        month                           TEXT
        year                            TEXT
        date                            TEXT
        open                            REAL
        high                            REAL
        low                             REAL
        settle                          REAL
        volume                          INTEGER
        open_interest                   INTEGER
        PRIMARY KEY(contract_id, date)  

    metadata
        contract_id TEXT PRIMARY KEY
        from_date   TEXT
        to_date     TEXT

    spot
        symbol  TEXT
        date    TEXT
        price   REAL

    cme_opts
        date                TEXT,
        name                TEXT,
        strike              REAL,
        expiry              TEXT, 
        call                INTEGER,
        last_traded         TEXT,
        settle              REAL,
        settle_delta        REAL,
        high_limit          REAL,
        low_limit           REAL,
        high_bid            REAL,
        low_bid             REAL,
        previous_volume     INTEGER,
        previous_interest   INTEGER,
        underlying_symbol   TEXT,
        underlying_exchange TEXT,
        underlying_id       TEXT,
        PRIMARY KEY(date, name, strike, expiry, call)
```

Note: "symbol" from the spot table can be joined on "name" from ohlc records.

### Updating the database

- The CME FTP is updated periodically throughout the day, with final settlement values available after `6:30 PM CT`.
- SRF is updated nightly (Tues-Sat) at `12:45 AM ET`.
- update.sh is a sample wrapper for update.py, which allows you to retrieve, clean (remove), and archive daily updates.

### Extending the database

The CME daily data allows you to record more contracts than exist in SRF. To track a contract not already in `contracts.csv` add a line for the contract. The `globex` symbol is the only strictly necessary field. Note that this file also provides the mapping between SRF and Globex codes. The database itself will use the Globex codes for all symbols.

I have populated `contracts.csv` with all of the contracts from SRF, plus some of those from CME that interest me. Feel free to edit the list as you wish.

### Notes and known issues

- SRF contains data from ICE exchanges. I have disabled some of these contracts, but in some cases have kept the data enabled using similar that trade on Globex. For example, `ATW` (ICE) can be tracked with `MTF` (NYMEX), as these products share the same index, though they trade on different exchanges. Similarly, NYBOT softs are tracked by the CME group, though they don't trade. In these cases, the SRF ICE data will populate the database, and daily updates will be ingested using the CME group data. These substitutes are not perfect. While new settlement values obtained from the CME data are likely similar to the historical SRF data, the open, high, low, volume, and open interest may be missing. More importantly, the CME contracts do not have the exact same expiration schedule as the ICE contracts. If you prefer, you can disable these contracts entirely using the `enabled` column in `contracts.csv`, or set the Globex code to `""` to retain the initial ICE data from SRF without any future updates from the CME group data.

- `from_date` is the date of the earliest data point for that contract, not necessarily its listing date. `to_date` should be the `last trading date` for the contract. These fields can be used to calculate the number of days a contract has been trading, or the number of days to expiration.

- `volume` is, for data derived from Steven's Reference Futures, the volume of that record's trading date. For CME-derived data, `volume` is from the previous day. For both data sets, `open interest` is from the previous day.

- The CME periodically emends errors in its data, posting these to https://www.cmegroup.com/market-data/files/final-settlement-changes.xls. These changes are not yet incorporated into the database.

- VX split 10:1 around 2007-03-26. I have adjusted the prices, volume, and open interest. A few of the volume and open interest values for March 2007 do not seem accurate, however.

### Future plans

- A new table for fundamental data
- Additional spot prices for grains, livestock, softs, and metals

[^1]: https://data.nasdaq.com/data/SRF-reference-futures
