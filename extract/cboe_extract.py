from datetime   import datetime
from json       import loads
from re         import match
from csv        import reader
from requests   import get
from re         import match
from sys        import argv
from time       import time


# VIX 1990-2003:        https://cdn.cboe.com/resources/us/indices/vixarchive.xls
# VIX 2004 onward:      https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv
# VVIX 2006 onward:     https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv
# VX 2013 onward:       https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{yyyy-mm-dd}.csv
# VX K04 - Z13:         https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{M}{YY}_VX.csv
# VX final settlement:  https://www.cboe.com/us/futures/market_statistics/final_settlement_prices/csv
# daily VX settlements: https://www.cboe.com/us/futures/market_statistics/settlement/csv?dt={yyyy-mm-dd}


config = loads(open("./config.json").read())

DATE_FMT    = config["date_fmt"]
LOG_FMT     = config["log_fmt"]
VX_FN_FMT   = "{date}_cfe_vx_{month}{year}.csv"
CALENDAR    = { "F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z" }


def get_csv(url: str):

    start = time()

    res = get(url)

    print(LOG_FMT.format("cboe_extract", "finish", f"{time() - start: 0.1f}", f"get_csv {url}", res.status_code))

    return res


def get_history(date: str):

    input_path = config["input_path"]

    # "archive" (may 2004 - dec 2013)
    
    url_template = "https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{0}{1}_VX.csv"
    
    for year in range (2004, 2014):

        year    = str(year)
        yy      = year[2:]

        for month in CALENDAR:

            res = get_csv(url_template.format(month, yy))

            if res.status_code != 200:

                continue

            else:

                fn = input_path + VX_FN_FMT.format(
                                    date  = date,
                                    month = month,
                                    year  = year
                                )
                
                with open(fn, "w") as fd:

                    fd.write(res.text)

    # "historical" (may 2013 - present)
    
    # settlement csv (metadata)

    settlement_url = "https://www.cboe.com/us/futures/market_statistics/final_settlement_prices/csv" 

    res = get_csv(settlement_url)

    if res.status_code != 200:

        print(LOG_FMT.format("cboe_extract", "error", "", f"get_history : settlements not retrieved : {res.text}", 1))

        return 1

    with open(f"{input_path}{date}_cfe_vx_final_settlements.csv", "w", encoding = "utf-8") as fd:

        fd.write(res.text)

    # "historical" csv (same format as "archive")

    url_template = "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{0}.csv"

    settlements = reader(res.text.splitlines())

    next(settlements)
    
    for record in settlements:

        # 0     symbol      'VX'
        # 1     symMY       'VX/K3'
        # 2     type        'Monthly'
        # 3     date        '2013-05-22'
        # 4     settlement  '13.1700'

        if record[0] != "VX" or record[2] != "Monthly":
            
            continue
        
        res = get_csv(url_template.format(record[3]))

        if res.status_code != 200:

            continue

        month   = record[1][3]
        year    = record[3][:4]
        fn      = input_path + VX_FN_FMT.format(
                                    date  = date,
                                    month = month,
                                    year  = year
                                )
        
        with open(fn, "w", encoding = "utf-8") as fd:
            
            fd.write(res.text)

    # remaining contracts: those currently listed but not yet expired
    # the "settlements" csv above does not give us expirations for current contracts
    # so for these we go to the daily settlements

    get_latest(date)

    return 0


def get_latest(date: str):

    input_path      = config["input_path"]
    url_template    = "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{0}.csv"

    # metadata

    res = get_current_settlements(date)

    if not res:

        msg = "get_history : error retrieving latest settlements (try running get_history() with a weekday input)"
        print(LOG_FMT.format("cboe_extract", "error", "", msg, 1))

        return 1
    
    else:

        with open(f"{input_path}{date}_cfe_latest_settlements.csv", "w", encoding = "utf-8") as fd:

            fd.write(res.text)

    # up-to-date data for listed contracts

    records = reader(res.text.splitlines())
    next(records)

    for record in records:

        # 0     symbol              'VX'
        # 1     symMY               'VX/G2', 'VX06/G2'
        # 2     expiration date     '2022-06-16',
        # 3     settlement          '26.8734'

        if record[0] != "VX" or len(record[1]) > 5:

            continue

        else:

            start = time()

            res = get_csv(url_template.format(record[2]))

            if res.status_code != 200:

                continue

            month   = record[1][3]
            year    = record[2][:4]
            fn      = input_path + VX_FN_FMT.format(
                                        date  = date,
                                        month = month,
                                        year  = year
                                    )
            
            with open(fn, "w", encoding = "utf-8") as fd:
                
                fd.write(res.text)

    return 0


def get_current_settlements(date: str):

    if not match("\d{4}-\d{2}-\d{2}", date):

        msg = "get_day : date format should be 'yyyy-mm-dd'"
        print(LOG_FMT.format("cboe_extract", "error", "", msg, 1))

        return

    start_all = time()

    res = get_csv(f"https://www.cboe.com/us/futures/market_statistics/settlement/csv?dt={date}")

    if res.status_code != 200:

        print(LOG_FMT.format("cboe_extract", "error", f"{time() - start_all: 0.1f}", f"get_day : {res.text}", res.status_code))

        return None

    else:

        return res


def get_files(cmd: str):

    start_all = time()

    parts = cmd.split()
    cmd   = parts[0]

    today = datetime.today()
    today = datetime.strftime(today, DATE_FMT)
    date  = today

    # by default today's date is used to get the latest settlements
    # which are needed for both "history" and "latest"

    # the user can override with a different date if, e.g., the script
    # is run on a weekend

    if len(parts) == 2:

        date = parts[1]

    print(LOG_FMT.format("cboe_extract", "start", "", f"get_files {cmd}", 0))

    if cmd == "history":

        err = get_history(date)

    elif cmd == "latest":

        err = get_latest(date)
    
    else:
        
        err = 1

    print(LOG_FMT.format("cboe_extract", "finish", f"{time() - start_all: 0.1f}", f"get_files {cmd}", err))


if __name__ == "__main__":

    get_files(argv[1])