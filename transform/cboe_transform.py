from csv        import writer
from datetime   import datetime
from json       import loads
from os         import walk
from re         import match
from time       import time


config      = loads(open("./config.json", "r").read())
input_path  = config["input_path"]
output_path = config["processed_path"]
LOG_FMT     = config["log_fmt"]

FROM_DATES = {}

def process_vx_ohlc_csv(date: str, fn: str):

    # 0 Trade Date      2021-01-20
    # 1 Futures         F (Jan 2021)
    # 2 Open            22.9000
    # 3 High            23.0000
    # 4 Low             22.2000
    # 5 Close           22.5000
    # 6 Settle          22.59
    # 7 Change          -0.635
    # 8 Total Volume    908
    # 9 EFP             0
    # 10 Open Interest  16066

    ohlc = []

    with open(f"{input_path}{fn}", "r") as fd:

        rows = [ row.split(",") for row in fd.read().splitlines() ][1:]
        
        # some files have mm/dd/yyyy, switch to yyyy-mm-dd

        for row in rows:

            if "/" in row[0]:

                dt = row[0].split("/")
                row[0] = f"{dt[2]}-{dt[0]}-{dt[1]}"

        # one file per contract, find start date
        
        first_row = rows[0]
        
        exchange    = "CFE"
        name        = "VX"
        month       = first_row[1][0]
        year        = f"20{first_row[1][-3:-1]}"
        id          = f"{exchange}_{name}{month}{year}"
        
        # first trade date

        FROM_DATES[id] = first_row[0]

        skip = 0

        for row in rows:

            if len(row) < 10:

                # some records are empty-ish

                skip += 1

                continue

            date_           = row[0]
            open_           = row[2]
            high            = row[3]
            low             = row[4]
            settle          = row[6]
            volume          = row[8]
            open_interest   = row[10]

            ohlc.append(
                [
                    id,
                    exchange,
                    name,
                    month,
                    year,
                    date_,
                    open_,
                    high,
                    low,
                    settle,
                    volume,
                    open_interest
                ]
            )
    
    with open(f"{output_path}{date}_ohlc.csv", "a") as fd:

        writer(fd).writerows(ohlc)


def process_vx_metadata_csv(date: str, fn: str):

    # there are two metadata files: cfe_vx_final_settlements.csv and cfe_latest_settlements.csv
    # the final settlements file has the expiration date for all expired contracts, but not listed contracts
    # the latest settlements file has the expiration date for active, listed contracts

    metadata = []

    with open(f"{input_path}{fn}", "r") as fd:

        rows = [ row.split(",") for row in fd.read().splitlines() ][1:]

        # 0 product         VX
        # 1 symbol          VX/F2
        # 2 expiration date 2021-01-19
        # 3 price           22.79

        # remove "duration type" (extra 3rd column) from final settlements file

        if "cfe_vx_final_settlements" in fn:

            rows = [
                [ row[0], row[1], row[3], row[4] ]
                for row in rows
            ]

        for row in rows:

            # filter for monthly VX

            if row[0] == "VX" and len(row[1]) == 5:
            
                month = row[1][3]
                year  = row[2][0:4]

                contract_id = f"CFE_VX{month}{year}"

                from_date = FROM_DATES[contract_id] if contract_id in FROM_DATES else "NULL"
                to_date   = row[2]

                metadata.append(
                    [
                        contract_id,
                        from_date,
                        to_date
                    ]
                )
    
    with open(f"{output_path}{date}_metadata.csv", "a") as fd:

        writer(fd).writerows(metadata)


def write_csv(date: str):

    start_all = time()

    print(LOG_FMT.format("cboe_transform", "start", "", "", 0))

    ohlc_pattern    = date + "_cfe_vx_.\d{4}.csv"
    ohlc_fns        = []
    metadata_fns    = []
    fns             = walk(input_path)
    bundles         = walk(input_path)

    for _, _, fns in bundles:
        
        ohlc_fns = [ 
            fn for fn in fns 
            if match(ohlc_pattern, fn) 
        ]

        metadata_fns = [
            fn for fn in fns
            if fn in [
                date + "_cfe_vx_final_settlements.csv",
                date + "_cfe_latest_settlements.csv"
            ]
        ]

        for fn in ohlc_fns:

            process_vx_ohlc_csv(date, fn)
        
        for fn in metadata_fns:

            process_vx_metadata_csv(date, fn)

    print(LOG_FMT.format("cboe_transform", "finish", f"{time() - start_all: 0.1f}", "", 0))


if __name__ == "__main__":

    write_csv(datetime.today().strftime("%Y-%m-%d"))