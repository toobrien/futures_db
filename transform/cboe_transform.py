from csv        import writer
from datetime   import datetime
from json       import loads
from os         import walk
from re         import match
from sys        import argv
from time       import time


config      = loads(open("./config.json", "r").read())
input_path  = config["input_path"]
output_path = config["processed_path"]
LOG_FMT     = config["log_fmt"]

METADATA = {}


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

        rows = [ 
            row.split(",")
            for row in fd.read().splitlines() 
        ][1:]

        # cleaning: remove empty/incomplete rows

        rows = [
            row for row in rows
            if len(row) == 11
        ]

        # cleaning: the file may have a disclaimer on the first line;
        # header row still needs a skip

        if not match("(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})", rows[0][0]):

            rows = rows[1:]


        # cleaning: some files have mm/dd/yyyy, switch to yyyy-mm-dd

        for row in rows:

            if "/" in row[0]:

                dt = row[0].split("/")
                row[0] = f"{dt[2]}-{dt[0].zfill(2)}-{dt[1].zfill(2)}"

        # cleaning: records before 2007-03-26 are 10x size

        for row in rows:

            if row[0] < "2007-03-26":

                # reduce prices 

                row[2] = float(row[2]) / 10 if row[2] != '' else "NULL"
                row[3] = float(row[3]) / 10 if row[3] != '' else "NULL"
                row[4] = float(row[4]) / 10 if row[4] != '' else "NULL"
                row[5] = float(row[5]) / 10 if row[5] != '' else "NULL"
                row[6] = float(row[6]) / 10 if row[6] != '' else "NULL"
                row[7] = float(row[7]) / 10 if row[7] != '' else "NULL"
                
                # increase vol and oi ?

                row[8]  = float(row[8]) * 10 if row[8] != '' else "NULL"
                row[10] = float(row[10]) * 10 if row[10] != '' else "NULL"

        # one file per contract, find start date
        
        first_row = rows[0]
        last_row  = rows[-1]
        
        exchange    = "CFE"
        name        = "VX"
        month       = first_row[1][0]
        year        = f"20{first_row[1][-3:-1]}"
        id          = f"{exchange}_{name}{month}{year}"
        
        # first trade date

        METADATA[id] = [ first_row[0], last_row[0] ]

        for row in rows:

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

    # for expired contracts, this method is mostly redundant. their "to" and "from" dates 
    # should already be set in the METADAT dictionary. however, for active contracts, the
    # METADATA's "to" date will be the last trade date. this method replaces those trade
    # dates with the true expiration date.

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

                if contract_id not in METADATA:

                    METADATA[contract_id] = [ "NULL", row[2] ]
                
                else:

                    METADATA[contract_id][1] = row[2]

    with open(f"{output_path}{date}_metadata.csv", "a") as fd:

        writer(fd).writerows([
            [ contract_id, dates[0], dates[1] ]
            for contract_id, dates in METADATA.items()
        ])


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

    if len(argv) < 2:
    
        write_csv(datetime.today().strftime("%Y-%m-%d"))
    
    else:

        write_csv(argv[1])