from csv            import reader, writer
from datetime       import datetime
from json           import loads
from os             import listdir
from re             import match
from time           import time
from typing         import Dict, Tuple


def get_id(
    id:                 str,
    enabled_contracts:  Dict[str , Dict]
) -> Tuple[str, str, str, str, str, bool]:

    id_parts = id.split("_") 
    
    name                = id_parts[1][:-5]
    exchange            = id_parts[0]
    exchange_contracts  = enabled_contracts[exchange]
    contract_details    = exchange_contracts[name]
    
    exchange    = contract_details["exchange"]
    name        = contract_details["globex"] if "globex" in contract_details else contract_details["srf"]
    month       = id_parts[1][-5:-4]
    year        = id_parts[1][-4:]
    new_id      = f"{exchange}_{name}{month}{year}"
    enabled     = contract_details["enabled"]

    return ( exchange, name, month, year, new_id, enabled )


def process_ohlc(
    input_path:         str,
    output_path:        str,
    enabled_contracts:  Dict[str, Dict]
):

    # unprocessed ohlc

    # 0 contract_id         str     CME_ADF2018
    # 1 date                str     2018-01-12
    # 2 open                float   0.7888
    # 3 high                float   0.7896
    # 4 low                 float   0.7856
    # 5 settle              float   0.7879
    # 6 volume              float   436.0
    # 7 prev. open interest float   306.0

    records = [
        [
            "contract_id",
            "exchange",
            "name",
            "month",
            "year",
            "date",
            "open",
            "high",
            "low",
            "settle",
            "volume",
            "open_interest"
        ]
    ]

    with open(input_path, "r") as fd:

        data = reader(fd)

        for record in data:

            (
                exchange,
                name,
                month,
                year,
                new_id,
                enabled
            ) = get_id(record[0], enabled_contracts)

            if enabled:

                for i in range(2, len(record)):

                    if record[i] == "" or record[i] == None:
                    
                        record[i] = "NULL"

                processed = [ 
                    new_id,     # contract_id 
                    exchange,   # exchange
                    name,       # name
                    month,      # month
                    year,       # year
                    record[1],  # date
                    record[2],  # open
                    record[3],  # high
                    record[4],  # low
                    record[5],  # settle
                    record[6],  # volume
                    record[7]   # open_interest (previous day)
                ]

                records.append(processed)

    with open(output_path, "a") as fd:

        w = writer(fd)
        w.writerows(records)


def process_metadata(
    input_path:         str,
    output_path:        str,
    enabled_contracts:  Dict[str, Dict]
):

    # unprocessed metadata

    # 0 code            CME_ADF2018
    # 1 name            CME Australian Dollar [...]
    # 2 description     <p><b>Contract Size:</b>100,000 [...]
    # 3 refreshed_at    2018-01-13 03:39:03
    # 4 from_date       2017-08-15
    # 5 to_date         2018-01-12

    with open(input_path, "r") as fd:

        data                = reader(fd)
        processed_records   = [
            [
                "contract_id",
                "from_date",
                "to_date"
            ]
        ]

        next(data)

        for record in data:
            
            _, _, _, _, new_id, enabled = get_id(record[0], enabled_contracts)

            if enabled:

                processed = [
                    new_id,     # contract_id
                    record[4],  # from_date
                    record[5]   # to_date
                ]

                processed_records.append(processed)

        with open(output_path, "a") as fd:

            w = writer(fd)
            w.writerows(processed_records)


def write_csv():

    start_all = time()

    config              = loads(open("./config.json", "r").read())
    input_path          = config["input_path"]
    output_path         = config["processed_path"]
    files               = listdir(input_path)
    contract_settings   = loads(open(config["contract_settings"], "r").read())

    # map enabled contracts back to SRF exchanges (i.e. "CME" or "ICE")
    # to help disambiguate "O" (Oats) from "O" (heating oil) in get_id()

    enabled_contracts = {
        "CME": {},
        "ICE": {}
    }

    for _, settings in contract_settings.items():

        ICE_EXCHANGES = [ "ICE", "NYBOT" ]

        if "srf" in settings:

            symbol = settings["srf"]

            if settings["exchange"] in ICE_EXCHANGES:

                enabled_contracts["ICE"][symbol] = settings

            else:

                enabled_contracts["CME"][symbol] = settings


    metadata_input_path = f"{input_path}SRF_metadata.csv"

    yyyy_mm_dd          = datetime.strftime(
                            datetime.today(),
                            "%Y_%m_%d"
                        )

    # write processed ohlc

    for file in files:

        m = match("SRF_\d+.*\.csv", file)
        
        if m:

            print(f"START\tprocessing {m[0]}")
            
            start = time()

            process_ohlc(
                input_path          = f"{input_path}{m[0]}",
                output_path         = f"{output_path}{yyyy_mm_dd}_ohlc.csv",
                enabled_contracts   = enabled_contracts
            )

            print(f"FINISH\tprocessing {m[0]}\t{time() - start: 0.4f}")

    # write processed metadata

    print(f"START\tprocessing SRF_metadata.csv")

    start = time()

    process_metadata(
        input_path          = metadata_input_path,
        output_path         = f"{output_path}{yyyy_mm_dd}_metadata.csv",
        enabled_contracts   = enabled_contracts
    )

    print(f"FINISH\tprocessing SRF_metadata.csv\t{time() - start: 0.4f}")
    print(f"FINISH\tsrf transform\t\t\t\t{time() - start_all: 0.4f}")
        

if __name__=="__main__":

    write_csv()