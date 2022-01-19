from csv import reader, writer
from json import loads


CALENDAR = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z"
}


def write_csv():

    config              = loads(open("./config.json", "r").read())
    input_path          = config["input_path"]
    processed_path      = config["processed_path"]
    contract_settings   = reader(open(config["contract_settings"], "r"))
    
    x = next(contract_settings) # skip headers

    # globex symbol : exchange

    enabled_contracts = {
        line[2] : line[3]
        for line in contract_settings
        if bool(line[4])
    }

    ohlc = [
        # header row
        [
            "id",
            "exchange",
            "name",
            "month",
            "year",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",           # note: previous day
            "open_interest"     # note: previous day
        ]
    ]

    metadata = [
        # header row
        [
            "id",
            "from_date",
            "to_date"
        ]
    ]

    metadata_seen = set()

    for fn in config["cme"]["cme_files"]:

        with open(f"{input_path}{fn}", "r") as fd:

            # unprocessed cme record

            # 0  BizDt          current date        YYYY-MM-DD
            # 1  Sym            globex symbol       ZN
            # 2  ID             clearing symbol     21
            # 3  StrkPx         strike price        float
            # 4  SecTyp         security type       FUT, OOF, COMBO, OOC
            # 5  MMY            exp y+m             YYYYMM
            # 6  MatDt          ?                   YYYY-MM-DD          
            # 7  PutCall        put or call         1 or 0
            # 8  Exch           exchange            NYMEX
            # 9  Desc           always empty        "" 
            # 10 LastTrdDt      last trade date     YYYY-MM-DD
            # 11 BidPrice       ?                   float
            # 12 OpeningPrice   open                float
            # 13 SettlePrice    settle              float
            # 14 SettleDelta    delta settle?       float
            # 15 HighLimit      high?               float
            # 16 LowLimit       low?                float
            # 17 DHighPrice     ?                   float
            # 18 DLowPrice      ?                   float
            # 19 HighBid        ?                   float    
            # 20 LowBid         ?                   float
            # 21 PrevDayVol     volume              int
            # 22 PrevDayOI      OI                  int
            # 23 FixingPrice    ?                   float
            # 24 UndlyExch      underlying exchange NYMEX
            # 25 UndlyID        underlying clearing 21
            # 26 UndlySecTyp    underlying sectype  FUT, OOF, COMBO, OOC
            # 27 UndlyMMY       exp y+m             YYYYMM
            # 28 BankBusDay     ?                   YYYY-MM-DD

            data = reader(fd)

            # skip header

            next(data)
            
            # build ohlc record set

            for row in data:

                if row[4] != "FUT" or row[1] not in enabled_contracts:
                    
                    continue

                symbol   = row[1]
                exchange = enabled_contracts[symbol]
                delivery = row[5]
                year     = delivery[0:4]
                month    = None

                # format varies: yyyymm vs yyyymmdd

                if len(delivery) == 6:

                    month = CALENDAR[int(delivery[4:])]

                else:

                    month = CALENDAR[int(delivery[4:6])]

                # propagage settle for contracts that didn't trade

                id          =   f"{exchange}_{symbol}{month}{year}"
                date        =   row[0]
                settle      =   row[13] if row[13] != "" else "NULL"
                open_       =   row[12] if row[12] != "" else settle
                high        =   row[15] if row[15] != "" else settle
                low         =   row[16] if row[16] != "" else settle
                vol         =   row[21] if row[21] != "" else "NULL"
                oi          =   row[22] if row[22] != "" else "NULL"

                from_date = row[0]  # see note below
                to_date =   row[10]

                ohlc.append(
                    [
                        id,
                        exchange,
                        month,
                        year,
                        date,
                        open_,
                        high,
                        low,
                        settle,
                        vol,
                        oi
                    ]
                )

                if id not in metadata_seen:

                    metadata_seen.add(id)

                    # note: from_date is today, which means the database insert
                    # should discard this record if there already a record for
                    # the same "id" with an earlier from_date

                    metadata.append( 
                        [
                            id,
                            from_date,
                            to_date
                        ]
                    )

    # write ohlc and metadata records to file
    # all records are for the same day

    yyyy_mm_dd = ohlc[1][1]

    for record_type, records in [
        ( "ohlc_cme", ohlc ),
        ( "metadata_cme", metadata )
    ]:

        with open(f"{processed_path}{yyyy_mm_dd}_{record_type}.csv", "w") as fd:

            w = writer(fd, delimiter = ",")
            w.writerows(records)


if __name__ == "__main__":

    write_csv()