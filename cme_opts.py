from csv            import reader
from json           import loads
from sqlite3        import Connection, OperationalError
from time           import time

CONFIG              = loads(open("./config.json").read())
CONTRACT_SETTINGS   = loads(open(CONFIG["contract_settings"]).read())
ENABLED             = {
    definition["opts"]: {
        "globex":   definition["globex"], 
        "exchange": definition["exchange"]
    }
    for _, definition in CONTRACT_SETTINGS.items()
    if "opts" in definition
}
MONTHS  = {
    1:  "F",
    2:  "G",
    3:  "H",
    4:  "J",
    5:  "K",
    6:  "M",
    7:  "N",
    8:  "Q",
    9:  "U",
    10:  "V",
    11: "X",
    12: "Z"
}
FILES       = CONFIG["cme"]["cme_files"]
INPUT_PATH  = CONFIG["input_path"]
LOG_FMT     = CONFIG["log_fmt"]


def load_opts(date: str, cur: Connection):

    start = time()

    print(LOG_FMT.format("load_opts", "start", "", f"load_opts", 0))

    processed = []

    for fn in FILES:

        try:
    
            with open(f"{INPUT_PATH}{date}_{fn}", encoding = "utf-8") as fd:

                rows = reader(fd)

                for row in rows:

                    ul_symbol   = row[25]
                    type        = row[4]

                    if ul_symbol in ENABLED and type == "OOF":

                        ul_exchange         = ENABLED[ul_symbol]["exchange"]
                        ul_symbol           = ENABLED[ul_symbol]["globex"]

                        # 0     BizDt         
                        # 1     Sym           globex
                        # 2     ID            clearing
                        # 3     StrkPx       
                        # 4     SecTyp        OOF or FUT
                        # 5     MMYY          YYYYMM       
                        # 6     MatDt         
                        # 7     PutCall       1 = call 0 = put
                        # 8     Exch          
                        # 9     Desc          ""
                        # 10    LastTrdDt     
                        # 11    BidPrice      ""
                        # 12    OpeningPrice  ""
                        # 13    SettlePrice  
                        # 14    SettleDelta
                        # 15    HighLimit     
                        # 16    LowLimit     
                        # 17    DHighPrice    ""
                        # 18    DLowPrice     ""
                        # 19    HighBid
                        # 20    LowBid
                        # 21    PrevDayVol    
                        # 22    PrevDayOI     
                        # 23    FixingPrice   ""
                        # 24    UndlyExch
                        # 25    UndlyID       globex
                        # 26    UndlySecTyp   FUT or ?
                        # 27    UndlyMMY      YYYYMM
                        # 28    BankBusDay    ?

                        date_               = row[0]
                        name                = row[1]
                        strike              = row[3]
                        expiry              = row[6]
                        call                = row[7]
                        last_traded         = row[10]
                        settle              = row[13]
                        settle_delta        = row[14]
                        high_limit          = row[15]
                        low_limit           = row[16]
                        high_bid            = row[19]
                        low_bid             = row[20]
                        previous_volume     = row[21]
                        previous_interest   = row[22]
                        underlying_month    = MONTHS[int(row[27][-2:])]
                        underlying_year     = row[27][0:4]
                        underlying_id       = f"{ul_exchange}_{ul_symbol}{underlying_month}{underlying_year}"

                        processed.append(
                            [
                                date_,
                                name,
                                strike,
                                expiry,
                                call,
                                last_traded,
                                settle,
                                settle_delta,
                                high_limit,
                                low_limit,
                                high_bid,
                                low_bid,
                                previous_volume,
                                previous_interest,
                                ul_symbol,
                                ul_exchange,
                                underlying_id
                            ]
                        )

        except FileNotFoundError:

            print(LOG_FMT.format("load_opts", "error", f"{time() - start: 0.1f}", f"skipping file {fn}", 1))
        
    table_statement = '''
        CREATE TABLE IF NOT EXISTS cme_opts (
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
        );
    '''

    try:

        cur.execute(table_statement)
    
    except OperationalError as e:

        print(LOG_FMT.format("load_opts", "error", f"{time() - start: 0.1f}", e, 1))

    if processed:

        record_statement = f'''
            INSERT OR REPLACE INTO cme_opts (
                date, name, strike, expiry, call, last_traded,
                settle, settle_delta, high_limit, low_limit, 
                high_bid, low_bid, previous_volume, previous_interest,
                underlying_symbol, underlying_exchange, underlying_id
            )
            VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?,
                ?, ?
            );
        '''

        try:

            cur.executemany(record_statement, processed)

        except OperationalError as e:

            print(LOG_FMT.format("load_opts", "error", f"{time() - start: 0.1f}", e, 1))

    print(LOG_FMT.format("load_opts", "finish", f"{time() - start: 0.1f}", f"load_opts", 0))