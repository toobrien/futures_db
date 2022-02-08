from csv        import reader
from datetime   import datetime
from json       import loads
from sqlite3    import connect, Connection, OperationalError
from sys        import argv
from time       import time
from typing     import List


def update_table(
    cur:                Connection,
    dates:              List[str],
    processed_path:     str,
    table_name:         str,
    table_statement:    str,
    record_statement:   str,
    LOG_FMT
):
    cur.execute(table_statement)

    start = time()

    print(LOG_FMT.format("load", "start", "", f"update_table {table_name}", 0))

    for date in dates:
        
        with open(f"{processed_path}{date}_{table_name}.csv", "r") as fd:
        
            records = reader(fd)

            try:

                cur.executemany(record_statement, records)

            except OperationalError as e:

                print(f"ERROR\t{table_name}\t\t{e}")

    print(LOG_FMT.format("load", "finish", f"{time() - start: 0.1f}", f"update_table {table_name}", 0))


def load_processed(dates):

    config          = loads(open("./config.json", "r").read())
    LOG_FMT         = config["log_fmt"]
    db_path         = config["database_path"]
    processed_path  = config["processed_path"]
    
    start_all = time()

    print(LOG_FMT.format("load", "start", "", f"load_processed {', '.join(dates)}", 0))

    con = connect(db_path)
    cur = con.cursor()

    # ohlc

    table_statement = '''
        CREATE TABLE IF NOT EXISTS ohlc (
            contract_id     TEXT, 
            exchange        TEXT,
            name            TEXT,
            month           TEXT, 
            year            TEXT,
            date            TEXT,
            open            REAL,
            high            REAL,
            low             REAL,
            settle          REAL,
            volume          INTEGER,
            open_interest   INTEGER,
            PRIMARY KEY(contract_id, date)
        );
    '''

    record_statement = f'''
        INSERT OR REPLACE INTO ohlc (
            contract_id, exchange, name, month, year, date,
            open, high, low, settle, volume, open_interest
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''

    update_table(
        cur,
        dates,
        processed_path,
        "ohlc",
        table_statement,
        record_statement,
        LOG_FMT
    )

    # metadata

    table_statement = '''
        CREATE TABLE IF NOT EXISTS metadata (
            contract_id TEXT PRIMARY KEY,
            from_date   TEXT,
            to_date     TEXT
        );
    '''

    record_statement = f'''
        INSERT INTO metadata (
            contract_id,
            from_date, 
            to_date
        )
        VALUES (?, ?, ?)
        ON CONFLICT(contract_id) DO UPDATE SET
            from_date = CASE WHEN excluded.from_date < from_date 
                THEN 
                    excluded.from_date 
                ELSE
                    from_date
                END,
            to_date = CASE WHEN excluded.to_date > to_date
                THEN
                    excluded.to_date
                ELSE
                    to_date
                END
        ;
    '''

    update_table(
        cur,
        dates,
        processed_path,
        "metadata",
        table_statement,
        record_statement,
        LOG_FMT
    )

    # spot 

    table_statement = '''
        CREATE TABLE IF NOT EXISTS spot (
            symbol      TEXT,
            date        TEXT,
            price       REAL,
            PRIMARY KEY(symbol, date)
        );
    '''

    record_statement = f'''
        INSERT INTO spot (symbol, date, price)
        VALUES (?, ?, ?)
        ;
    '''

    update_table(
        cur,
        dates,
        processed_path,
        "spot",
        table_statement,
        record_statement,
        LOG_FMT
    )

    con.commit()
    con.close()

    print(LOG_FMT.format("load", "finish", f"{time() - start_all: 0.1f}", f"load_processed {', '.join(dates)}", 0))


if __name__ == "__main__":

    if len(argv) < 2:

        today = datetime.strftime(
                    datetime.today(),
                    "%Y-%m-%d",
                )

        load_processed([ today ])

    else:

        load_processed([ argv[1] ])