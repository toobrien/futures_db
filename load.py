from csv        import reader
from datetime   import datetime
from json       import loads
from sqlite3    import connect, Connection, OperationalError
from time       import time
from typing     import List


def update_table(
    cur:                Connection,
    dates:              List[str],
    processed_path:     str,
    table_name:         str,
    table_statement:    str,
    record_statement:   str
):
    cur.execute(table_statement)

    start = time()

    print(f"START\t{table_name}")

    for date in dates:
        
        with open(f"{processed_path}{date}_{table_name}.csv", "r") as fd:
        
            records = reader(fd)

            try:

                cur.executemany(record_statement, records)

            except OperationalError as e:

                print(f"ERROR\t{table_name}\t\t{e}")

    print(f"FINISH\t{table_name}\t\t{time() - start: .3f}")


def load_processed(dates):

    config          = loads(open("./config.json", "r").read())
    db_path         = config["database_path"]
    processed_path  = config["processed_path"]
    
    start_all = time()

    print("START\tload_processed")

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
        record_statement
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
            from_date = excluded.from_date
            WHERE excluded.from_date < from_date
        ;
    '''

    update_table(
        cur,
        dates,
        processed_path,
        "metadata",
        table_statement,
        record_statement
    )

    # metadata again, this time with to_date... annoying

    record_statement = f'''
        INSERT INTO metadata (
            contract_id,
            from_date, 
            to_date
        )
        VALUES (?, ?, ?)
        ON CONFLICT(contract_id) DO UPDATE SET
            from_date = excluded.from_date
            WHERE excluded.to_date > to_date
        ;
    '''

    update_table(
        cur,
        dates,
        processed_path,
        "metadata",
        table_statement,
        record_statement
    )
    

    con.commit()
    con.close()

    print(f"FINISHED\tload_processed\t{time() - start_all:0.2f}")


if __name__ == "__main__":

    today = datetime.strftime(
                "%Y_%m_%d",
                datetime.today()
            )

    load_processed([ today ])