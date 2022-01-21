from csv        import reader
from datetime   import datetime
from json       import loads
from sqlite3    import connect, Connection, OperationalError
from time       import time
from typing     import List


def load_ohlc(
    cur:            Connection,
    dates:          List[str],
    processed_path: str
):

    cur.execute(
            '''
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
                open_interest   INTEGER
                PRIMARY KEY(conract_id, date)
            );
            '''
        )

    statement = f'''
    INSERT INTO ohlc (
        contract_id, exchange, name, month, year, date,
        open, high, low, settle, volume, open_interest
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''

    start = time()

    print("START\tohlc")

    for date in dates:
        
        with open(f"{processed_path}{date}_ohlc.csv", "r") as fd:
        
            records = reader(fd)

            try:

                cur.executemany(statement, records)

            except OperationalError as e:

                print(f"ERROR\tohlc\t\t{e}")

    print(f"FINISH\tohlc\t\t{time() - start: .3f}")


def load_metadata(
    cur:            str,
    dates:          List[str],
    processed_path: str
):

    cur.execute(
        '''
            CREATE TABLE IF NOT EXISTS metadata (
                contract_id TEXT PRIMARY KEY,
                from_date   TEXT,
                to_date     TEXT
            );
        '''
    )

    statement = f'''
        INSERT INTO metadata (contract_id, from_date, to_date)
        VALUES (?, ?, ?);
    '''

    start = time()

    print("START\tmetadata")

    for date in dates:
        
        with open(f"{processed_path}{date}_metadata.csv", "r") as fd:
        
            records = reader(fd)

            try:

                cur.executemany(statement, records)

            except OperationalError as e:

                print(f"ERROR\tmetadata\t\t{e}")

    print(f"FINISH\tmetadata\t\t{time() - start: .3f}")


def load_processed(dates):

    config          = loads(open("./config", "r").read())
    db_path         = config["database_path"]
    processed_path  = config["processed_path"]
    
    start_all = time()

    print("START\tload_processed")

    con = connect(db_path)
    cur = con.cursor()

    load_ohlc(cur, dates, processed_path)
    load_metadata(cur, processed_path)

    con.commit()
    con.close()

    print(f"FINISHED\tload_processed\t{time() - start_all:0.2f}")


if __name__ == "__main__":

    today = datetime.strftime(
                "%Y_%m_%d",
                datetime.today()
            )

    load_processed([ today ])