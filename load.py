from json import loads
from sqlite3 import connect, OperationalError


if __name__ == "__main__":

    config = loads(open("./config", "r").read())

    # init db
    
    con = connect(f"./srf.db")
    cur = con.cursor()

    # create ohlc table

    cur.execute("DROP TABLE IF EXISTS ohlc")
    
    cur.execute(
        '''
        CREATE TABLE ohlc (
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
        );
        '''
    )

    # insert ohlc records
    statement = f'''
    INSERT INTO ohlc (
        contract_id, exchange, name, month, year, date,
        open, high, low, settle, volume, open_interest
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''

    try:

        cur.executemany(statement, rs)

    except OperationalError as e:

        print(f"ohlc error\t\t{e}")

        print("finished ohlc\t\t{:.2f}".format(time() - start))

    # create metadata table
    cur.execute("DROP TABLE IF EXISTS metadata")
    cur.execute(
        '''
            CREATE TABLE metadata
            (
            contract_id TEXT,
            from_date TEXT,
            to_date TEXT
            );
        '''
    )

    # get metadata
    data = reader(get_csv_file(metadata_url))

    print("downloaded metadata\t{:.2f}".format(time() - start))

    # prepare and clean metadata records
    corrections = {}

    rs = []

    for r in data:

        for i in range(len(r)):

            if r[i] == "" or r[i] == None:

                r[i] = "NULL"

            if r[0] in corrections:

                for k, v in corrections[r[0]].items():

                    r[k] = v 

            rs.append([ r[0], r[4], r[5] ])

    # insert metadata records
    statement = f'''
    INSERT INTO metadata (contract_id, from_date, to_date)
    VALUES (?, ?, ?);
    '''

    try:

        cur.executemany(statement, rs)

    except OperationalError as e:

        print(f"metadata error\t\t{e}")

    # close db
    con.commit()
    con.close()

    print("finished srf db\t{:.2f}".format(time() - start))