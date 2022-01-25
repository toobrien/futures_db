from requests   import get
from csv        import reader, writer
from datetime   import date, datetime
from json       import loads


def write_csv(yyyy_mm_dd: str):

    # extract

    config      = loads(open("./config.json", "r").read())
    out_path    = config["processed_path"]

    results = []

    url = f"https://www.cboe.com/us/futures/market_statistics/settlement/csv?dt={yyyy_mm_dd}"
    
    res = get(url)

    if res.status_code != 200:

        print(f"ERROR\tGET {url}\t{res.status_code}")
        print(res.text)

        return

    else:

        print(f"FINISH\tGET {url}\t{res.status_code}")

        results = res.text.splitlines()

    # transform

    ohlc        = []
    metadata    = []

    y           = yyyy_mm_dd[3]
    yyyy        = yyyy_mm_dd[0:4]
    next_yyyy   = str(int(yyyy) + 1)

    for row in reader(results):

        if "VX/" in row[1]:

            exchange        = "CFE"
            name            = "VX"
            month           = row[1][3]
            year            = yyyy if row[1][4] == y else next_yyyy
            date_           = yyyy_mm_dd
            id              = f"CFE_VX{month}{year}"
            open_           = "NULL"
            high            = "NULL"
            low             = "NULL"
            settle          = float(row[3])
            volume          = "NULL"
            open_interest   = "NULL"

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

            # database will discard from_date if newer than existing entry

            from_date = yyyy_mm_dd
            to_date   = row[2]

            metadata.append(
                [
                    id,
                    from_date,
                    to_date
                ]
            )
    
    with open(f"{out_path}{yyyy_mm_dd}_ohlc.csv", "a") as fd:

        writer(fd).writerows(ohlc)

    with open(f"{out_path}{yyyy_mm_dd}_metadata.csv", "a") as fd:

        writer(fd).writerows(metadata)


if __name__ == "__main__":

    write_csv(datetime.today().strftime("%Y_%m_%d"))