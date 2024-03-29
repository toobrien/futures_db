from csv        import writer
from datetime   import datetime
from json       import JSONDecodeError, loads
from sys        import argv
from time       import time

config      = loads(open("./config.json", "r").read())
input_path  = config["input_path"]
output_path = config["processed_path"]
spot        = config["eia"]["spot"]
LOG_FMT     = config["log_fmt"]
DATE_FMT    = config["date_fmt"]


def write_csv(date: str):

    for symbol, code in spot.items():

        fn = f"{input_path}{date}_eia_{code}.json"

        with open(fn) as fd:

            start       = time()
            process     = True
            rows        = []
            processed   = []

            try:
                
                res = loads(fd.read())
                rows = res["response"]["data"]

            except JSONDecodeError as e:

                print(LOG_FMT.format("eia_transform", "error", f"json decode error: {fn}", code, 1))

                process = False

            # 0     date    20220201
            # 1     price   5.45

            if process:

                for row in rows:

                    date_   = row["period"]
                    price   = float(row["value"]) if row["value"] else "NULL"
                
                    processed.append([ symbol, date_, price ])

            with open(f"{output_path}{date}_spot.csv", "a", newline = "") as fd:

                w = writer(fd)
                w.writerows(processed)

                print(LOG_FMT.format("eia_transform", "finish", f"{time() - start: 0.1f}", code, 0))


if __name__ == "__main__":

    start_all = time()

    cmd = argv[1]

    print(LOG_FMT.format("eia_transform", "start", "", cmd, 0))

    if len(argv) < 2:
    
        write_csv(datetime.today().strftime(DATE_FMT))
    
    else:

        write_csv(cmd)

    print(LOG_FMT.format("eia_transform", "finish", f"{time() - start_all: 0.1f}", cmd, 0))