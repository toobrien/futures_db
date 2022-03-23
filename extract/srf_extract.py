from datetime   import datetime
from io         import BytesIO
from json       import loads
from requests   import get
from sys        import argv
from time       import time
from zipfile    import ZipFile


# srf reference: https://data.nasdaq.com/data/SRF-Reference-Futures/usage/export


def get_files(cmd: str):

    config          = loads(open("./config.json", "r").read())
    LOG_FMT         = config["log_fmt"]
    DATE_FMT        = config["date_fmt"]
    input_path      = config["input_path"]
    nasdaq_api_key  = config["srf"]["nasdaq_api_key"]
    ohlc_url        = f"{config['srf']['ohlc_url']}?api_key={nasdaq_api_key}"
    metadata_url    = f"{config['srf']['metadata_url']}?api_key={nasdaq_api_key}"
    today           = datetime.strftime(datetime.today(), DATE_FMT)

    # cmd == "all" or "partial"
    # all for whole srf db; partial for today's rows
    # metadata is downloaded either way

    if cmd == "partial":
        
        ohlc_url += f"&download_type=partial"

    print(LOG_FMT.format("srf_extract", "start", "", cmd, 0))

    start_all = time()

    for option in [ 
        ( ohlc_url, "ohlc" ),
        ( metadata_url, "metadata" )
    ]:

        url  = option[0]
        kind = option[1]

        stem = url.split("?")[0]

        print(LOG_FMT.format("srf_extract", "start", "", f"GET {stem}", 0))

        start = time()

        data = get(url, stream = True)
        bytes = BytesIO()

        for chunk in data.iter_content(chunk_size=1024):

            bytes.write(chunk)

        with ZipFile(bytes) as zip:
            
            for fn in zip.namelist():

                bytes = zip.read(fn)
                
                with open(f"{input_path}{today}_srf_{kind}.csv", "wb", encoding = "utf-8") as fd:

                    fd.write(bytes)


        print(LOG_FMT.format("srf_extract", "finish", f"{time() - start: 0.1f}", f"GET {stem}", 200))

    print(LOG_FMT.format("srf_extract", "finish", f"{time() - start_all: 0.1f}", cmd, 0))


if __name__ == "__main__":

    cmd = argv[1]

    get_files(cmd)