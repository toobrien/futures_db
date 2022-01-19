from io import BytesIO
from json import loads
from requests import get
from sys import argv
from time import time
from zipfile import ZipFile


# reference:
# https://data.nasdaq.com/data/SRF-Reference-Futures/usage/export


if __name__ == "__main__":

    config          = loads(open("./config.json", "r").read())
    input_path      = config["input_path"]
    nasdaq_api_key  = config["srf"]["nasdaq_api_key"]
    ohlc_url        = f"{config['srf']['ohlc_url']}?api_key={nasdaq_api_key}"
    metadata_url    = f"{config['srf']['metadata_url']}?api_key={nasdaq_api_key}"
    
    # cmd == "all" or "partial"
    # all for whole srf db; partial for today's rows
    # metadata is downloaded either way

    cmd = argv[1]

    if cmd == "partial":
        
        ohlc_url += f"&download_type=partial"

    print(f"START\t{cmd}")

    start_all = time()

    for url in [ 
        ohlc_url,
        metadata_url 
    ]:

        stem = url.split("?")[0]

        print(f"GET\t{stem}\tSTART")

        start = time()

        data = get(url, stream = True)
        bytes = BytesIO()

        for chunk in data.iter_content(chunk_size=1024):

            bytes.write(chunk)

        ZipFile(bytes).extractall(input_path)

        print(f"GET\t{stem}\tFINISH\t{time() - start: 0.3f}")

    print(f"FINISH\t{cmd}\t{time() - start_all: 0.3f}")