from datetime   import datetime
from json       import loads
from requests   import get
from sys        import argv
from time       import time

config          = loads(open("./config.json").read())
input_path      = config["input_path"] 
DATE_FMT        = config["date_fmt"]
LOG_FMT         = config["log_fmt"]
api_key         = config["eia"]["api_key"]
url             = config["eia"]["url"]
spot            = config["eia"]["spot"]

def get_files(cmd):

    today           = datetime.today() 
    today_eia_fmt   = datetime.strftime(today, "%Y%m%d")
    today           = datetime.strftime(today, DATE_FMT)

    for symbol, code in spot.items():

        start = time()

        url_ = url.format(api_key, code)
        
        if cmd == "latest":

            url_ += "&start={}".format(today_eia_fmt)

        res = get(url_)

        if res.status_code == 200:

            print(LOG_FMT.format("eia_extract", "finish", f"{time() - start: 0.1f}", url_, res.status_code))

            with open(f"{input_path}{today}_eia_{code}.json", "w", encoding = "utf-8") as fd:

                fd.write(res.text)
        
        else:

            print(LOG_FMT.format("eia_extract", "error", f"{time() - start: 0.1f}", url_, res.status_code))
    

if __name__ == "__main__":

    start_all = time()

    cmd = argv[1]

    print(LOG_FMT.format("eia_extract", "start", "", cmd, 0))

    get_files(cmd)

    print(LOG_FMT.format("eia_extract", "finish", f"{time() - start_all: 0.1f}", cmd, 0))