from datetime   import datetime
from ftplib     import FTP
from json       import loads
from sys        import argv
from time       import time


def get_files(argv):

    config      = loads(open("./config.json", "r").read())
    DATE_FMT    = config["date_fmt"]
    LOG_FMT     = config["log_fmt"]
    input_path  = config["input_path"]

    today       = datetime.strftime(datetime.today(), DATE_FMT)

    ftp = FTP(config["cme"]["cme_ftp"])
    ftp.login()
    ftp.cwd("settle")

    cmd = argv[1]

    print(LOG_FMT.format("cme_extract", "start", "", cmd, 0))

    if cmd == "list":

        ftp.retrlines("LIST")

    elif cmd == "get":

        write(argv[2], today, ftp, input_path, LOG_FMT)

    elif cmd == "all":
    
        files = config["cme"]["cme_files"]

        start_all = time()

        for fn in files:
        
            write(fn, today, ftp, input_path, LOG_FMT)

        print(LOG_FMT.format("cme_extract", "finish", f"{time() - start_all: 0.1f}", argv[1], 0))

    ftp.quit()


def write(fn, today, ftp, input_path, LOG_FMT):

        with open(f"{input_path}{today}_{fn}", "wb") as fd:

            print(LOG_FMT.format("cme_extract", "start", "", f"RETR {fn}", 0))
            
            start = time()

            ftp.retrbinary(f"RETR {fn}", fd.write)
            
            print(LOG_FMT.format("cme_extract", "finish", f"{time() - start: 0.1f}", f"RETR {fn}", 0))


if __name__ == "__main__":

    get_files(argv)