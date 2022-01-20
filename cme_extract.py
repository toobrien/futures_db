from ftplib import FTP
from json import loads
from sys import argv
from time import time


def get_files(argv):

    config = loads(open("./config.json", "r").read())
    input_path = config["input_path"]

    ftp = FTP(config["cme"]["cme_ftp"])
    ftp.login()
    ftp.cwd("settle")

    cmd = argv[1]

    if cmd == "list":

        ftp.retrlines("LIST")

    elif cmd == "get":

        write(argv[2], ftp, input_path)

    elif cmd == "all":
    
        files = config["cme"]["cme_files"]

        start_all = time()

        for fn in files:
        
            write(fn, ftp, input_path)

        print(f"finished\t\t\t{time() - start_all: 0.2f}")

    ftp.quit()


def write(fn, ftp, input_path):

        with open(f"{input_path}{fn}", "wb") as fd:

            print(f"downloading\t{fn}")
            
            start = time()

            ftp.retrbinary(f"RETR {fn}", fd.write)
            
            print(f"complete\t{fn}\t{time() - start: 0.4f}")


if __name__ == "__main__":

    get_files(argv)
