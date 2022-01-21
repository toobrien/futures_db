from    argparse import ArgumentParser
import  cme_extract
import  cme_transform
from    datetime import datetime
from    load import load_processed
import  srf_extract
import  srf_transform

if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument(
        "sources",
        nargs = "*",
        choices = [
            "cboe_all", 
            "cboe_latest",
            "cme_all",
            "cme_latest",
            "srf_all",
            "srf_latest",
            "matrix"
        ]
    )

    parser.add_argument(
        "--dates",
        nargs = "*"
    )
    
    args = parser.parse_args()

    load    = False
    dates   = parser.dates
    sources = parser.sources

    for source in sources:

        if source == "cboe_all":

            load = True

        elif source == "cboe_latest":

            load = True

        elif source == "cme_all":

            load = True

        elif source == "cme_latest":

            cme_extract.get_files([None, "all"])
            cme_transform.write_csv()
            load = True

        elif source == "srf_all":

            srf_extract.get_files("all")
            srf_transform.write_csv()
            load = True

        elif source == "srf_latest":

            srf_extract.get_files("partial")
            srf_transform.write_csv()
            load = True

        elif source == "matrix":

            pass

    if load:

        if not dates:

            # default: load today's records

            dates.append(
                datetime.strftime(
                    datetime.today(),
                    "%Y_%m_%d"
                )
            )

        load_processed(dates)
