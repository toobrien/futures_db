from    argparse        import ArgumentParser
import  cme_extract
import  cme_transform
from    datetime        import datetime
from    json            import loads
from    load            import load_processed
from    os              import remove, walk
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
            "srf_latest"
        ]
    )

    parser.add_argument("--dates", "-d", nargs = "*")
    parser.add_argument(
        "--archive",
        "-a",
        dest    = "archive",
        action  = "store_true"
    )
    parser.add_argument(
        "--clean",
        "-c",
        dest    = "clean",
        action  = "store_true"
    )

    args = parser.parse_args()

    load    = False
    archive = args.archive
    clean   = args.clean
    dates   = args.dates
    sources = args.sources

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

    # insert records into database

    if load:

        if not dates:

            # default: load today's records

            dates = [
                datetime.strftime(
                    datetime.today(),
                    "%Y_%m_%d"
                )
            ]

        load_processed(dates)

    # delete raw input and processed records, if necessary

    if clean:

        with open("./config.json", "r") as fd:

            config = loads(fd.read())

            for path in [ 
                "input_path",
                "processed_path"
            ]:

                _, _, fns = walk(config[path])

                for fn in fns:
                    
                    remove(f"{path}{fn}")