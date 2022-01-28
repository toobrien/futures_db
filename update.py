from    argparse        import ArgumentParser
import  cboe_et
import  cme_extract
import  cme_transform
from    datetime        import datetime
from    json            import loads
from    load            import load_processed
from    os              import remove, walk
import  srf_extract
import  srf_transform
from    time            import time
from    zipfile         import ZipFile


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument(
        "--sources",
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
        nargs = 1,
        choices = [
            "input",
            "processed",
            "all"
        ]
    )
    parser.add_argument(
        "--clean",
        "-c",
        dest    = "clean",
        action  = "store_true"
    )

    args = parser.parse_args()

    load    = False
    archive = args.archive or []
    clean   = args.clean
    dates   = args.dates
    sources = args.sources or []
    
    config      = loads(open("./config.json", "r").read())
    LOG_FMT     = config["log_fmt"]
    DATE_FMT    = config["date_fmt"]

    today       = datetime.strftime(
                    datetime.today(),
                    DATE_FMT
                )

    start_all = time()

    print(LOG_FMT.format("update", "start", "", ", ".join(sources), 0))

    for source in sources:

        if source == "cboe_all":

            load = True

        elif source == "cboe_latest":

            load = True
            cboe_et.write_csv(today)

        elif source == "cme_all":

            load = True

        elif source == "cme_latest":

            cme_extract.get_files([None, "all"])
            cme_transform.write_csv(today)
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

            dates = [ today ]

        load_processed(dates)

    # archive input and/or processed files, if necessary

    if archive:

        print(LOG_FMT.format("update", "start", "", f"archive {', '.join(archive)}", 0))

        start_archive = time()

        archive_path    = config["archive_path"]
        input_path      = config["input_path"]
        processed_path  = config["processed_path"]

        input_files     = next(walk(input_path))[2]
        processed_files = next(walk(processed_path))[2]

        # all files should be prefixed with DATE_FMT

        if not dates:

            dates = [ today ]

        # zip

        for date in dates:

            with ZipFile(f"{archive_path}{date}.zip", "a") as zip:

                    if archive[0] in [ "all", "input" ]:
                    
                        for fn in input_files:

                            if date in fn:

                                print(LOG_FMT.format("update", "start", "", fn, 0))

                                start = time()
                                
                                zip.write(f"{input_path}{fn}")

                                print(LOG_FMT.format("update", "finish", f"{time() - start: 0.1f}", fn, 0))
                    
                    if archive[0] in [ "all", "processed" ]:
                    
                        for fn in processed_files:
                        
                            if date in fn:

                                print(LOG_FMT.format("update", "start", "", fn, 0))
                                
                                start = time()

                                zip.write(f"{processed_path}{fn}")

                                print(LOG_FMT.format("update", "finish", f"{time() - start: 0.1f}", fn, 0))

        print(LOG_FMT.format("update", "finish", f"{time() - start_archive: 0.1f}", f"archive {', '.join(archive)}", 0))

    # delete raw input and processed records, if necessary

    if clean:

        print(LOG_FMT.format("update", "start", "", f"clean", 0))

        start_clean = time()

        with open("./config.json", "r") as fd:

            config = loads(fd.read())

            for path in [ 
                config["input_path"],
                config["processed_path"]
            ]:

                bundles = walk(path)

                for _, _, fns in bundles:
                    
                    for fn in fns:
                        
                        if fn != ".gitignore":
                        
                            remove(f"{path}{fn}")

                            print(LOG_FMT.format("update", "finish", "", f"clean {fn}", 0))

        print(LOG_FMT.format("update", "finish", f"{time() - start_clean: 0.1f}", f"clean", 0))

    print(LOG_FMT.format("update", "finish", f"{time() - start_all: 0.1f}", "", 0))