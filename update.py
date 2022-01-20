from argparse import ArgumentParser
import cme_extract
import cme_transform
import srf_extract
import srf_transform

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
    
    args = parser.parse_args()

    for source in args.sources:

        if source == "cboe_all":

            pass

        elif source == "cboe_latest":

            pass

        elif source == "cme_all":

            pass

        elif source == "cme_latest":

            cme_extract.get_files([None, "all"])
            cme_transform.write_csv()

        elif source == "srf_all":

            srf_extract.get_files("all")
            srf_transform.write_csv()

        elif source == "srf_latest":

            srf_extract.get_files("partial")
            srf_transform.write_csv()
