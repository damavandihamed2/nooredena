import warnings
import argparse

from utils.database import make_connection

from fda.crawler.fda_atc_crawler import update_medicines_atc
from fda.crawler.fda_data_crawler import update_medicines_data
from fda.crawler.fda_data_crawler_selected import update_medicines_data_selected



warnings.filterwarnings("ignore")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="FDA Medicines data update runner"
    )

    parser.add_argument(
        "job",
        choices=["atc", "data", "selected"],
        help="Job to execute"
    )

    return parser.parse_args()


def run(job: str) -> None:

    if job not in {"atc", "data", "selected"}:
        raise ValueError(f"Unknown TTS job: {job}")

    db_conn = make_connection()
    if db_conn:

        try:
            if job == "atc":
                print("Search and update Medicines ATC")
                update_medicines_atc(db_connection=db_conn)

            elif job == "data":
                print("Fetch and update Medicines Detail Data")
                update_medicines_data(db_connection=db_conn)

            elif job == "selected":
                print("Fetch and update Data Only for Selected Medicines")
                update_medicines_data_selected(db_connection=db_conn)

        finally:
            db_conn.close()


def main():

    args = parse_arguments()
    run(args.job)


if __name__ == "__main__":
    main()
