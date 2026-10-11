import warnings
import argparse

from sigma.crawler.fetch import Agent
from utils.database import make_connection
from sigma.crawler.update_data import (
    update_buysell_data,
    update_profitloss_data,
    update_portfolio_data,
    update_dividend_data,
    update_all
)



warnings.filterwarnings("ignore")

JOBS = {
    "buysell": update_buysell_data,
    "profitloss": update_profitloss_data,
    "portfolio": update_portfolio_data,
    "dividend": update_dividend_data,
    "all": update_all,
}
PORTFOLIO_IDS = [1, 25]
AUTHENTICATOR = [{"username": "damavandi", "password": "Dd@123456"}]


def parse_arguments():

    parser = argparse.ArgumentParser(
        description="Sigma data update runner"
    )

    parser.add_argument(
        "job",
        choices=JOBS.keys(),
        help="Job to execute"
    )

    return parser.parse_args()


def run(job: str) -> None:

    if job not in JOBS:
        raise ValueError(f"Unknown TSETMC job: {job}")


    db_conn = make_connection()

    if db_conn:

        sigma_agent = Agent(
            username=AUTHENTICATOR[0]["username"],
            password=AUTHENTICATOR[0]["password"],
            db_connection=db_conn
        )
        sigma_agent.login()

        try:

            print(f"Running TSETMC job: {job}")

            JOBS[job](
                agent=sigma_agent,
                portfolio_ids_list=PORTFOLIO_IDS,
                db_connection=db_conn
            )

        finally:
            db_conn.close()


def main():

    args = parse_arguments()
    run(args.job)


if __name__ == "__main__":
    main()
