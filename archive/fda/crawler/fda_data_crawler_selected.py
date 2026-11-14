import numpy as np
import pandas as pd
from tqdm import tqdm
from pyodbc import Connection
import os, sys, jdatetime, logging

from utils.custom_logger import get_logger
from utils.database import insert_to_database
from fda.crawler.fda import (
    get_medicine_data,
    get_fda_cookies
)



def get_selected_medicines(links_list: list[str], logger: logging.Logger) -> list[dict]:
    cookies = get_fda_cookies()
    all_data = []
    for l in tqdm(range(len(links_list))):
        medicine_data = get_medicine_data(link=links_list[l], cookies=cookies, max_tries=10, logger=logger)
        if medicine_data:
            all_data.append(medicine_data)
    return all_data


def parse_selected_medicines(medicines_data_list: list[dict], today_date: str) -> pd.DataFrame:
    crawl_data = pd.DataFrame(medicines_data_list)
    crawl_data.replace({"": np.nan, "-": np.nan}, inplace=True, regex=False)
    crawl_data["consumer_package_price"] = crawl_data["consumer_package_price"].replace(
        0, np.nan, inplace=False, regex=False).astype(dtype=float)
    crawl_data["price"] = crawl_data["price"].replace(0, np.nan, inplace=False, regex=False).astype(dtype=float)
    crawl_data.replace("MG'ML", "MG/ML", regex=True, inplace=True)
    crawl_data.replace("'", '', regex=True, inplace=True)
    crawl_data["date"] = today_date
    crawl_data["medicine_id"] = crawl_data["link"].str.split("/", expand=True).iloc[:, -1].astype(int)
    medicines_history_selected = crawl_data[["date", "medicine_id", "consumer_package_price", "price"]]
    return medicines_history_selected


def update_selected_db(medicines_history_selected: pd.DataFrame) -> None:
    insert_to_database(
        dataframe=medicines_history_selected,
        database_table="[nooredenadb].[fda].[medicines_history_selected]"
    )


def update_medicines_data_selected(db_connection: Connection) -> None:

    today = jdatetime.datetime.now().strftime(format="%Y/%m/%d")

    logger = get_logger(
        db_connection=db_connection,
        file_name="fda_data_crawler_selected.py",
        level=logging.INFO,
        project_dir=os.path.dirname(os.path.abspath(sys.argv[0]))
    )

    latest_links = pd.read_sql(
        "SELECT DISTINCT(link) AS link FROM [nooredenadb].[fda].[medicines_weight]",
        db_connection
    )
    latest_links = latest_links["link"].str.replace(" ", "").values.tolist()

    crawl_data = get_selected_medicines(
        links_list=latest_links,
        logger=logger
    )

    selected_df = parse_selected_medicines(
        medicines_data_list=crawl_data,
        today_date=today
    )

    update_selected_db(
        medicines_history_selected=selected_df
    )
