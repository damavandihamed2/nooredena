import logging
import numpy as np
import pandas as pd
import os, sys, jdatetime
from pyodbc import Connection

from utils.custom_logger import get_logger
from utils.database import insert_to_database
from fda.crawler.fda import (
    get_fda_cookies,
    search_atc,
    update_brand_owners_table,
    update_license_owners_table
)



def get_all_atc(atc_list: list[str], logger: logging.Logger) -> list[dict]:

    cookies = get_fda_cookies()
    all_list = []
    for c in range(len(atc_list)):
        print(atc_list[c])
        atc_search_result = search_atc(
            atc=atc_list[c], cookies=cookies, page=1, page_size=9999, logger=logger, time_out=60
        )
        if atc_search_result:
            all_list += atc_search_result
    return all_list


def parse_atc_data(atc_data_list: list[dict], today_date: str) -> pd.DataFrame:
    all_atc_df = pd.DataFrame(atc_data_list)
    all_atc_df.drop(labels=["package_price"], axis=1, inplace=True, errors="ignore")
    all_atc_df["date"] = today_date
    all_atc_df.replace("MG'ML", "MG/ML", regex=True, inplace=True)
    all_atc_df.replace("'", '', regex=True, inplace=True)
    all_atc_df.replace(
        {"brand_owner": {"": np.nan}, "license_owner": {"": np.nan}, "package": {"": np.nan}},
        regex=False, inplace=True)

    return all_atc_df


def update_brand_and_license(all_atc_df: pd.DataFrame, db_connection: Connection) -> None:
    brand_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[brand_owners]", db_connection)
    license_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[license_owners]", db_connection)
    all_atc_df_ = all_atc_df.merge(brand_owners, on=["brand_owner"], how="left").merge(
        license_owners, on=["license_owner"], how="left")

    brand_owners_na = all_atc_df_[~all_atc_df_["brand_owner"].isna() & all_atc_df_["brand_owner_id"].isna()]
    license_owners_na = all_atc_df_[
        ~all_atc_df_["license_owner"].isna() & all_atc_df_["license_owner_id"].isna()]
    if not brand_owners_na.empty:
        update_brand_owners_table(
            new_brand_owners_list=brand_owners_na["brand_owner"].unique().tolist(), db_conn=db_connection)
    if not license_owners_na.empty:
        update_license_owners_table(
            new_license_owners_list=license_owners_na["license_owner"].unique().tolist(), db_conn=db_connection)


def update_atc_db(all_atc_df: pd.DataFrame, db_connection: Connection) -> None:

    brand_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[brand_owners]", db_connection)
    license_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[license_owners]", db_connection)
    all_atc_df = all_atc_df.merge(brand_owners, on=["brand_owner"], how="left").merge(
        license_owners, on=["license_owner"], how="left")
    all_atc_df["medicine_id"] = all_atc_df["link"].str.split("/", expand=True).iloc[:, -1].astype(int)
    all_atc_df["generic_code"] = all_atc_df["generic_code"].astype(int)

    medicines = all_atc_df.drop(labels=["brand_owner", "license_owner"], axis=1, inplace=False)
    insert_to_database(dataframe=medicines, database_table="[nooredenadb].[fda].[medicines_atc_search]")
    crsr = db_connection.cursor()
    crsr.execute("DELETE FROM [nooredenadb].[fda].[medicines_atc_search] WHERE "
                 "date != (SELECT MAX(date) FROM [nooredenadb].[fda].[medicines_atc_search])")
    crsr.close()


def update_medicines_atc(db_connection: Connection) -> None:

    all_atc_list = ["A", "B", "C", "D", "G", "H", "J", "L", "M", "N", "P", "R", "S", "V"]
    today = jdatetime.datetime.today().strftime(format="%Y/%m/%d")

    logger = get_logger(
        db_connection=db_connection,
        enable_database=False,
        enable_file=False,
        file_name="fda_atc_crawler.py",
        project_dir=os.path.dirname(os.path.abspath(sys.argv[0]))
    )

    all_medicines_list = get_all_atc(atc_list=all_atc_list, logger=logger)
    all_medicines = parse_atc_data(atc_data_list=all_medicines_list, today_date=today)

    update_brand_and_license(all_atc_df=all_medicines, db_connection=db_connection)

    update_atc_db(all_atc_df=all_medicines, db_connection=db_connection)
