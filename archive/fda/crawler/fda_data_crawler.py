import logging
import numpy as np
import pandas as pd
from tqdm import tqdm
from pyodbc import Connection
import os, sys, jdatetime, warnings

from fda.crawler.fda import (
    get_medicine_data, get_fda_cookies,
    update_brand_owners_table, update_license_owners_table,
    update_producers_table, update_roas_table,
    update_dosage_forms_table, update_atcs_table
)
from utils.custom_logger import get_logger
from utils.database import insert_to_database



warnings.filterwarnings("ignore")


def get_all_medicines(links_list: list[str], logger: logging.Logger) -> list[dict]:
    cookies = get_fda_cookies()
    all_data = []
    for l in tqdm(range(len(links_list))):
        medicine_data = get_medicine_data(link=links_list[l], cookies=cookies, max_tries=10, logger=logger)
        if medicine_data:
            all_data.append(medicine_data)
    return all_data


def parse_medicines_data(medicines_data_list: list[dict], today_date: str) -> pd.DataFrame:
    crawl_data = pd.DataFrame(medicines_data_list)
    crawl_data.replace({"": np.nan, "-": np.nan}, inplace=True, regex=False)
    crawl_data["consumer_package_price"] = crawl_data["consumer_package_price"].replace(
        0, np.nan, inplace=False, regex=False).astype(dtype=float)
    crawl_data["price"] = crawl_data["price"].replace(0, np.nan, inplace=False, regex=False).astype(dtype=float)
    crawl_data.replace("MG'ML", "MG/ML", regex=True, inplace=True)
    crawl_data.replace("'", '', regex=True, inplace=True)
    crawl_data["date"] = today_date
    return crawl_data


def update_normalizer_tables(all_medicines_df: pd.DataFrame, db_connection: Connection):
    brand_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[brand_owners]", db_connection)
    license_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[license_owners]", db_connection)
    atcs = pd.read_sql("SELECT ATC, ATC_id FROM [nooredenadb].[fda].[ATCs]", db_connection)
    dosage_forms = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[dosage_forms]", db_connection)
    producers = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[producers]", db_connection)
    roas = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[ROAs]", db_connection)
    crawl_data_ = all_medicines_df.merge(
        brand_owners, on="brand_owner", how="left"
    ).merge(
        license_owners, on="license_owner", how="left"
    ).merge(
        atcs, on="ATC", how="left"
    ).merge(
        dosage_forms, on="dosage_form", how="left"
    ).merge(
        producers, on="producer", how="left"
    ).merge(
        roas, on="roa", how="left"
    )

    brand_owners_na = crawl_data_[~crawl_data_["brand_owner"].isna() & crawl_data_["brand_owner_id"].isna()]
    if not brand_owners_na.empty:
        update_brand_owners_table(new_brand_owners_list=brand_owners_na["brand_owner"].unique().tolist(), db_conn=db_connection)

    license_owners_na = crawl_data_[~crawl_data_["license_owner"].isna() & crawl_data_["license_owner_id"].isna()]
    if not license_owners_na.empty:
        update_license_owners_table(new_license_owners_list=license_owners_na["license_owner"].unique().tolist(), db_conn=db_connection)

    producers_na = crawl_data_[~crawl_data_["producer"].isna() & crawl_data_["producer_id"].isna()]
    if not producers_na.empty:
        update_producers_table(new_producers_list=producers_na["producer"].unique().tolist(), db_conn=db_connection)

    roas_na = crawl_data_[~crawl_data_["roa"].isna() & crawl_data_["roa_id"].isna()]
    if not roas_na.empty:
        update_roas_table(new_roas_list=roas_na["roa"].unique().tolist(), db_conn=db_connection)

    dosage_forms_na = crawl_data_[~crawl_data_["dosage_form"].isna() & crawl_data_["dosage_form_id"].isna()]
    if not dosage_forms_na.empty:
        update_dosage_forms_table(new_dosage_forms_list=dosage_forms_na["dosage_form"].unique().tolist(), db_conn=db_connection)

    atcs_na = crawl_data_[~crawl_data_["ATC"].isna() & crawl_data_["ATC_id"].isna()]
    if not atcs_na.empty:
        atcs_na = atcs_na[['ATC', 'ATC_1', 'ATC_2', 'ATC_3', 'ATC_4', 'ATC_5']]
        atcs_na.drop_duplicates(subset=['ATC'], inplace=True, ignore_index=True, keep='first')
        update_atcs_table(new_atcs_list=atcs_na.to_dict(orient='records'), db_conn=db_connection)


def normalize_medicines_data(all_medicines_df: pd.DataFrame, db_connection: Connection):
    brand_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[brand_owners]", db_connection)
    license_owners = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[license_owners]", db_connection)
    atcs = pd.read_sql("SELECT ATC, ATC_id FROM [nooredenadb].[fda].[ATCs]", db_connection)
    dosage_forms = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[dosage_forms]", db_connection)
    producers = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[producers]", db_connection)
    roas = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[ROAs]", db_connection)
    crawl_data = all_medicines_df.merge(
        brand_owners, on="brand_owner", how="left"
    ).merge(
        license_owners, on="license_owner", how="left"
    ).merge(
        atcs, on="ATC", how="left"
    ).merge(
        dosage_forms, on="dosage_form", how="left"
    ).merge(
        producers, on="producer", how="left"
    ).merge(
        roas, on="roa", how="left"
    )
    crawl_data["medicine_id"] = crawl_data["link"].str.split("/", expand=True).iloc[:, -1].astype(int)
    return crawl_data


def update_history_db(all_medicines_df: pd.DataFrame, db_connection: Connection):

    medicines_history = all_medicines_df[
        ["date", "medicine_id", "link", "name", "dosage_form_id", "roa_id", "license_owner_id", "brand_owner_id",
         "producer_id", "license_date", "consumer_package_price", "GTIN", "IRC", "package", "general_name",
         "price", "ATC_id"]
    ]
    insert_to_database(dataframe=medicines_history, database_table="[nooredenadb].[fda].[medicines_page]")

    crsr = db_connection.cursor()
    crsr.execute("DELETE FROM [nooredenadb].[fda].[medicines_page] "
                 "WHERE date != (SELECT MAX(date) FROM [nooredenadb].[fda].[medicines_page])")
    crsr.close()

    medicines_history = medicines_history[["date", "medicine_id", "consumer_package_price", "price"]]
    insert_to_database(dataframe=medicines_history, database_table="[nooredenadb].[fda].[medicines_history]")

    medicines_atc_search = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[medicines_atc_search]", db_connection)
    medicines_atc_search.drop(
        labels=["date", "link", "license_owner_id", "brand_owner_id", "package", "product_code", "name_latin"],
        axis=1,
        inplace=True
    )
    medicines_page = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[medicines_page]", db_connection)
    medicines = medicines_page.merge(medicines_atc_search, on=['medicine_id'], how='left')
    insert_to_database(dataframe=medicines, database_table="[nooredenadb].[fda].[medicines]")
    crsr = db_connection.cursor()
    crsr.execute("DELETE FROM [nooredenadb].[fda].[medicines] "
                 "WHERE date != (SELECT MAX(date) FROM [nooredenadb].[fda].[medicines])")
    crsr.close()



def update_medicines_data(db_connection: Connection):

    today = jdatetime.datetime.now().strftime(format="%Y/%m/%d")

    logger = get_logger(
        db_connection=db_connection,
        file_name="fda_data_crawler.py",
        project_dir=os.path.dirname(os.path.abspath(sys.argv[0]))
    )

    latest_links = pd.read_sql("SELECT * FROM [nooredenadb].[fda].[medicines_atc_search]", db_connection)
    latest_links = latest_links["link"].str.replace(" ", "").values.tolist()

    crawl_data = get_all_medicines(links_list=latest_links, logger=logger)

    crawl_data_parsed = parse_medicines_data(
        medicines_data_list=crawl_data,
        today_date=today
    )

    update_normalizer_tables(
        all_medicines_df=crawl_data_parsed,
        db_connection=db_connection
    )

    crawl_data_normalized = normalize_medicines_data(
        all_medicines_df=crawl_data_parsed,
        db_connection=db_connection
    )

    update_history_db(
        all_medicines_df=crawl_data_normalized,
        db_connection=db_connection
    )
