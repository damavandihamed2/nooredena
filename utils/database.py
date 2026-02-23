import os
import time
import pyodbc
import numpy as np
from tqdm import tqdm
from pandas import DataFrame
from dotenv import load_dotenv


load_dotenv(dotenv_path=".env", override=False)

db_server_ip = os.getenv("DB_SERVER")
db_server_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

authenticators = {
    "public": {"username": "userpublic", "password": "Nooredena@public.123"}
}

def make_connection(
        username: str = db_username,
        password: str = db_password,
        auto_commit: bool = True, timeout: int = 10, retries: int = 10, delay: int = 3
) -> pyodbc.Connection | None:
    conn_str = (f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={db_server_ip}; DATABASE={db_server_name}; "
                f"UID={username}; PWD={password}; Encrypt=No")
    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(conn_str, timeout=timeout, autocommit=auto_commit)
        except pyodbc.Error as e:
            print(f"[make_connection] Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(delay)


def insert_to_database(dataframe: DataFrame, database_table: str, loading: bool = True, batch_size: int = 1000) -> None:
    db_conn = make_connection()
    cursor = db_conn.cursor()
    cursor.fast_executemany = True
    cols = ", ".join(dataframe.columns.tolist())
    placeholders = ", ".join("?" * len(dataframe.columns))
    sql = f"INSERT INTO {database_table} ({cols}) VALUES ({placeholders})"
    data = []
    for row in dataframe.itertuples(index=False, name=None):
        cleaned = tuple(None if (isinstance(x, float) and np.isnan(x)) else x for x in row)
        data.append(cleaned)
    for i in (tqdm(range(0, len(data), batch_size)) if loading else range(0, len(data), batch_size)):
        batch = data[i:i + batch_size]
        cursor.executemany(sql, batch)
    cursor.close()
    db_conn.close()
