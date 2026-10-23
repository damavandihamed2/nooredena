import time
import pyodbc
import numpy as np
from tqdm import tqdm
from typing import Optional
from pandas import DataFrame
from dotenv import dotenv_values



ENV = dotenv_values(dotenv_path=".env")
db_ip, db_name, db_user, db_pass = ENV["DB_SERVER"], ENV["DB_NAME"], ENV["DB_USERNAME"], ENV["DB_PASSWORD"]

public_user = {
    "public": {"username": "userpublic", "password": "Nooredena@public.123"}
}


def make_connection(username: str = db_user, password: str = db_pass, auto_commit: bool = True,
                    timeout: int = 10, retries: int = 10, delay: int = 3) -> Optional[pyodbc.Connection]:
    conn_str = (f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={db_ip}; DATABASE={db_name}; "
                f"UID={username}; PWD={password}; Encrypt=No")
    for attempt in range(1, retries + 1):
        try:
            connection = pyodbc.connect(conn_str, timeout=timeout, autocommit=auto_commit)
            return connection
        except pyodbc.Error as e:
            print(f"[make_connection] Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise ConnectionError(f"[make_connection] Failed to connect to {conn_str} after {retries} try, error: {e}")
            time.sleep(delay)
    return None


def insert_to_database(
        dataframe: DataFrame,
        database_table: str,
        db_connection: Optional[pyodbc.Connection] = None,
        loading: bool = True,
        batch_size: int = 1000
) -> None:

    should_close = False
    if db_connection is None:
        db_connection = make_connection()
        should_close = True


    cursor = db_connection.cursor()
    cursor.fast_executemany = True

    cols = ", ".join(dataframe.columns.tolist())
    placeholders = ", ".join("?" * len(dataframe.columns))
    sql = f"INSERT INTO {database_table} ({cols}) VALUES ({placeholders})"

    data = [
        tuple(None if (isinstance(x, float) and np.isnan(x)) else x for x in row)
        for row in dataframe.itertuples(index=False, name=None)
    ]

    iterator = tqdm(range(0, len(data), batch_size)) if loading else range(0, len(data), batch_size)
    for i in iterator:
        batch = data[i:i + batch_size]
        cursor.executemany(sql, batch)

    cursor.close()
    if should_close:
        db_connection.close()
