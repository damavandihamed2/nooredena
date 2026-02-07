import time
import pyodbc
import numpy as np
from tqdm import tqdm


db_server_ip = "172.20.30.34"
db_name = "nooredenadb"
authenticators = {
    "admin": {"username": "sa", "password": "l0XZD6y$B86h"},
    "public": {"username": "userpublic", "password": "Nooredena@public.123"}
}


def make_connection(
        username: str = authenticators["admin"]["username"],
        password: str = authenticators["admin"]["password"],
        auto_commit: bool = True, timeout: int = 10, retries: int = 10, delay: int = 3
):
    conn_str = (f"DRIVER={{ODBC Driver 18 for SQL Server}}; SERVER={db_server_ip}; DATABASE={db_name}; "
                f"UID={username}; PWD={password}; Encrypt=No")
    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(conn_str, timeout=timeout, autocommit=auto_commit)
        except pyodbc.Error as e:
            print(f"[make_connection] Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(delay)


def insert_to_database(dataframe, database_table, loading=True, batch_size: int = 1000):
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
