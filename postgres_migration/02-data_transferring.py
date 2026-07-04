import io
import csv
import json
import pyodbc
import psycopg2
from collections import defaultdict



JSON_PATH = "./postgres_migration/metadata.json"

PG_CONFIG = {
    "host": "194.180.11.115",
    "port": 35432,
    "dbname": "bourse",
    "user": "bourse_app",
    "password": "LulCSM167gWYYPRKQt1ZL1atHmY8nZcB",
    "sslmode": "disable"
}

SQLSERVER_CONFIG = {
    "driver": "{ODBC Driver 18 for SQL Server}",
    "server": "172.20.30.34",
    "port": "1433",
    "database": "nooredenadb",
    "uid": "sa",
    "pwd": "l0XZD6y$B86h"
}

CHUNK_SIZE = 50000


def load_rows():
    with open(JSON_PATH, "r", encoding="utf8") as f:
        data = json.load(f)

    return list(data.values())[0]


def group_tables(rows):

    tables = defaultdict(list)

    for r in rows:
        key = (r["schema_name"], r["table_name"])
        tables[key].append(r)

    for k in tables:
        tables[k] = sorted(tables[k], key=lambda x: x["column_id"])

    return tables


def connect_sqlserver():

    conn_str = (
        f"DRIVER={SQLSERVER_CONFIG['driver']};"
        f"SERVER={SQLSERVER_CONFIG['server']};"
        f"DATABASE={SQLSERVER_CONFIG['database']};"
        f"UID={SQLSERVER_CONFIG['uid']};"
        f"PWD={SQLSERVER_CONFIG['pwd']};"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)


def connect_postgres():
    return psycopg2.connect(**PG_CONFIG)


def copy_chunk(pg_conn, schema, table, columns, rows):

    buffer = io.StringIO()
    writer = csv.writer(
        buffer,
        delimiter="\t",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n"
    )

    for r in rows:
        writer.writerow(["" if v is None else v for v in r])

    buffer.seek(0)

    cols = ",".join(f'"{c}"' for c in columns)

    sql = f"""
    COPY "{schema}"."{table}" ({cols})
    FROM STDIN WITH (
        FORMAT csv,
        DELIMITER E'\\t',
        NULL '',
        QUOTE '"'
    )
    """

    cur = pg_conn.cursor()
    cur.copy_expert(sql, buffer)
    pg_conn.commit()

def migrate_table(sql_conn, pg_conn, schema, table, columns):

    print(f"Migrating {schema}.{table}")

    cols_sql = ",".join(f"[{c}]" for c in columns)

    query = f"SELECT {cols_sql} FROM [{schema}].[{table}]"

    cursor = sql_conn.cursor()
    cursor.execute(query)

    while True:

        rows = cursor.fetchmany(CHUNK_SIZE)

        if not rows:
            break

        copy_chunk(pg_conn, schema, table, columns, rows)

        print(f"{len(rows)} rows copied")


def main():

    rows = load_rows()

    tables = group_tables(rows)

    sql_conn = connect_sqlserver()
    pg_conn = connect_postgres()

    for (schema, table), cols in tables.items():
        if schema == "commodity":

            column_names = [c["column_name"] for c in cols]

            migrate_table(
                sql_conn,
                pg_conn,
                schema,
                table,
                column_names
            )

    sql_conn.close()
    pg_conn.close()

    print("Migration finished")


if __name__ == "__main__":
    main()

"""
[
    # excluded
    "symbols_data_today", "symbols_detail_data", "symbols_data_daily", "symbol_beta", "symbols_data",
    "stock_historical_data",
    "sector_detail_data", "sector_detail_data_daily", "sector_detail_data_today", "sector_historical_data",
    "raw_shareholders", "daily_shareholders_change", "funds_reserves_change", "funds_shareholding_change",
    "indices_data", "indices_data_today", "indices_return",
    "market_data", "market_data_today", "market_return",
    "options_data_daily", "options_data_today"

    # included
    "market_data_daily", "indices", "indices_history", "symbols", "symbols_clienttype", "symbols_history",
    "symbols_dividend", "symbols_capital_change", "symbols_ros", "symbols_ros_clienttype",
    "symbols_ros_history", "symbols_trade_history"
]
"""
