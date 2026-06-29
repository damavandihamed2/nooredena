import io
import csv
import json
import pyodbc
import psycopg2
from collections import defaultdict



JSON_PATH = "/Users/hameddamavandi/Documents/metadata.json"

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

# def copy_chunk(pg_conn, schema, table, columns, rows):
#
#     buffer = io.StringIO()
#
#     for r in rows:
#         line = "\t".join("" if v is None else str(v) for v in r)
#         buffer.write(line + "\n")
#
#     buffer.seek(0)
#
#     cols = ",".join(f'"{c}"' for c in columns)
#
#     sql = f"""
#     COPY "{schema}"."{table}" ({cols})
#     FROM STDIN WITH (
#         FORMAT text,
#         DELIMITER E'\\t',
#         NULL ''
#     )
#     """
#
#     cur = pg_conn.cursor()
#     cur.copy_expert(sql, buffer)
#     pg_conn.commit()
#

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
        if schema == "tsetmc" and table == "symbols":
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
