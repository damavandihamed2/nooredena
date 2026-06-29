import json
from collections import defaultdict
import psycopg2

JSON_PATH = "./postgres_migration/metadata.json"

PG_CONFIG = {
    "host": "194.180.11.115",
    "port": 35432,
    "dbname": "bourse",
    "user": "bourse_app",
    "password": "LulCSM167gWYYPRKQt1ZL1atHmY8nZcB",
    "sslmode": "disable"
}


def map_type(row):
    t = row["data_type"].lower()

    if t == "bigint":
        return "bigint"

    if t == "int":
        return "integer"

    if t == "smallint":
        return "smallint"

    if t == "bit":
        return "boolean"

    if t in ["float"]:
        return "double precision"

    if t in ["real"]:
        return "real"

    if t in ["varchar", "nvarchar", "text"]:
        if row["max_length"] and row["max_length"] > 0 and row["max_length"] != -1:
            return f"varchar({row['max_length']})"
        return "text"

    if t in ["datetime", "datetime2", "smalldatetime"]:
        return "timestamp"

    if t == "date":
        return "date"

    if t == "uniqueidentifier":
        return "uuid"

    if t in ["decimal", "numeric"]:
        p = row["precision"]
        s = row["scale"]
        return f"numeric({p},{s})"

    return "text"


def load_rows():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # چون کلید top-level همان query است
    rows = list(data.values())[0]
    return rows


def group_tables(rows):
    tables = defaultdict(list)

    for r in rows:
        key = (r["schema_name"], r["table_name"])
        tables[key].append(r)

    for k in tables:
        tables[k] = sorted(tables[k], key=lambda x: x["column_id"])

    return tables


def create_schemas(conn, schemas):
    cur = conn.cursor()

    for s in schemas:
        sql = f'CREATE SCHEMA IF NOT EXISTS "{s}"'
        print(sql)
        cur.execute(sql)

    conn.commit()


def create_tables(conn, tables):
    cur = conn.cursor()

    for (schema, table), cols in tables.items():

        col_defs = []

        for c in cols:

            col_name = c["column_name"]
            col_type = map_type(c)

            nullable = "" if c["is_nullable"] else " NOT NULL"

            col_defs.append(f'"{col_name}" {col_type}{nullable}')

        cols_sql = ",\n".join(col_defs)

        sql = f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
        {cols_sql}
        );
        '''

        print(f"Creating {schema}.{table}")
        cur.execute(sql)

    conn.commit()


def main():

    rows = load_rows()

    schemas = sorted(set(r["schema_name"] for r in rows))

    tables = group_tables(rows)

    conn = psycopg2.connect(**PG_CONFIG)

    create_schemas(conn, schemas)

    create_tables(conn, tables)

    conn.close()

    print("DONE")


if __name__ == "__main__":
    main()
