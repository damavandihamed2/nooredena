import json
from pathlib import Path
from pyodbc import Connection

from utils.database import make_connection


def get_schema_metadata(conn: Connection, schemas: list[str]):

    placeholders = ",".join("?" for _ in schemas)

    query = f"""
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        c.column_id,
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable
    FROM sys.columns c
    JOIN sys.tables t 
        ON c.object_id = t.object_id
    JOIN sys.schemas s 
        ON t.schema_id = s.schema_id
    JOIN sys.types ty 
        ON c.user_type_id = ty.user_type_id
    WHERE s.name IN ({placeholders})
    ORDER BY 
        s.name,
        t.name,
        c.column_id
    """

    cursor = conn.cursor()
    cursor.execute(query, schemas)

    columns = [c[0] for c in cursor.description]

    rows = [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

    cursor.close()

    return json.dumps(rows, ensure_ascii=False, indent=2)


def save_json_to_path(json_data, output_path: str):
    if isinstance(json_data, str):
        json_data = json.loads(json_data)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    return str(output_path)


if __name__ == "__main__":

    schema_list = ["bourseview", "codal", "commodity", "economic", "enigma", "iee", "ime", "rahavard", "tgju", 'tsetmc']

    db_conn = make_connection()
    data_json = get_schema_metadata(conn=db_conn, schemas=schema_list)
    db_conn.close()

    save_json_to_path(data_json, "./postgres_migration/metadata.json")

