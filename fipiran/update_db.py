from pyodbc import Connection


def upsert_fund_types(types_data: list[dict], db_connection) -> None:

    if not types_data:
        return

    cursor = db_connection.cursor()

    sql = """
        MERGE INTO [nooredenadb].[fipiran].[fund_types] AS Target
        USING (VALUES (?, ?, ?)) AS Source ([fundType], [isActive], [name])
            ON Target.[fundType] = Source.[fundType]
        WHEN MATCHED THEN
            UPDATE SET 
                Target.[isActive] = Source.[isActive],
                Target.[name] = Source.[name],
                Target.[updatedAt] = SYSDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([fundType], [isActive], [name])
            VALUES (Source.[fundType], Source.[isActive], Source.[name]);
    """

    rows = [
        (
            item.get("fundType"),
            item.get("isActive"),
            item.get("name")
        )
        for item in types_data
    ]

    cursor.executemany(sql, rows)
    cursor.close()




def upsert_funds_all(funds_data: list[dict[str, str | int | bool | None]], db_connection: Connection) -> None:

    if not funds_data:
        return

    cursor = db_connection.cursor()
    cursor.fast_executemany = False

    target_table = "[nooredenadb].[fipiran].[funds_all]"
    staging_table = "#funds_all_stagging"

    cols_list = [k for k in {k for f in funds_data for k in f.keys()}]
    cols_sql = ", ".join([f"[{c}]" for c in cols_list])

    cursor.execute(f"""
            IF OBJECT_ID('tempdb..{staging_table}') IS NOT NULL 
                DROP TABLE {staging_table};

            SELECT {cols_sql}
            INTO {staging_table}
            FROM {target_table}
            WHERE 1 = 0;
        """)

    placeholders = ", ".join(["?"] * len(cols_list))
    insert_staging_sql = f"INSERT INTO {staging_table} ({cols_sql}) VALUES ({placeholders});"

    rows = [tuple(item.get(col, None) for col in cols_list) for item in funds_data]
    cursor.executemany(insert_staging_sql, rows)

    cursor.execute(f"""
            CREATE CLUSTERED INDEX IX_staging_reg_group ON {staging_table} ([regNo], [groupId]);
        """)

    update_cols = [c for c in cols_list if c.lower() not in ["regno", "groupid"]]
    set_clause = ", ".join([f"target.[{c}] = stage.[{c}]" for c in update_cols])

    cursor.execute(f"""
            UPDATE target
            SET 
                {set_clause},
                target.[updatedAt] = SYSDATETIME()
            FROM {target_table} target
            INNER JOIN {staging_table} stage 
                ON target.[regNo] = stage.[regNo]
                AND target.[groupId] = stage.[groupId];
        """)

    cursor.execute(f"""
        INSERT INTO {target_table} ({cols_sql})
        SELECT 
            {cols_sql}
        FROM {staging_table} stage
        WHERE NOT EXISTS (
            SELECT 1 FROM {target_table} target
            WHERE target.[regNo] = stage.[regNo] 
              AND target.[groupId] = stage.[groupId]
        );
    """)

    cursor.execute(f"DROP TABLE {staging_table};")
    cursor.close()
