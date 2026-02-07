import json

from utils.database import make_connection



def get_tokens(app: str, web_address: str, **kwargs) -> dict | None:
    portfolio_id = kwargs.pop("portfolio_id", None)
    if kwargs: raise TypeError(f"Unsupported kwargs: {', '.join(kwargs.keys())}")
    sql = "SELECT json_data FROM [nooredenadb].[extra].[auth_tokens] WHERE app = ? AND web_address = ?"
    params = [app, web_address]
    if portfolio_id is not None:
        sql += " AND portfolio_id = ?"
        params.append(portfolio_id)
    conn = make_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row: return None
        return json.loads(row[0])
    finally:
        try: cur.close()
        except Exception: pass
        conn.close()


def update_tokens(app: str, web_address: str, json_data: dict, **kwargs) -> None:
    portfolio_id = kwargs.pop("portfolio_id", None)
    if kwargs: raise TypeError(f"Unsupported kwargs: {', '.join(kwargs.keys())}")
    json_data_str = json.dumps(json_data)
    sql = """
       MERGE [nooredenadb].[extra].[auth_tokens] AS tgt
       USING (SELECT ? AS app, ? AS web_address, ? AS portfolio_id) AS src
           ON  tgt.app = src.app
           AND tgt.web_address = src.web_address
           AND (
               (tgt.portfolio_id = src.portfolio_id)
               OR (tgt.portfolio_id IS NULL AND src.portfolio_id IS NULL)
           )
       WHEN MATCHED THEN
           UPDATE SET json_data = ?
       WHEN NOT MATCHED THEN
           INSERT (app, web_address, portfolio_id, json_data)
           VALUES (src.app, src.web_address, src.portfolio_id, ?);
       """
    params = [app, web_address, portfolio_id, json_data_str, json_data_str]
    conn = make_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    finally:
        try: cur.close()
        except Exception: pass
        conn.close()

# def get_tokens(app: str, web_address: str) -> dict | None:
#     db_conn = make_connection()
#     crsr = db_conn.cursor()
#     crsr.execute(f"SELECT json_data FROM [nooredenadb].[extra].[auth_tokens] "
#                  f"WHERE app='{app}' AND web_address='{web_address}';")
#     data = crsr.fetchall()
#     crsr.close()
#     db_conn.close()
#     if data:
#         try:
#             json_data_str = data[0][0]
#             tokens = json.loads(json_data_str)
#             return tokens
#         except Exception:
#             pass
#
# def update_tokens(app: str, web_address: str, json_data: dict) -> None:
#     json_data_str = json.dumps(json_data)
#     db_conn = make_connection()
#     crsr = db_conn.cursor()
#     crsr.execute(f"SELECT * FROM [nooredenadb].[extra].[auth_tokens] "
#                  f"WHERE app='{app}' AND web_address='{web_address}'")
#     data = crsr.fetchall()
#     if data:
#         crsr.execute(f"UPDATE [nooredenadb].[extra].[auth_tokens] "
#                      f"SET json_data='{json_data_str}' "
#                      f"WHERE app='{app}' AND web_address='{web_address}'")
#         crsr.close()
#         db_conn.close()
#     else:
#         crsr.execute(
#             f"INSERT INTO [nooredenadb].[extra].[auth_tokens] (app, web_address, json_data) VALUES (?, ?, ?)",
#             (app, web_address, json_data_str))
#         crsr.close()
#         db_conn.close()
