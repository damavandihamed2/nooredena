import pandas as pd

from utils.database import make_connection, insert_to_database




db_conn = make_connection()


trades_ime = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[trades_ime] WHERE symbol IN (SELECT ContractCode FROM nooredenadb.ime.live_tablo_cdc)", db_conn)
trades_ime = trades_ime.groupby(by=['date', 'portfolio_id', 'symbol', 'type'], as_index=False).sum()

trades_ime["value"] = trades_ime["value"] + trades_ime["commission"] * (trades_ime["type"] * -2) + 3
trades_ime.to_excel("c:/users/h.damavandi/desktop/trades_ime.xlsx", index=False)


