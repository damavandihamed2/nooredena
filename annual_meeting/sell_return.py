import warnings
import pandas as pd

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
(fnt, color1, color2) = ("B Nazanin", "#56c4cd", "#f8a81d")

sells_start_date = "1403/05/07"
sells_end_date = "1403/06/26"

query_sells = ("SELECT symbol, date, SUM(net_value) as value FROM [nooredenadb].[sigma].[sigma_buysell]"
               f" WHERE date>='{sells_start_date}' AND date<='{sells_end_date}' AND symbol NOT IN "
               "('اعتماد', 'کارا', 'افران', 'لبخند', 'سپر', 'یاقوت') AND type"
               "!='اختیار معامله'  GROUP BY date, symbol ORDER BY date")
sells = pd.read_sql(query_sells, db_conn)
sells = sells[sells["symbol"] != "وشهرح"].reset_index(drop=True, inplace=False)
sells["symbol"].replace({"ک": "ك", "ی": "ي"}, regex=True, inplace=True)
sell_symbols = sells["symbol"].unique().tolist()
symbols = pd.read_sql(f"SELECT * FROM [nooredenadb].[tsetmc].[symbols] WHERE symbol IN {str(tuple(sell_symbols))} "
                      f"AND active=1", db_conn)

sell_symbols_id = symbols["symbol_id"].values.tolist()
query_symbols_history = (f"SELECT TEMP2.date, TEMP1.symbol_id, TEMP1.yesterday_price, TEMP1.final_price FROM (SELECT "
                         f"[date] as Miladi, [symbol_id], [yesterday_price], [final_price] FROM "
                         f"[nooredenadb].[tsetmc].[symbols_history] WHERE date>20240101 AND symbol_id IN "
                         f"{str(tuple(sell_symbols_id))} AND trade_amount>0) AS TEMP1 LEFT JOIN (SELECT "
                         f"TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) AS Miladi, [Jalali_1] as date FROM "
                         f"[nooredenadb].[extra].[dim_date]) AS TEMP2 ON TEMP1.Miladi=TEMP2.Miladi")
symbols_history = pd.read_sql(query_symbols_history, db_conn)
symbols_history.sort_values(by=["symbol_id", "date"], inplace=True, ignore_index=True, ascending=False)

symbols_history["shifted"] = symbols_history.groupby(by="symbol_id", as_index=False).shift(-1)["final_price"]
symbols_history.dropna(subset=["shifted"], inplace=True, ignore_index=True)
symbols_history["alpha"] = symbols_history["yesterday_price"] / symbols_history["shifted"]
symbols_history["beta"] = symbols_history[["symbol_id", "alpha"]].groupby(
    by="symbol_id", as_index=False).cumprod().shift(1).fillna(1, inplace=False)

symbols_history["final_price_adj"] = symbols_history["final_price"] * symbols_history["beta"]
symbols_history = symbols_history[["date", "symbol_id", "final_price_adj"]].merge(
    symbols[["symbol", "symbol_id"]],on="symbol_id", how="left")[["date", "symbol", "final_price_adj"]]
symbols_history = symbols_history[symbols_history["date"] < '1403/11/01']


symbols_last_date = symbols_history.groupby(by="symbol", as_index=False).max()[["symbol", "date"]]
symbols_last_price = symbols_last_date.merge(symbols_history, on=["symbol", "date"], how="left")
symbols_last_price.rename({"final_price_adj": "last_price"}, axis=1, inplace=True)

sells = sells.merge(symbols_history, on=["symbol", "date"], how="left")
sells = sells.merge(symbols_last_price[["symbol", "last_price"]], on=["symbol"], how="left")
sells["return"] = (sells["last_price"] / sells["final_price_adj"]) - 1
sells["return_value"] = sells["return"] * sells["value"]

sells.to_excel("c:/users/h.damavandi/desktop/sells.xlsx", index=False)





