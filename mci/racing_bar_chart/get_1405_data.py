import warnings
import pandas as pd
from tqdm import tqdm

from utils.database import make_connection
from mci.racing_bar_chart.utils.get_data import get_total_share, get_final_price



warnings.filterwarnings("ignore")
db_conn = make_connection()


symbols_query = ("SELECT s.*, sh.last_date FROM (SELECT symbol, symbol_id, final_last_date, active FROM "
                "[nooredenadb].[tsetmc].[symbols] WHERE sector != '68' AND flow != 6 AND "
                "(total_share*final_price) > 150000000000000) AS s LEFT JOIN (select symbol_id, MAX(date) last_date "
                "FROM [nooredenadb].[tsetmc].[symbols_history] WHERE trade_amount <> 0 AND [date] < 20260622 "
                "GROUP BY symbol_id) sh ON s.symbol_id=sh.symbol_id")
symbols = pd.read_sql(symbols_query, db_conn)
db_conn.close()
symbols.dropna(subset=["last_date"], inplace=True)
symbols = symbols[symbols["final_last_date"] >= 20260101]
symbols = symbols[symbols["last_date"] >= 20260101]
symbols.rese_index(drop=True, inplace=True)
symbols["total_share"] = None
symbols["final_price"] = None

for i in tqdm(range(len(symbols))):
    s_id = symbols["symbol_id"].iloc[i]
    s_date = int(symbols["last_date"].iloc[i])
    if symbols["total_share"].iloc[i] is None:
        t_s = get_total_share(s_id, s_date)
        symbols["total_share"].iloc[i] = t_s
    if symbols["final_price"].iloc[i] is None:
        f_p = get_final_price(s_id, s_date)
        symbols["final_price"].iloc[i] = f_p


symbols["market_cap"] = (symbols["total_share"] * symbols["final_price"]) / 1e9
symbols.sort_values("market_cap", ascending=False, inplace=True, ignore_index=True)
symbols["rank"] = range(1, len(symbols) + 1)
symbols["date"] = 1405
symbols = symbols[["symbol", "market_cap", "rank", "date"]]
# symbols.to_excel("c:/users/h.damavandi/desktop/symbols.xlsx", index=False)

