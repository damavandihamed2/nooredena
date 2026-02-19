import pandas as pd
import requests as rq
from tqdm import tqdm
from time import sleep
import random, warnings, datetime

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
last_year = int((datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y%m%d"))

####################################################################################################

symbols = pd.read_sql(f"SELECT symbol, symbol_id, final_price, total_share FROM [nooredenadb].[tsetmc].[symbols]"
                      f" WHERE active=1 AND final_last_date>={last_year}", db_conn)
symbols["market_cap"] = symbols["final_price"] * symbols["total_share"]

portfolio = pd.read_sql("SELECT symbol FROM [nooredenadb].[extra].[portfolio]", db_conn)
portfolio["portfolio"] = True

symbols = symbols.merge(portfolio, on="symbol", how="left").fillna(False, inplace=False)
# symbols = symbols[symbols["portfolio"] | (symbols["market_cap"] >= 10 * 1e13)].reset_index(drop=True, inplace=False)
symbols = symbols[symbols["portfolio"]].reset_index(drop=True, inplace=False)

####################################################################################################

symbols_df = []
for i in tqdm(range(len(symbols))):
    symbol_id = symbols["symbol_id"].iloc[i]
    url = f"https://tradersarena.ir/data/{symbol_id}/reports"
    res = rq.get(url=url)
    if res.status_code == 200:
        res = res.json()
        SSup = res["SSup"]
        SRes = res["SRes"]
        del res["SSup"]
        del res["SRes"]
        for k, v in SSup.items():
            res["S" + k] = v["y"]
            res["S" + k + "_date"] = v["x"]
        for k, v in SRes.items():
            res["S" + k] = v["y"]
            res["S" + k + "_date"] = v["x"]
        DSup = res["DSup"]
        DRes = res["DRes"]
        del res["DSup"]
        del res["DRes"]
        res["symbol_id"] = symbol_id

        symbols_df.append(res)

        sleep(random.randint(5, 31)/10)

symbols_df = pd.DataFrame(symbols_df)
symbols_df = symbols_df[["symbol_id", "SS0", "SS0_date", "SR0", "SR0_date"]]


crsr = db_conn.cursor()
for i in tqdm(range(len(symbols_df))):
    symbols_id = symbols_df["symbol_id"].iloc[i]
    for c in ["SS0", "SR0"]:
        if not pd.isna(symbols_df[c].iloc[i]):
            param = int(symbols_df[c].iloc[i])
            param_date = symbols_df[c + "_date"].iloc[i]
            crsr.execute(f"UPDATE [nooredenadb].[tradersarena].[symbols] SET {c}={param}, {c}_date='{param_date}'"
                         f" WHERE symbol_id='{symbols_id}'")
crsr.close()
