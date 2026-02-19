import pandas as pd
from tqdm import tqdm
import requests as rq
import warnings, jdatetime

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()

header_ipcheck = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"}
url1 = "https://rahavard365.com/api/v2/ip-check"
res1 = rq.get(url=url1, headers=header_ipcheck)
cookie1 = res1.headers["Set-Cookie"].split(";")[0]

########################################################################################################

header_stocks = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/126.0.0.0 Safari/537.36'}
stocks = rq.get(url="https://rahavard365.com/api/v2/market-data/stocks?last_trade=any", headers=header_stocks)
stocks = pd.DataFrame(stocks.json()["data"])
stocks = stocks[["name", "asset_id", "slug", "instrument_state", "instrument_description", "trade_date_time"]]

########################################################################################################

closed_stocks = rq.get(url="https://rahavard365.com/api/v2/market-data/closed-assets", headers=header_stocks)
closed_stocks = pd.DataFrame(closed_stocks.json()["data"]["closed_asset_list"])
closed_stocks = closed_stocks[~closed_stocks["asset_id"].isin(stocks["asset_id"])]
closed_stocks = closed_stocks[["name", "asset_id", "slug", "reason_of_close", "last_trade_date_time"]].rename(
    {"reason_of_close": "instrument_description", "last_trade_date_time": "trade_date_time"}, axis=1, inplace=False)
closed_stocks["instrument_state"] = ["closed"] * len(closed_stocks)

########################################################################################################

stocks = pd.concat([stocks, closed_stocks], axis=0, ignore_index=True)
stocks["type"] = ["stock"] * len(stocks)
stocks["fiscal_month"] = None

for s in tqdm(range(len(stocks))):
    asset_id = stocks["asset_id"].iloc[s]
    response = rq.get(
        url=f"https://rahavard365.com/api/v2/asset/{asset_id}",
        headers=header_stocks
    )
    if response.status_code == 200:
        try:
            res_json = response.json()
            data = res_json.get("data")
            eps = data.get("eps")
            if eps:
                fiscal_year = eps.get("fiscal_year")
                fiscal_month = int(
                    jdatetime.datetime.fromgregorian(
                        year=int(fiscal_year[:4]), month=int(fiscal_year[5:7]), day=int(fiscal_year[8:10])
                    ).strftime("%m"))
                stocks["fiscal_month"].iloc[s] = fiscal_month
        except Exception as e:
            print(f"Error for {asset_id=} ({e})")

########################################################################################################

funds_etf = rq.get("https://rahavard365.com/api/v2/market-data/etf-funds?fund_type=1&category_id=all&last_trade=any",
                   headers=header_stocks)
funds_etf = pd.DataFrame(funds_etf.json()["data"])
funds_etf = funds_etf[["name", "asset_id", "slug", "instrument_state", "instrument_description", "trade_date_time"]]
funds_etf["type"] = ["fund"] * len(funds_etf)

########################################################################################################

stocks = pd.concat([stocks, funds_etf], axis=0, ignore_index=True)
stocks.rename({"name": "symbol", "instrument_state": "state", "instrument_description": "description",
               "trade_date_time": "last_date"}, axis=1, inplace=True)

crsr = db_conn.cursor()
crsr.execute("TRUNCATE TABLE [nooredenadb].[rahavard].[symbols]")
crsr.close()

insert_to_database(dataframe=stocks, database_table="[nooredenadb].[rahavard].[symbols]")

