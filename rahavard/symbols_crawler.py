import pandas as pd
from tqdm import tqdm
import warnings, jdatetime

from rahavard.rahavard365 import Agent
from utils.database import make_connection, insert_to_database



warnings.filterwarnings("ignore")
db_conn = make_connection()
agent = Agent()
column_list = ["name", "asset_id", "slug", "instrument_state", "instrument_description", "trade_date_time"]

##################################################

stocks_data = agent.get_active_stocks()
stocks_df = pd.DataFrame(stocks_data)
stocks_df = stocks_df[column_list]

closed_stocks_data = agent.get_closed_stocks()
closed_stocks_df = pd.DataFrame(closed_stocks_data)
closed_stocks_df.rename({"reason_of_close": "instrument_description", "last_trade_date_time": "trade_date_time"},
                        axis=1, inplace=True)
closed_stocks_df = closed_stocks_df[~closed_stocks_df["asset_id"].isin(stocks_df["asset_id"])]
closed_stocks_df["instrument_state"] = "closed"
closed_stocks_df = closed_stocks_df[column_list]

##################################################

stocks_df = pd.concat([stocks_df, closed_stocks_df], axis=0, ignore_index=True)
stocks_df[["type", "fiscal_year"]] = "stock", None
for s in tqdm(range(len(stocks_df))):
    asset_id = stocks_df["asset_id"].iloc[s]
    data = agent.get_asset_data(asset_id=asset_id)
    if (not data) or (not data.get("eps")):
        continue
    fiscal_year = data.get("eps").get("fiscal_year")
    fiscal_month = jdatetime.datetime.fromgregorian(year=int(fiscal_year[:4]), month=int(fiscal_year[5:7]),
                                                    day=int(fiscal_year[8:10])).month
    stocks_df["fiscal_month"].iloc[s] = fiscal_month

##################################################

funds_etf_data = agent.get_funds_etf()
funds_etf_df = pd.DataFrame(funds_etf_data)[column_list]
funds_etf_df["type"] = "fund"

##################################################

stocks_df = pd.concat([stocks_df, funds_etf_df], axis=0, ignore_index=True)
stocks_df.rename({"name": "symbol", "instrument_state": "state", "instrument_description": "description",
                  "trade_date_time": "last_date"}, axis=1, inplace=True)

crsr = db_conn.cursor()
crsr.execute("TRUNCATE TABLE [nooredenadb].[rahavard].[symbols]")
crsr.close()
insert_to_database(dataframe=stocks_df, database_table="[nooredenadb].[rahavard].[symbols]")
