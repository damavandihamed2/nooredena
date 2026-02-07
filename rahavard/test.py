import pandas as pd
from tqdm import tqdm
from time import sleep
from datetime import datetime
import random, warnings, jdatetime

from rahavard.rahavard365 import Agent, Asset
from utils.database import make_connection, insert_to_database

warnings.filterwarnings("ignore")
powerbi_database = make_connection()

portfolio = pd.read_excel(r"C:\Users\damavandi.nooredena\Desktop\old desktop\هوشمندی معاملات\portfolio(1402-10-30).xlsx")
rahavard_symbols = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[symbols]", powerbi_database)

portfolio_ = portfolio.merge(rahavard_symbols[["symbol", "slug", "asset_id", "type"]], on="symbol", how="left")

dps_df = pd.DataFrame()
dps_fund_df = pd.DataFrame()
capital_changes_df = pd.DataFrame()

agent = Agent(username="09372377126", password="Dh74@123456")

for i in tqdm(range(len(portfolio_))):
    if portfolio_["type"].iloc[i] == "stock":
        asset = Asset(asset_id=portfolio_["asset_id"].iloc[i], agent=agent)
        sleep(random.randint(100, 301) / 100)
        asset.get_dps()
        sleep(random.randint(100, 301) / 100)
        asset.get_capital()
        if asset.dps is not None:
            dps_temp = asset.dps
            dps_temp["asset_id"] = [portfolio_["asset_id"].iloc[i]] * len(dps_temp)
            dps_df = pd.concat([dps_df, dps_temp], axis=0, ignore_index=True)
        if asset.capital_changes is not None:
            capital_changes_temp = asset.capital_changes
            capital_changes_temp["asset_id"] = [portfolio_["asset_id"].iloc[i]] * len(capital_changes_temp)
            capital_changes_df = pd.concat([capital_changes_df, capital_changes_temp], axis=0, ignore_index=True)
    elif portfolio_["type"].iloc[i] == "fund":
        asset = Asset(asset_id=portfolio_["asset_id"].iloc[i], agent=agent)
        sleep(random.randint(100, 301) / 100)
        asset.get_fund_dps()
        if (asset.fund_dps is not None) and (len(asset.fund_dps) > 0):
            dps_temp = asset.fund_dps
            dps_temp["asset_id"] = [portfolio_["asset_id"].iloc[i]] * len(dps_temp)
            dps_fund_df = pd.concat([dps_fund_df, dps_temp], axis=0, ignore_index=True)
    else:
        pass
    sleep(random.randint(100, 301) / 100)

dps_fund_df["date"] = [jdatetime.datetime.fromgregorian(
    year=datetime.strptime(dps_fund_df["date"].iloc[0][:10], "%Y-%m-%d").year,
    month=datetime.strptime(dps_fund_df["date"].iloc[0][:10], "%Y-%m-%d").month,
    day=datetime.strptime(dps_fund_df["date"].iloc[0][:10], "%Y-%m-%d").day
).strftime("%Y/%m/%d") for i in range(len(dps_fund_df))]
insert_to_database(dataframe=dps_fund_df, database_table="[nooredenadb].[rahavard].[dps_funds]")

dps_df["fiscal_year"] = [jdatetime.datetime.fromgregorian(
    year=datetime.strptime(dps_df["fiscal_year"].iloc[0][:10], "%Y-%m-%d").year,
    month=datetime.strptime(dps_df["fiscal_year"].iloc[0][:10], "%Y-%m-%d").month,
    day=datetime.strptime(dps_df["fiscal_year"].iloc[0][:10], "%Y-%m-%d").day
).strftime("%Y/%m/%d") for i in range(len(dps_df))]
dps_df["date_time"] = [jdatetime.datetime.fromgregorian(
    year=datetime.strptime(dps_df["date_time"].iloc[0][:10], "%Y-%m-%d").year,
    month=datetime.strptime(dps_df["date_time"].iloc[0][:10], "%Y-%m-%d").month,
    day=datetime.strptime(dps_df["date_time"].iloc[0][:10], "%Y-%m-%d").day
).strftime("%Y/%m/%d") for i in range(len(dps_df))]
dps_df["announcement_date"] = [jdatetime.datetime.fromgregorian(
    year=datetime.strptime(dps_df["announcement_date"].iloc[0][:10], "%Y-%m-%d").year,
    month=datetime.strptime(dps_df["announcement_date"].iloc[0][:10], "%Y-%m-%d").month,
    day=datetime.strptime(dps_df["announcement_date"].iloc[0][:10], "%Y-%m-%d").day
).strftime("%Y/%m/%d") for i in range(len(dps_df))]
insert_to_database(dataframe=dps_df, database_table="[nooredenadb].[rahavard].[dps]")

# capital_changes_df.to_pickle("c:/users/damavandi.NOOREDENA/desktop/old desktop/capital_changes_df.pkl")
capital_changes_df = pd.read_pickle("c:/users/damavandi.NOOREDENA/desktop/old desktop/capital_changes_df.pkl")
for c in ["company", "report", "meeting", "underwriting_end_report",
          "registration_report", "stock_certificate_receive_report"]:
    capital_changes_df[c] = pd.json_normalize(capital_changes_df[c])

# capital_changes_df.to_excel("c:/users/damavandi.NOOREDENA/desktop/old desktop/capital_changes_df.xlsx", index=False)

capital_changes_df.drop(columns=["capital_change", "capital_change_percent", "is_contribution", "is_premium",
                                 "is_reserve", "contribution_percent", "premium_percent", "reserve_percent",
                                 "contribution_percentage", "is_capital_changed_increased", "is_capital_changed",
                                 "previous_capital_percent"], inplace=True)
insert_to_database(dataframe=capital_changes_df, database_table="[nooredenadb].[rahavard].[capital_changes]")

tmp_ = pd.DataFrame(capital_changes_df["comments"].str.len())

########################################################################################################################
########################################################################################################################
