import pandas as pd
from tqdm import tqdm
from time import sleep
import random, warnings

from rahavard.rahavard365 import Agent, Asset
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()

rahavard_symbols = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[symbols]", db_conn)

agent = Agent(username="09372377126", password="Dh74@123456")

dps_df = pd.DataFrame()
dps_fund_df = pd.DataFrame()
capital_changes_df = pd.DataFrame()
for i in tqdm(range(len(rahavard_symbols))):
    asset_id = rahavard_symbols["asset_id"].iloc[i]
    if rahavard_symbols["type"].iloc[i] == "stock":
        asset = Asset(asset_id=asset_id, agent=agent)
        sleep(random.randint(50, 151) / 100)
        asset.get_dps()
        sleep(random.randint(50, 151) / 100)
        asset.get_capital()
        if asset.dps is not None:
            dps_temp = asset.dps
            dps_temp["asset_id"] = asset_id
            dps_df = pd.concat([dps_df, dps_temp], axis=0, ignore_index=True)
        if asset.capital_changes is not None:
            capital_changes_temp = asset.capital_changes
            capital_changes_temp["asset_id"] = asset_id
            capital_changes_df = pd.concat([capital_changes_df, capital_changes_temp], axis=0, ignore_index=True)
    elif rahavard_symbols["type"].iloc[i] == "fund":
        asset = Asset(asset_id=asset_id, agent=agent)
        sleep(random.randint(50, 151) / 100)
        asset.get_fund_dps()
        if (asset.fund_dps is not None) and (len(asset.fund_dps) > 0):
            dps_temp = asset.fund_dps
            dps_temp["asset_id"] = asset_id
            dps_fund_df = pd.concat([dps_fund_df, dps_temp], axis=0, ignore_index=True)
    else:
        pass
    sleep(random.randint(50, 151) / 100)

########################################################################################################################

crsr = db_conn.cursor()
crsr.execute("Truncate TABLE [nooredenadb].[rahavard].[dps_funds]")
crsr.close()
insert_to_database(dataframe=dps_fund_df, database_table="[nooredenadb].[rahavard].[dps_funds]")

########################################################################################################################

dps_df = dps_df[~((dps_df["asset_id"] == "604") & (dps_df["date_time"] == "2025-03-03T00:00:00+03:30"))]
crsr = db_conn.cursor()
crsr.execute("Truncate TABLE [nooredenadb].[rahavard].[dps]")
crsr.close()
insert_to_database(dataframe=dps_df, database_table="[nooredenadb].[rahavard].[dps]")

########################################################################################################################

for c in ["company", "report", "meeting", "underwriting_end_report", "registration_report",
          "stock_certificate_receive_report"]:
    capital_changes_df[c] = pd.json_normalize(capital_changes_df[c])
capital_changes_df.drop(columns=["capital_change", "capital_change_percent", "is_contribution", "is_premium",
                                 "is_reserve", "contribution_percent", "premium_percent", "reserve_percent",
                                 "contribution_percentage", "is_capital_changed_increased", "is_capital_changed",
                                 "previous_capital_percent"], inplace=True)
crsr = db_conn.cursor()
crsr.execute("Truncate TABLE [nooredenadb].[rahavard].[capital_changes]")
crsr.close()
insert_to_database(dataframe=capital_changes_df, database_table="[nooredenadb].[rahavard].[capital_changes]")

########################################################################################################################
