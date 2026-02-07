import pandas as pd
from pathlib import Path
import os, sys, warnings, jdatetime, subprocess

from utils.custom_logger import get_logger
from utils.database import make_connection, insert_to_database

from brokers.utils.date_parser import get_last_date
from brokers.utils.funcs import safe_login, safe_call_func
from brokers.omex.omex import OmexAgent
from brokers.online_plus.online_plus import OnlinePlusAgent


db_conn = make_connection()
warnings.filterwarnings("ignore")
try:
    filename = os.path.basename(__file__)
except:
    filename = "brokers_trades.py"
logger = get_logger(db_connection=db_conn, file_name=filename,
                    project_dir=os.path.dirname(os.path.abspath(sys.argv[0])))

brokers_list = pd.read_sql("SELECT bc.broker_id, bc.portfolio_id, bc.username, bc.password, b.type_online, "
                           "b.address_online FROM [nooredenadb].[brokers].[brokers_credential] bc LEFT JOIN "
                           "nooredenadb.brokers.brokers b ON bc.broker_id = b.broker_id WHERE bc.system_type=1 "
                           "AND bc.active=1 AND bc.option_active=1", db_conn)

today = jdatetime.date.today().strftime("%Y/%m/%d")
last_date_omex = get_last_date(broker_type="omex", db_conn=db_conn, db_style=False)
last_date_online_plus = get_last_date(broker_type="online_plus", db_conn=db_conn, db_style=False)

option_portfolio_omex_df = pd.DataFrame()
option_settlements_omex_df = pd.DataFrame()
option_portfolio_online_plus_df = pd.DataFrame()
option_settlements_online_plus_df = pd.DataFrame()

for b in range(len(brokers_list)):

    broker_type = brokers_list['type_online'].iloc[b]
    portfolio_id = int(brokers_list['portfolio_id'].iloc[b])

    if broker_type == "omex":
        agent = safe_login(
            broker_class=OmexAgent, address=brokers_list["address_online"].iloc[b], portfolio_id=portfolio_id,
            username=brokers_list["username"].iloc[b], password=brokers_list["password"].iloc[b])
        safe_call_func(agent.get_option_portfolio)
        safe_call_func(agent.get_option_settlements, start_date=last_date_omex, end_date=today)
        if agent.option_portfolio:
            options_df = pd.DataFrame(agent.option_portfolio)
            options_df["portfolio_id"] = brokers_list['portfolio_id'].iloc[b]
            options_df["broker_id"] = brokers_list['broker_id'].iloc[b]
            option_portfolio_omex_df = pd.concat([option_portfolio_omex_df, options_df], axis=0, ignore_index=True)
        if agent.option_settlements:
            settlements_df = pd.DataFrame(agent.option_settlements)
            settlements_df["portfolio_id"] = brokers_list['portfolio_id'].iloc[b]
            settlements_df["broker_id"] = brokers_list['broker_id'].iloc[b]
            option_settlements_omex_df = pd.concat([option_settlements_omex_df, settlements_df], axis=0, ignore_index=True)

    elif broker_type == "online_plus":
        agent = safe_login(
            broker_class=OnlinePlusAgent, address=brokers_list["address_online"].iloc[b], portfolio_id=portfolio_id,
            username=brokers_list["username"].iloc[b], password=brokers_list["password"].iloc[b])
        safe_call_func(agent.get_option_portfolio)
        safe_call_func(agent.get_option_settlements, start_data=last_date_online_plus, end_date=today)
        if agent.option_portfolio:
            option_portfolio = agent.option_portfolio
            options_df = pd.DataFrame(option_portfolio)
            options_df["portfolio_id"] = brokers_list['portfolio_id'].iloc[b]
            options_df["broker_id"] = brokers_list['broker_id'].iloc[b]
            option_portfolio_online_plus_df = pd.concat(
                [option_portfolio_online_plus_df, options_df], axis=0, ignore_index=True)
        if agent.option_settlements:
            option_settlements = agent.option_settlements["Data"]
            option_settlements_df = pd.DataFrame(option_settlements)
            option_settlements_df["portfolio_id"] = brokers_list['portfolio_id'].iloc[b]
            option_settlements_df["broker_id"] = brokers_list['broker_id'].iloc[b]
            option_settlements_online_plus_df = pd.concat(
                [option_settlements_online_plus_df, option_settlements_df], axis=0, ignore_index=True)

    else:
        raise ValueError(f"There isn't any Agent defined for type_online = {broker_type}, please check it out.")



#################### Updating Option Portfolio ####################

if not option_portfolio_omex_df.empty:
    crsr = db_conn.cursor()
    crsr.execute(f"TRUNCATE TABLE [nooredenadb].[brokers].[option_portfolio_omex]")
    crsr.close()
    insert_to_database(dataframe=option_portfolio_omex_df,
                       database_table="[nooredenadb].[brokers].[option_portfolio_omex]")

if not option_portfolio_online_plus_df.empty:
    crsr = db_conn.cursor()
    crsr.execute(f"TRUNCATE TABLE [nooredenadb].[brokers].[option_portfolio_online_plus]")
    crsr.close()
    insert_to_database(dataframe=option_portfolio_online_plus_df,
                       database_table="[nooredenadb].[brokers].[option_portfolio_online_plus]")

#################### Updating Option Settlements ####################

if not option_settlements_omex_df.empty:
    option_settlements_omex_df = option_settlements_omex_df.replace(
        {"rMaximum": {True: 1, False: 0}, "rFraction": {True: 1, False: 0}, "rlp": {True: 1, False: 0}}, inplace=False)
    option_settlements_omex_df.drop(columns=['firstName', 'lastName', 'nationalCode', "customerId"], inplace=True)

    d1 = get_last_date(broker_type="omex", db_conn=db_conn, db_style=True)
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[option_settlements_omex] WHERE eDate>='{d1}'")
    crsr.close()

    insert_to_database(dataframe=option_settlements_omex_df,
                       database_table="[nooredenadb].[brokers].[option_settlements_omex]")


if not option_settlements_online_plus_df.empty:
    option_settlements_online_plus_df.drop(columns=["CustomerId"], inplace=True)
    option_settlements_online_plus_df["SettlementDate"] = option_settlements_online_plus_df["SettlementDate"].str.strip()

    d2 = get_last_date(broker_type="online_plus", db_conn=db_conn, db_style=True)
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[option_settlements_online_plus] WHERE SettlementDate>='{d2}'")
    crsr.close()

    insert_to_database(dataframe=option_settlements_online_plus_df,
                       database_table="[nooredenadb].[brokers].[option_settlements_online_plus]")

###########################################################################

project_path = str(Path(__name__).resolve().parent)
subprocess.run([sys.executable, "-m", "brokers.options_cleaning"], cwd=project_path)
