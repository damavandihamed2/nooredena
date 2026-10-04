import math
import warnings
import jdatetime
import numpy as np
import pandas as pd

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()


today = jdatetime.datetime.today()
trades = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[ime_trades_coinonline] WHERE status='انجام شده'", db_conn)
trades = trades[["date", "symbol", "trade_type", "price", "volume", "portfolio_id", "broker_id"]]
trades["trade_type"].replace({"خرید": 1, "فروش": 2}, inplace=True)
trades_df = trades.groupby(["date", "symbol", "trade_type", "portfolio_id", "broker_id"], as_index=False).sum()

##################################################

statements = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[ime_statements_coinonline]", db_conn)

statements_value = statements[statements["description"] == "مبلغ معامله"]
statements_value["trade_type"] = ((statements_value["creditor"] > 0) * 1) + 1
statements_value["value"] = statements_value["debtor"] + statements_value["creditor"]
statements_value = statements_value[["date", "document_id", "symbol", "trade_type", "value", "portfolio_id", "broker_id"]]

statements_commission_broker = statements[statements["description"] == "کارمزد کارگزار جهت انجام معامله"]
statements_commission_broker = statements_commission_broker.rename({"debtor": "commission_broker"}, axis=1, inplace=False)
statements_commission_broker["document_id"] -= 1

statements_commission_ime = statements[statements["description"] == "کارمزد بورس کالا"]
statements_commission_ime = statements_commission_ime.rename({"debtor": "commission_ime"}, axis=1, inplace=False)
statements_commission_ime["document_id"] -= 2

statements_commission_total = statements_commission_ime.merge(
    statements_commission_broker, how="outer", on=["document_id", "portfolio_id", "broker_id"])
statements_commission_total["commission"] = statements_commission_total["commission_ime"]+ statements_commission_total["commission_broker"]
statements_commission_total = statements_commission_total[["document_id", "commission", "portfolio_id", "broker_id"]]

statements_df = statements_value.merge(statements_commission_total, on=["document_id", "portfolio_id", "broker_id"], how="outer")
statements_df = statements_df.drop("document_id", axis=1, inplace=False).groupby(["date", "symbol", "trade_type", "portfolio_id", "broker_id"], as_index=False).sum()

##################################################

df = trades_df.merge(statements_df, on=['date', 'symbol', 'trade_type', 'portfolio_id', 'broker_id'], how="outer")
df.rename({"trade_type": "type"}, axis=1, inplace=True)
df.drop("price", axis=1, inplace=True)

last_date = pd.read_sql("SELECT MAX(date) FROM [nooredenadb].[brokers].[trades_ime]", db_conn)[""].iloc[0]
df = df[df["date"] >= last_date].reset_index(drop=True)
if not df.empty:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[trades_ime] WHERE date >= '{last_date}'")
    insert_to_database(df, "[nooredenadb].[brokers].[trades_ime]")

##################################################

CDCs = pd.read_sql("SELECT ContractCode FROM [nooredenadb].[ime].[live_tablo_cdc]", db_conn)
CDCs = CDCs["ContractCode"].values.tolist()

portfolio_ime = pd.read_sql(f"SELECT * FROM [nooredenadb].[portfolio].[portfolio_ime]", db_conn)
last_date = portfolio_ime["date"].iloc[0]

trades = pd.read_sql(
    f"SELECT * FROM [nooredenadb].[brokers].[trades_ime] WHERE date > '{last_date}' AND "
    f"symbol IN (SELECT ContractCode FROM nooredenadb.ime.live_tablo_cdc)",
    db_conn
)

if not trades.empty:

    trades["value"] = trades["value"] + trades["commission"]
    trades.drop(columns=["commission", "broker_id"], inplace=True)
    trades = trades.groupby(by=["date", "portfolio_id", "symbol", "type"], as_index=False).sum()
    trades["price"] = trades["value"] / trades["volume"]

    portfolio_ime["cost_per_share"] = portfolio_ime["total_cost"] / portfolio_ime["amount"]
    trades = trades.merge(portfolio_ime[["symbol", "portfolio_id", "cost_per_share"]], on=["symbol", "portfolio_id"], how="left")

    trades["total_cost"] = [np.nan if trades["type"].iloc[i] == 1 else
                            np.nan if np.isnan(trades["cost_per_share"].iloc[i]) else
                            math.ceil(trades["volume"].iloc[i] * trades["cost_per_share"].iloc[i])
                            for i in range(len(trades))]
    trades = trades[["date", "portfolio_id", "symbol", "type", "volume", "value", "price", "total_cost"]]

    crsr = db_conn.cursor()
    crsr.execute("TRUNCATE TABLE [nooredenadb].[brokers].[trades_last_ime]")
    crsr.close()

    if trades["date"].iloc[0] == today.strftime("%Y/%m/%d"):
        insert_to_database(dataframe=trades, database_table="[nooredenadb].[brokers].[trades_last_ime]")

##################################################

trades_last = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[trades_last_ime]", db_conn)
portfolio_ime = pd.read_sql(f"SELECT * FROM [nooredenadb].[portfolio].[portfolio_ime]", db_conn)

if (not trades_last.empty) and (trades_last["date"].iloc[0] == today.strftime("%Y/%m/%d")):

    trades_last["volume"] = trades_last["volume"] * ((trades_last["type"] * -2) + 3)
    trades_last["cost"] = trades_last["total_cost"].fillna(trades_last["value"], inplace=False) * ((trades_last["type"] * -2) + 3)

    portfolio_ime_ = portfolio_ime.merge(
        trades_last[["symbol", "portfolio_id", "volume", "cost"]], on=["symbol", "portfolio_id"], how="outer")
    portfolio_ime_.fillna({"amount": 0, "total_cost": 0, "volume": 0, "cost": 0}, inplace=True)

    portfolio_ime_["amount"] = portfolio_ime_["amount"] + portfolio_ime_["volume"]
    portfolio_ime_["total_cost"] = portfolio_ime_["total_cost"] + portfolio_ime_["cost"]
    portfolio_ime_["date"] = today.strftime("%Y/%m/%d")

    portfolio_ime_ = portfolio_ime_[["date", "symbol", "amount", "total_cost", "portfolio_id"]]

    if (portfolio_ime_["amount"] < 0).sum() > 0:
        raise ValueError("Some sold CDCs doesn't exist in portfolio, please check and try again!")

    crsr = db_conn.cursor()
    crsr.execute("TRUNCATE TABLE [nooredenadb].[portfolio].[portfolio_ime_temp]")
    crsr.close()
    insert_to_database(dataframe=portfolio_ime_, database_table="[nooredenadb].[portfolio].[portfolio_ime_temp]")
