import warnings
import pandas as pd

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()


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

