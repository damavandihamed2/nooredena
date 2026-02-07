import warnings
import pandas as pd

from utils.database import make_connection


start_day = "1402/10/30"
warnings.filterwarnings("ignore")
powerbi_database = make_connection()

##################################################

credits1 = pd.read_sql("SELECT trades_rayan.transactionDate as date, trades_rayan.remaining, "
                       "trades_rayan.broker_id FROM [nooredenadb].[brokers].[trades_rayan] INNER JOIN (SELECT max"
                       "(row_) as row__, transactionDate as date, broker_id FROM [nooredenadb].[brokers].[trades_rayan]"
                       " where transactionDate>='1402/10/30' group by transactionDate, broker_id) as temp ON row__=row_"
                       " ORDER BY date, broker_id", powerbi_database)
credits2 = pd.read_sql("SELECT temp2.broker_id, temp2.remaining, temp_date.date FROM (SELECT temp.row_, temp.broker_id,"
                       " temp.date_, trades_tadbir_ledger.Remain as remaining FROM (SELECT max(row_) as row_, "
                       "broker_id, substring(TransactionDate, 1, 10) as date_ FROM "
                       "[nooredenadb].[brokers].[trades_tadbir_ledger] where TransactionDate>='2024-01-20' and "
                       "Description not in ('سند اختتامیه', 'سند افتتاحیه مورخ {0}') group by substring(TransactionDate,"
                       " 1, 10), broker_id) as temp INNER JOIN [nooredenadb].[brokers].[trades_tadbir_ledger] ON "
                       "temp.row_=trades_tadbir_ledger.row_) as temp2 JOIN (SELECT Jalali_1 as date, "
                       "TRY_CONVERT(varchar, Miladi) date_ FROM [nooredenadb].[extra].[dim_date] WHERE "
                       "Jalali_1>='1402/10/30' AND Miladi<=(SELECT GETDATE())) as temp_date ON "
                       "temp2.date_=temp_date.date_ ORDER BY date", powerbi_database)

credits = pd.concat([credits1, credits2], axis=0, ignore_index=True)

credits_index = pd.MultiIndex.from_product(
    [credits["date"].unique(), credits["broker_id"].unique()], names=['date', 'broker_id'])
credits_df = pd.DataFrame(index=credits_index).reset_index(drop=False, inplace=False)
credits_df = credits_df.merge(credits, on=["date", "broker_id"], how="left").sort_values(
    by=["broker_id", "date"], inplace=False)
credits_df["remaining"] = credits_df.groupby("broker_id")["remaining"].ffill()
credits_df["remaining"] = (credits_df["remaining"] < 0) * credits_df["remaining"]
credits_df = credits_df[["date", "remaining"]].groupby(by="date", as_index=False).sum()
credits_df["remaining"] *= -1

##################################################

sigma_portfolio = pd.read_sql("SELECT date, sum(total_cost) as cost, sum(gross_value_final_price) as value "
                              "FROM [nooredenadb].[sigma].[sigma_portfolio] where date>='1402/10/30' and "
                              "type in ('صندوق', 'گواهی سپرده کالایی', 'حق تقدم', 'سهام') "
                              "group by date order by date", powerbi_database)
sigma_dividend = pd.read_sql("SELECT meeting_date as date, sum(value) as dividend FROM "
                             "[nooredenadb].[sigma].[sigma_dividend] where meeting_date >= '1402/10/30' and "
                             "value > 0 group by meeting_date order by meeting_date", powerbi_database)
sigma_profit = pd.read_sql("SELECT date, sum(net_profit) as profit FROM [nooredenadb].[sigma].[sigma_buysell] where "
                           "date >= '1402/10/30' and action='فروش' and type!='اختیار معامله' group by date "
                           "order by date", powerbi_database)

dataframe = sigma_portfolio.merge(sigma_dividend, on="date", how="outer")
dataframe = dataframe.merge(sigma_profit, on="date", how="outer")
dataframe = dataframe.merge(credits_df, on="date", how="outer")

dataframe["remaining"].ffill(inplace=True)
dataframe["remaining_yesterday"] = dataframe["remaining"].shift(1)
dataframe["value_yesterday"] = dataframe["value"].shift(1)
dataframe["cost_yesterday"] = dataframe["cost"].shift(1)
dataframe.fillna({"dividend": 0, "profit": 0}, inplace=True)
dataframe["portfo_return"] = ((dataframe["value"] + dataframe["dividend"] + dataframe["profit"] -
                               dataframe["cost"] - (dataframe["value_yesterday"] - dataframe["cost_yesterday"])
                               ) / dataframe["value_yesterday"]) + 1
dataframe["credit_profit"]  = (dataframe["portfo_return"] - 1) * dataframe["remaining_yesterday"]
dataframe.to_excel("c:/users/h.damavandi/desktop/dataframe.xlsx", index=False)
