import warnings
import pandas as pd

from utils.database import make_connection
from annual_meeting.utils.data import get_trades_value

warnings.filterwarnings("ignore")
db_conn = make_connection()

market_start_date = 20250120
portfolio_start_date = "1403/11/01"
portfolio_end_date = "1404/10/30"
query_symbols = ("SELECT market_trade_value, date FROM (SELECT date_, SUM(trade_value) as market_trade_value FROM "
                 "(SELECT date as date_, [symbol_id], [trade_value] FROM [nooredenadb].[tsetmc].[symbols_history] WHERE "
                 f"date>={market_start_date}) AS TEMP1 RIGHT JOIN (SELECT symbol_id FROM [nooredenadb].[tsetmc].[symbols] "
                 "WHERE sector!='68') AS TEMP2 ON TEMP1.symbol_id=TEMP2.symbol_id WHERE date_ IS NOT NULL GROUP BY "
                 "date_) AS TEMP3 LEFT JOIN (SELECT TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) AS date_, Jalali_1 "
                 "AS date FROM [nooredenadb].[extra].[dim_date]) AS TEMP4 "
                 "ON TEMP3.date_=TEMP4.date_")
query_symbols_ros = ("SELECT market_trade_value, date FROM (SELECT date as date_, SUM(trade_value) as market_trade_value"
                     f" FROM [nooredenadb].[tsetmc].[symbols_ros_history] WHERE date>={market_start_date} GROUP BY date) "
                     "AS TEMP1 LEFT JOIN (SELECT TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) AS date_, Jalali_1 AS "
                     "date FROM [nooredenadb].[extra].[dim_date]) AS TEMP2 "
                     "ON TEMP1.date_=TEMP2.date_")

symbols_trade_value = pd.read_sql(query_symbols, db_conn)
symbols_ros_trade_value = pd.read_sql(query_symbols_ros, db_conn)
market_trade_value = pd.concat([symbols_trade_value, symbols_ros_trade_value], axis=0, ignore_index=True)
market_trade_value = market_trade_value.groupby(by="date", as_index=False).sum()
market_trade_value = market_trade_value[market_trade_value["market_trade_value"] > 0].reset_index(
    drop=True, inplace=False)

portfolio_trade_value = get_trades_value(
    start_date=portfolio_start_date,
    end_date=portfolio_end_date)
portfolio_trade_value.rename({"value": "portfolio_trade_value"}, axis=1, inplace=True)

dataframe = market_trade_value.merge(portfolio_trade_value, on="date", how="outer")
dataframe = dataframe[(dataframe["date"] >= portfolio_start_date) & (dataframe["date"] <= portfolio_end_date)]

dataframe.fillna(0, inplace=True)
dataframe["market_trade_value"] /= 1e9
dataframe["portfolio_trade_value"] /= 1e9

dim_date = pd.read_sql("SELECT [Jalali_1] as date, [jyear], [JWeekNum] FROM [nooredenadb].[extra].[dim_date] "
                       , db_conn)
dim_date["jyear"] = dim_date["jyear"].astype(int)
dataframe_weekly = dataframe.copy()
dataframe_weekly = dataframe_weekly.merge(dim_date, on="date", how="left")
dataframe_weekly_ = dataframe_weekly.groupby(by=["jyear", "JWeekNum"], as_index=False).max()[["date", "jyear", "JWeekNum"]]
dataframe_weekly__ = dataframe_weekly.groupby(by=["jyear", "JWeekNum"], as_index=False).sum(numeric_only=True)[["jyear", "JWeekNum", "market_trade_value", "portfolio_trade_value"]]
dataframe_weekly = dataframe_weekly_.merge(dataframe_weekly__, on=["jyear", "JWeekNum"], how="left")
dataframe_weekly = dataframe_weekly[["date", "market_trade_value", "portfolio_trade_value"]]

dataframe_monthly = dataframe.copy()
dataframe_monthly["year-month"] = dataframe_monthly["date"].str[:7]
dataframe_monthly = dataframe_monthly[["year-month", "market_trade_value", "portfolio_trade_value"]].groupby(
    by="year-month", as_index=False).sum().rename({"year-month": "date"}, axis=1, inplace=False)

dataframe["ratio"] = dataframe["portfolio_trade_value"] / dataframe["market_trade_value"]
dataframe_weekly["ratio"] = dataframe_weekly["portfolio_trade_value"] / dataframe_weekly["market_trade_value"]
dataframe_monthly["ratio"] = dataframe_monthly["portfolio_trade_value"] / dataframe_monthly["market_trade_value"]

writer = pd.ExcelWriter(f"./annual_meeting/market_vs_portfolio_trade_value.xlsx", engine="xlsxwriter")
dataframe.to_excel(writer, sheet_name="daily", index=False)
dataframe_weekly.to_excel(writer, sheet_name="weekly", index=False)
dataframe_monthly.to_excel(writer, sheet_name="monthly", index=False)
writer.close()

