import numpy as np
import pandas as pd
import math, time, warnings, jdatetime

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()
today = jdatetime.datetime.today().strftime('%Y/%m/%d')

####################################################################################

trades_tadbir_start = pd.read_sql("SELECT max(date) as date FROM [nooredenadb].[brokers].[trades]",
                                  db_conn)["date"].iloc[0]
trades_tadbir_start = jdatetime.datetime.strptime(trades_tadbir_start, "%Y/%m/%d").togregorian().strftime(
    "%Y-%m-%d")
trades_tadbir = pd.read_sql(f"SELECT * FROM [nooredenadb].[brokers].[trades_tadbir] WHERE "
                            f"TradeDate>='{trades_tadbir_start}'", db_conn)

if len(trades_tadbir) > 0:
    trades_tadbir["TradeSideTitle"].replace({"خرید": 1, "فروش": 2}, inplace=True)
    trades_tadbir["SymbolBoard"] = trades_tadbir["Symbol"].str[-1]
    trades_tadbir["Symbol"] = trades_tadbir["Symbol"].str[:-1]
    trades_tadbir["TradeDate"] = [jdatetime.datetime.fromgregorian(
        year=int(trades_tadbir["TradeDate"].iloc[i][:4]), month=int(trades_tadbir["TradeDate"].iloc[i][5:7]),
        day=int(trades_tadbir["TradeDate"].iloc[i][8:10])).strftime("%Y/%m/%d") for i in range(len(trades_tadbir))]
    trades_tadbir["is_ros"] = trades_tadbir["Symbol"].str[-1] == "ح"
    trades_tadbir["is_paid_ros"] = (trades_tadbir["is_ros"]) & (trades_tadbir["TradeSideTitle"] == 1) & \
                                   (((trades_tadbir["Volume"] * (trades_tadbir["Price"] + 1000)) +
                                     trades_tadbir["TotalCommision"] - trades_tadbir["NetPrice"]).abs() <
                                    ((trades_tadbir["Volume"] * trades_tadbir["Price"]) +
                                     trades_tadbir["TotalCommision"] - trades_tadbir["NetPrice"]).abs())
    trades_tadbir.drop(columns=["CdsSymbol", "TotalCommision", "TradeId", "TradeNumber", "VolumeInPrice"], inplace=True)
    if len(trades_tadbir) > 0:
        trades_tadbir['Symbol'] = trades_tadbir.apply(
            lambda row: row['Symbol'][:len(row["Symbol"]) + (row["is_paid_ros"] * -1)],
            axis=1)
    trades_tadbir["is_ros"] = trades_tadbir["is_ros"] ^ trades_tadbir["is_paid_ros"]
    trades_tadbir.rename({"TradeDate": "date", "Symbol": "symbol", "Volume": "volume", "TradeSideTitle": "type",
                          "Price": "price", "SymbolBoard": "board", "NetPrice": "value"}, axis=1, inplace=True)
else:
    trades_tadbir = pd.DataFrame()

####################################################################################

trades_rayan_start = pd.read_sql(
    "SELECT max(date) as date FROM [nooredenadb].[brokers].[trades]", db_conn)["date"].iloc[0]
trades_rayan = pd.read_sql(f"SELECT * FROM [nooredenadb].[brokers].[trades_rayan] where "
                           f"transactionDate>='{trades_rayan_start}'", db_conn)

if len(trades_rayan) > 0:
    trades_rayan.drop(columns=["row_", "branch", "fcKey", "branchId", "csTypeName", "rowOrder", "debtor", "creditor",
                               "remaining"], inplace=True, errors="ignore")
    trades_rayan = trades_rayan[~trades_rayan["comments"].str.split(expand=True)[0].isin(["تفاوت", "پذيره"])]
    trades_rayan['bourseAccount'] = trades_rayan.apply(
        lambda row: row['bourseAccount'][(row["bourseAccount"][0] == "ذ" * 1):], axis=1)
    trades_rayan["order_id"] = trades_rayan["comments"].str.extract(r'(\([0-9a-zA-Z]*\))')[0].str[1:-1]
    trades_rayan = trades_rayan[trades_rayan["csTypeId"].isin([1, 2])]
    trades_rayan["amount"] = trades_rayan["amount"].abs()
    trades_rayan.reset_index(drop=True, inplace=True)
    trades_rayan["SymbolBoard"] = [
        trades_rayan["order_id"].iloc[i][len(trades_rayan["insMaxLCode"].iloc[i]) - 1] if
        (trades_rayan["insMaxLCode"].iloc[i] is not None) else "1"
        for i in range(len(trades_rayan))]
    trades_rayan["is_ros"] = trades_rayan["comments"].str.contains("تقدم")
    trades_rayan["is_ros_payment"] = trades_rayan["comments"].str.contains("تقدم استفاده نشده")
    trades_rayan["comments"] = trades_rayan["comments"] * ~trades_rayan["is_ros_payment"]

    trades_rayan = trades_rayan.groupby(by=["transactionDate", "bourseAccount", "order_id", "qty", "price", "csTypeId",
                                            "broker_id", "SymbolBoard"], as_index=False).sum()

    trades_rayan["is_paid_ros"] = trades_rayan["is_ros"] == 2
    trades_rayan["is_ros"] = trades_rayan["is_ros"] == 1
    trades_rayan["is_ros_payment"] = trades_rayan["is_ros_payment"] == 1
    trades_rayan["tmp"] = "ح"
    trades_rayan["tmp"] = trades_rayan["is_ros"] * trades_rayan["tmp"]
    trades_rayan["bourseAccount"] = trades_rayan["bourseAccount"] + trades_rayan["tmp"]
    trades_rayan.drop(columns=["order_id", "comments", "insMaxLCode", "is_ros_payment", "tmp"], inplace=True)
    trades_rayan.rename({"transactionDate": "date", "bourseAccount": "symbol", "qty": "volume", "csTypeId": "type",
                         "SymbolBoard": "board", "amount": "value"}, axis=1, inplace=True)
else:
    trades_rayan = pd.DataFrame()

###########################################################################################

trades_df = pd.concat([trades_tadbir, trades_rayan], axis=0, ignore_index=True)
if len(trades_df) > 0:
    trades_df.replace({"is_ros": {True: 1, False: 0}, "is_paid_ros": {True: 1, False: 0}}, inplace=True, regex=False)
    trades_df["board"] = trades_df["board"].astype("int")
    trades_df.sort_values(["date", "broker_id", "type", "symbol"], ignore_index=True, inplace=True)
    trades_df["symbol"].replace({"ی": "ي", "ک": "ك"}, regex=True, inplace=True)
    try:
        crsr = db_conn.cursor()
        crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[trades] WHERE date >= '{trades_rayan_start}';")
        crsr.close()
        insert_to_database(dataframe=trades_df, database_table="[nooredenadb].[brokers].[trades]")
    except Exception as e:
        print(e)

###########################################################################################

query_portfolio = "SELECT * FROM [nooredenadb].[portfolio].[portfolio]"
portfolio = pd.read_sql(query_portfolio, db_conn)

trades_last = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[trades] where date=(SELECT max(date) FROM "
                          "[nooredenadb].[brokers].[trades]);", db_conn)
trades_last["board"].replace({0: 1}, inplace=True)
trades_last = trades_last.drop(columns=["broker_id", "price"], inplace=False).groupby(
    by=["date", "portfolio_id", "symbol", "is_ros", "is_paid_ros", "type", 'board'], as_index=False).sum()

trades_last["price"] = trades_last["value"] / trades_last["volume"]
trades_last["description"] = ["تقدم استفاده نشده - بلوکی" if (trades_last["is_paid_ros"].iloc[i] == 1) &
                                                             (trades_last["board"].iloc[i] > 1) else
                              "بلوکی" if (trades_last["board"].iloc[i] > 1) else "تقدم استفاده نشده" if
                              (trades_last["is_paid_ros"].iloc[i] == 1) else "" for i in range(len(trades_last))]

query_options = ("SELECT TEMP1.symbol, TEMP1.strike_price, TEMP2.date FROM (SELECT strike_price, end_date, call_symbol"
                 " AS symbol FROM [nooredenadb].[tsetmc].[options_data_today] union SELECT strike_price, end_date, "
                 "put_symbol AS symbol FROM [nooredenadb].[tsetmc].[options_data_today]) AS TEMP1 LEFT JOIN (SELECT "
                 "TRY_CONVERT(int, REPLACE(TRY_CONVERT(VARCHAR,Miladi), '-', '')) as end_date, Jalali_1 as date "
                 "FROM [nooredenadb].[extra].[dim_date]) AS TEMP2 ON TEMP1.end_date=TEMP2.end_date")
options = pd.read_sql(query_options, db_conn)
options["strike_price"] = options[["strike_price"]].applymap('{:,}'.format)
options["option_description"] = "(" + options["strike_price"] + ")" + " " + "(" + options["date"] + ")"
options = options[["symbol", "option_description"]]
trades_last = trades_last.merge(options, on="symbol", how="left")
trades_last["option_description"].fillna("", inplace=True)
trades_last["description"] = trades_last["description"] + trades_last["option_description"]
trades_last.drop(columns=["option_description"], inplace=True)

portfolio["cost_per_share"] = portfolio["total_cost"] / portfolio["amount"]
portfolio["cost_per_share_sep"] = portfolio["total_cost_sep"] / portfolio["amount"]

trades_last = trades_last.merge(portfolio[["portfolio_id", "symbol", "cost_per_share", "cost_per_share_sep"]],
                                on=["portfolio_id", "symbol"], how="left")
trades_last["total_cost"] = [
    np.nan if trades_last["type"].iloc[i] == 1 else np.nan if np.isnan(trades_last["cost_per_share"].iloc[i]) else
    math.ceil(trades_last["cost_per_share"].iloc[i] * trades_last["volume"].iloc[i]) for i in range(len(trades_last))]
trades_last["total_cost_sep"] = [
    np.nan if trades_last["type"].iloc[i] == 1 else np.nan if np.isnan(trades_last["cost_per_share_sep"].iloc[i]) else
    math.ceil(trades_last["cost_per_share_sep"].iloc[i] * trades_last["volume"].iloc[i]) for i in range(len(trades_last))]
trades_last.drop(labels=["cost_per_share", "cost_per_share_sep"], axis=1, inplace=True)

crsr = db_conn.cursor()
crsr.execute("TRUNCATE TABLE [nooredenadb].[brokers].[trades_last]")
crsr.close()

if trades_last["date"].iloc[0] == today:
    insert_to_database(dataframe=trades_last, database_table="[nooredenadb].[brokers].[trades_last]")

##########################################################################################

query_portfolio = "SELECT * FROM [nooredenadb].[portfolio].[portfolio]"
portfolio = pd.read_sql(query_portfolio, db_conn)

query_symbols_sector = "SELECT symbol, sector_name FROM [nooredenadb].[tsetmc].[symbols] WHERE active = 1"
symbols_sector = pd.read_sql(query_symbols_sector, db_conn)

query_trades_last = ("SELECT date, portfolio_id, symbol, type, SUM(volume) AS volume, SUM(value) AS value, "
                     "SUM(total_cost) AS total_cost, SUM(total_cost_sep) AS total_cost_sep FROM "
                     "[nooredenadb].[brokers].[trades_last] WHERE SUBSTRING(symbol, 1, 1) NOT IN ('ض', 'ط') "
                     "GROUP BY date, portfolio_id, symbol, type")
trades_last = pd.read_sql(query_trades_last, db_conn)

portfolio["sub_sector"].fillna("", inplace=True)
sub_sectors = portfolio[["symbol", "sub_sector"]].drop_duplicates(
    inplace=False, ignore_index=True).rename({"sub_sector": "subsector"}, axis=1, inplace=False)

if (len(trades_last) > 0) and (trades_last["date"].iloc[0] == today):

    trades_last["volume"] = trades_last["volume"] * (((trades_last["type"] == 1) * 2) - 1)
    trades_last["cost"] = trades_last["total_cost"].fillna(trades_last["value"], inplace=False)
    trades_last["cost"] = trades_last["cost"] * (((trades_last["type"] == 1) * 2) - 1)
    trades_last["cost_sep"] = trades_last["total_cost_sep"].fillna(trades_last["value"], inplace=False)
    trades_last["cost_sep"] = trades_last["cost_sep"] * (((trades_last["type"] == 1) * 2) - 1)

    trades_last.drop(labels=["date", "type", "total_cost", "total_cost_sep"], axis=1, inplace=True)
    trades_last = trades_last.merge(sub_sectors, on="symbol", how="left")
    trades_last = trades_last.merge(symbols_sector, on="symbol", how="left")
    trades_last = trades_last.fillna({"subsector": trades_last["sector_name"]}, inplace=False).drop(
        ["sector_name"], axis=1, inplace=False)
    trades_last = trades_last.groupby(by=["portfolio_id", "symbol", "subsector"], as_index=False).sum()

    portfolio_ = portfolio.merge(trades_last, on=["portfolio_id", "symbol"], how="outer")
    portfolio_.fillna({"sub_sector": portfolio_["subsector"]}, inplace=True)
    portfolio_.fillna(
        {"amount": 0, "total_cost": 0, "total_cost_sep": 0, "volume": 0, "cost": 0, "cost_sep": 0}, inplace=True)
    portfolio_["amount"] = portfolio_["amount"] + portfolio_["volume"]
    portfolio_["total_cost"] = portfolio_["total_cost"] + portfolio_["cost"]
    portfolio_["total_cost_sep"] = portfolio_["total_cost_sep"] + portfolio_["cost_sep"]
    portfolio_["date"] = today
    portfolio_ = portfolio_[["date", "symbol", "amount", "total_cost", "total_cost_sep", "sub_sector", "portfolio_id"]]
    if (portfolio_["amount"] < 0).sum() > 0:
        print("Some sold stocks doesn't exist in portfolio, please check and try again!")
        time.sleep(5)
        raise ValueError("Some sold stocks doesn't exist in portfolio, please check and try again!")

    portfolio_ = portfolio_[portfolio_["amount"] > 0].reset_index(drop=True, inplace=False)
    portfolio_["sub_sector"].replace({"": None}, inplace=True)

    crsr = db_conn.cursor()
    crsr.execute("TRUNCATE TABLE [nooredenadb].[portfolio].[portfolio_temp]")
    crsr.close()
    insert_to_database(dataframe=portfolio_, database_table="[nooredenadb].[portfolio].[portfolio_temp]")

else:
    pass

###########################################################################################

options_data = pd.read_sql("SELECT call_symbol AS symbol, contract_size, strike_price FROM "
                           "[nooredenadb].[tsetmc].[options_data_today] UNION SELECT put_symbol AS symbol, "
                           "contract_size, strike_price FROM [nooredenadb].[tsetmc].[options_data_today]", db_conn)
options_embedded_data = pd.read_sql("SELECT symbol, symbol_name strike_price, 1 contract_size FROM "
                                    "[nooredenadb].[tsetmc].[symbols_data_today] WHERE yval=600", db_conn)
options_embedded_data["strike_price"] = options_embedded_data["strike_price"].str.split("-", expand=True)[1].astype(int)
options_data = pd.concat([options_data, options_embedded_data], axis=0, ignore_index=True)

query_trades_last_options = ("SELECT date, portfolio_id, symbol, type, volume, value FROM "
                             "[nooredenadb].[brokers].[trades_last] WHERE symbol LIKE N'[هضط]%[0-9]'")
trades_last_options = pd.read_sql(query_trades_last_options, db_conn)
trades_last_options = trades_last_options.merge(options_data, on="symbol", how="left")
trades_last_options["volume"] = trades_last_options["volume"] / trades_last_options["contract_size"]

query_portfolio = "SELECT * FROM [nooredenadb].[portfolio].[portfolio_options]"
portfolio_options = pd.read_sql(query_portfolio, db_conn)
portfolio_options_ = portfolio_options.drop(labels=["date", "contract_size", "strike_price"], axis=1, inplace=False)

if (len(trades_last_options) > 0) and (trades_last_options["date"].iloc[0] == today):
    portfolio_options_ = portfolio_options_.merge(
        trades_last_options.drop(labels=["date", "contract_size", "strike_price"], axis=1, inplace=False),
        on=["portfolio_id", "symbol", "type"], how="outer")
    portfolio_options_.fillna({"amount": 0, "total_cost": 0, "volume": 0, "value": 0}, inplace=True)
    portfolio_options_["amount"] = portfolio_options_["amount"] + portfolio_options_["volume"]
    portfolio_options_["total_cost"] = portfolio_options_["total_cost"] + portfolio_options_["value"]
    portfolio_options_.drop(labels=["volume", "value"], axis=1, inplace=True)
    portfolio_options_["amount"] = portfolio_options_["amount"] * (((portfolio_options_["type"] == 1) * 2) - 1)
    portfolio_options_["total_cost"] = portfolio_options_["total_cost"] * (((portfolio_options_["type"] == 1) * 2) - 1)

    portfolio_options_ = portfolio_options_.groupby(by=["portfolio_id", "symbol"], as_index=False).sum()
    portfolio_options_ = portfolio_options_[portfolio_options_["amount"] != 0].reset_index(drop=True, inplace=False)
    portfolio_options_["type"] = ((portfolio_options_["amount"] < 0) * 1) + 1
    portfolio_options_["amount"] = portfolio_options_["amount"].abs()
    portfolio_options_["total_cost"] = portfolio_options_["total_cost"].abs()
    portfolio_options_["date"] = today
    portfolio_options_ = portfolio_options_[["date", "portfolio_id", "symbol", "type", "amount", "total_cost"]]
    portfolio_options_ = portfolio_options_.merge(options_data, on="symbol", how="left")

    crsr = db_conn.cursor()
    crsr.execute("TRUNCATE TABLE [nooredenadb].[portfolio].[portfolio_options_temp]")
    crsr.close()
    insert_to_database(dataframe=portfolio_options_, database_table="[nooredenadb].[portfolio].[portfolio_options_temp]")
else:
    portfolio_options_ = portfolio_options_.merge(options_data, on="symbol", how="left")
    portfolio_options_.dropna(subset=["strike_price"], inplace=True, ignore_index=True)
    portfolio_options_["date"] = today

    crsr = db_conn.cursor()
    crsr.execute("TRUNCATE TABLE [nooredenadb].[portfolio].[portfolio_options_temp]")
    crsr.close()
    insert_to_database(dataframe=portfolio_options_, database_table="[nooredenadb].[portfolio].[portfolio_options_temp]")


##########################################################################################

# # Historical data of  our CREDIT at each BROKER
# query_ = "SELECT t1.broker_id, t1.transactionDate , t1.row_, t1.remaining FROM [nooredenadb].[brokers].[trades_rayan]" \
#          " t1 JOIN (SELECT broker_id, transactionDate, MAX(row_) AS row_ FROM [nooredenadb].[brokers].[trades_rayan]" \
#          " GROUP BY broker_id, transactionDate) t2 ON t1.broker_id = t2.broker_id AND" \
#          " t1.transactionDate = t2.transactionDate AND t1.row_ = t2.row_ ORDER BY broker_id, transactionDate;"
# tmp = pd.read_sql(query_, db_conn)
# brokers = pd.read_sql("SELECT broker_name, broker_id FROM brokers.brokers", db_conn)
# tmp = tmp.merge(brokers, on="broker_id", how="left")
# # tmp_ = tmp[tmp["transactionDate"] == "1403/10/30"]
# tmp["year_month"] = tmp["transactionDate"].str[:7]
# tmp["day"] = tmp["transactionDate"].str[-2:]
# tmp_ = tmp[["broker_name", "year_month", "day"]].groupby(by=["broker_name", "year_month"], as_index=False
#                                                          ).max().sort_values(["broker_name", "year_month"],
#                                                                              inplace=False, ignore_index=True)
# tmp__ = tmp_.merge(
#     tmp[["broker_name", "year_month", "day", "remaining"]],
#     on=["broker_name", "year_month", "day"]
# )
# # tmp__ = tmp__[tmp__["year_month"] == "1403/10"].reset_index(drop=True, inplace=False)
# # tmp__["remaining"].sum()
# tmp__ = tmp__[tmp__["year_month"] >= "1402/10"].reset_index(drop=True, inplace=False)
# tmp___ = tmp__[["year_month", "remaining"]].groupby(by="year_month", as_index=False).sum()
