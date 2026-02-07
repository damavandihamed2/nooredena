import numpy as np
import pandas as pd
import warnings, jdatetime

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()


query_portfolio_prx_last_date = ("SELECT max(date) AS date FROM [nooredenadb].[company].[portfolio_history]"
                                 " WHERE portfolio_id!=1")
portfolio_prx_last_date = pd.read_sql(query_portfolio_prx_last_date, db_conn)["date"].iloc[0]

query_portfolio_prx = (f"SELECT * FROM [nooredenadb].[company].[portfolio_history] "
                       f"WHERE portfolio_id!=1 AND date='{portfolio_prx_last_date}'")
portfolio_prx = pd.read_sql(query_portfolio_prx, db_conn)

query_trades = ("SELECT date, portfolio_id, symbol, type, is_ros, is_paid_ros, SUM(volume) AS volume, "
                "SUM(value) AS value FROM [nooredenadb].[brokers].[trades] WHERE portfolio_id != 1 AND "
                f"SUBSTRING(symbol, 1, 1) NOT IN ('ض', 'ط') AND date > '{portfolio_prx_last_date}' "
                "GROUP BY date, portfolio_id, symbol, type, is_ros, is_paid_ros ORDER BY date")
trades = pd.read_sql(query_trades, db_conn)

query_portfolio_transfers = "SELECT * FROM [nooredenadb].[company].[portfolio_transfers]"
portfolio_transfers = pd.read_sql(query_portfolio_transfers, db_conn)

portfolio_prx_history = pd.DataFrame(
    columns=["date", "portfolio_id", "symbol", "is_ros", "is_paid_ros", "volume", "total_cost"])
trades_prx = pd.DataFrame(columns=["date", "portfolio_id", "type", "symbol", "is_ros",
                                   "is_paid_ros", "volume", "value", "total_cost"])

one_day = jdatetime.timedelta(days=1)
start_date = portfolio_prx["date"].iloc[0]
start_date = jdatetime.datetime.strptime(start_date, '%Y/%m/%d')
end_date = jdatetime.datetime.today()


def adjust_changes(base_portfolio: pd.DataFrame, change_table: pd.DataFrame) -> pd.DataFrame:
    base_portfolio_tmp = base_portfolio.copy()
    base_portfolio_tmp = pd.concat([base_portfolio_tmp, change_table], axis=0, ignore_index=True)
    base_portfolio_tmp = base_portfolio_tmp.groupby(
        by=["portfolio_id", "symbol", "is_ros", "is_paid_ros"], as_index=False).sum()
    base_portfolio_tmp = base_portfolio_tmp[base_portfolio_tmp["volume"] > 0]
    return base_portfolio_tmp


current_date = start_date
while current_date <= end_date:

    portfolio_prx.drop(columns=["date"], inplace=True)

    portfolio_transfers_current_date = portfolio_transfers.loc[
        portfolio_transfers.date == current_date.strftime('%Y/%m/%d')].reset_index(drop=True)
    if len(portfolio_transfers_current_date) > 0:
        portfolio_transfers_current_date_sender = portfolio_transfers_current_date[
            ["symbol", "portfolio_id", "is_ros", "is_paid_ros", "volume", "total_cost"]]
        portfolio_transfers_current_date_sender[["volume", "total_cost"]] *= -1
        portfolio_transfers_current_date_reciever = portfolio_transfers_current_date[
            ["symbol", "portfolio_id_new", "is_ros", "is_paid_ros", "volume", "total_cost"]].rename(
            {"portfolio_id_new": "portfolio_id"}, axis=1, inplace=False)
        portfolio_transfers_current_date = pd.concat(
            [portfolio_transfers_current_date_sender, portfolio_transfers_current_date_reciever],
            axis=0, ignore_index=True)
        portfolio_prx_tmp = adjust_changes(base_portfolio=portfolio_prx, change_table=portfolio_transfers_current_date)
        portfolio_prx = portfolio_prx_tmp.copy()
    else:
        pass

    trades_current_date = trades.loc[trades.date == current_date.strftime('%Y/%m/%d')].reset_index(drop=True)
    if len(trades_current_date) > 0:

        portfolio_prx_tmp = portfolio_prx.copy()

        portfolio_prx_tmp["cost_per_share"] = portfolio_prx_tmp["total_cost"] / portfolio_prx_tmp["volume"]
        portfolio_prx_tmp = portfolio_prx_tmp[["portfolio_id", "symbol", "is_ros", "is_paid_ros", "cost_per_share"]]
        trades_current_date = trades_current_date.merge(
            portfolio_prx_tmp, on=["portfolio_id", "symbol", "is_ros", "is_paid_ros"], how="left")
        trades_current_date["cost_per_share"] = np.where(
            trades_current_date["type"] == 2, trades_current_date["cost_per_share"], np.nan)
        trades_current_date["total_cost"] = np.ceil(
            (trades_current_date["cost_per_share"] * trades_current_date["volume"]).fillna(np.nan))

        trades_current_date.drop(columns=["cost_per_share"], inplace=True)
        trades_prx = pd.concat([trades_prx, trades_current_date.fillna({"total_cost": 0})], axis=0, ignore_index=True)

        trades_current_date.fillna({"total_cost": trades_current_date["value"]}, inplace=True)
        trades_current_date["volume"] *= ((trades_current_date["type"] * -2) + 3)
        trades_current_date["total_cost"] *= ((trades_current_date["type"] * -2) + 3)
        trades_current_date.drop(columns=["date", "type", "value"], inplace=True)

        portfolio_prx_tmp = adjust_changes(base_portfolio=portfolio_prx, change_table=trades_current_date)
        portfolio_prx = portfolio_prx_tmp.copy()

    else:
        pass

    portfolio_prx["date"] = current_date.strftime('%Y/%m/%d')
    portfolio_prx_history = pd.concat([portfolio_prx_history, portfolio_prx], axis=0, ignore_index=True)
    current_date += one_day

####################################################################################################

portfolio_prx_history["date_"] = portfolio_prx_history["date"].str[:7]
portfolio_prx_history_ = portfolio_prx_history[["date_", "date"]].groupby(
    by=["date_"], as_index=False).max().drop(columns=["date_"])
portfolio_prx_history_ = portfolio_prx_history.drop(columns=["date_"]).merge(
    portfolio_prx_history_, on=["date"], how="right")
portfolio_prx_history_ = portfolio_prx_history_[(portfolio_prx_history_["date"].str[:7] != end_date.strftime("%Y/%m"))
                                                & (portfolio_prx_history_["date"] != portfolio_prx_last_date)]

####################################################################################################

query_portfolio_last_date = ("SELECT max(date) AS date FROM [nooredenadb].[company].[portfolio_history]"
                                 " WHERE portfolio_id=1")
portfolio_last_date = pd.read_sql(query_portfolio_last_date, db_conn)["date"].iloc[0]


query_portfolio_history = ("WITH LastDates AS (SELECT MAX([date]) last_date FROM [nooredenadb].[sigma].[portfolio] "
                           "GROUP BY LEFT([date],7)) SELECT p.date,p.portfolio_id,p.symbol,p.type,p.status,p.amount,"
                           "p.total_cost,p.final_price FROM [nooredenadb].[sigma].[portfolio] p JOIN LastDates AS ld "
                           "ON p.[date]=ld.last_date WHERE SUBSTRING(p.[symbol],1,1) NOT IN ('ض', 'ط')")
portfolio_history = pd.read_sql(query_portfolio_history, db_conn)
# portfolio_history["symbol"].replace("همراه اول(غیر بورسی) ", "همراه", regex=False, inplace=True)

# portfolio_history = portfolio_history[portfolio_history["date"] > portfolio_last_date]
# portfolio_history = portfolio_history[portfolio_history["date"].str[:7] != end_date.strftime("%Y/%m")]

portfolio_history["symbol"].replace({"دارایکم": "دارا يكم"}, inplace=True)
portfolio_history["symbol"].replace({"ک": "ك", "ی": "ي"}, regex=True, inplace=True)
portfolio_history["is_ros"] = (portfolio_history["type"] == "حق تقدم") * 1
portfolio_history["is_paid_ros"] = (portfolio_history["status"] == "حق\\u200cتقدم پذیره نویسی شده") * 1
portfolio_history = portfolio_history[
    ["date", "portfolio_id", "symbol", 'amount', 'total_cost', 'is_ros', 'is_paid_ros', "final_price"]].rename(
    columns={"amount": "volume"}, inplace=False)
portfolio_history = portfolio_history.groupby(
    by=["date", "portfolio_id", "symbol", "is_ros", "is_paid_ros", "final_price"], as_index=False).sum()


portfolio_history_ = portfolio_history[portfolio_history["date"] == "1403/06/31"].sort_values(
    by=["symbol", 'is_ros', 'is_paid_ros', "final_price"], inplace=False, ignore_index=True)

# insert_to_database(dataframe=portfolio_history, database_table="[nooredenadb].[company].[portfolio_history]")

####################################################################################################

portfolio_history_total = pd.concat([portfolio_history, portfolio_prx_history_], axis=0, ignore_index=True)
if len(portfolio_history_total) > 0:
    insert_to_database(
        dataframe=portfolio_history_total, database_table="[nooredenadb].[company].[portfolio_history]")

####################################################################################################

query_trades_buy = ("SELECT [date],[portfolio_id],[symbol],[action],[type],[amount],[net_value],[total_cost] FROM"
                " [nooredenadb].[sigma].[buysell] WHERE action = 'خرید' AND "
                "type NOT IN ('اختیار تبعی فروش', 'اختیار معامله') ORDER BY date")
trades_buy = pd.read_sql(query_trades_buy, db_conn)
trades_buy["symbol"].replace({"دارایکم": "دارا يكم"}, inplace=True)
trades_buy["symbol"].replace({"ک": "ك", "ی": "ي"}, regex=True, inplace=True)
trades_buy["type"] = 1
trades_buy["is_ros"] = (trades_buy["type"] == 'حق تقدم') * 1
trades_buy["is_paid_ros"] = 0
trades_buy = trades_buy[
    ['date', 'portfolio_id', 'symbol', 'type', 'is_ros', 'is_paid_ros', 'amount', 'net_value', 'total_cost']].rename(
    {'amount': "volume", 'net_value': "value"}, axis=1, inplace=False)

####################################################################################################

query_trades_sell = ("SELECT [date],[portfolio_id],[asset],[type],[amount],[net_value],[total_cost] FROM "
                     "[nooredenadb].[sigma].[profitloss] where type != 'اختیار معامله'  AND amount != 0 ORDER BY date")
trades_sell = pd.read_sql(query_trades_sell, db_conn)
trades_sell["is_ros"] = (trades_sell["type"] == "حق تقدم") * 1
trades_sell["is_paid_ros"] = 0
trades_sell["type"] = ((trades_sell["amount"] < 0) * 1) + ((trades_sell["amount"] > 0) * 2)
trades_sell["amount"] = trades_sell["amount"].abs()
trades_sell["net_value"] = trades_sell["net_value"].abs()
trades_sell["total_cost"] = ((trades_sell["type"] == 2) * trades_sell["total_cost"])
trades_sell['symbol'] = trades_sell['asset'].str.extract(r'\(([^()]*)\)\s*$')
trades_sell["symbol"].replace({"دارایکم": "دارا يكم"}, inplace=True)
trades_sell["symbol"].replace({"ک": "ك", "ی": "ي"}, regex=True, inplace=True)
trades_sell = trades_sell[
    ['date', 'portfolio_id', 'symbol', 'is_ros', 'is_paid_ros', 'type', 'amount', 'net_value', 'total_cost']].rename(
    {"amount": "volume", 'net_value': "value"}, axis=1, inplace=False)

trades = pd.concat([trades_buy, trades_sell], axis=0, ignore_index=True)

trades_last_date = pd.read_sql("SELECT MAX(date) date FROM [nooredenadb].[company].[trades] "
                               "WHERE portfolio_id=1", db_conn)["date"].iloc[0]
trades = trades[trades["date"] > trades_last_date].reset_index(drop=True)
if len(trades) > 0:
    insert_to_database(dataframe=trades, database_table="[nooredenadb].[company].[trades]")

####################################################################################################

trades_prx_last_date = pd.read_sql("SELECT MAX(date) date FROM [nooredenadb].[company].[trades] "
                               "WHERE portfolio_id!=1", db_conn)["date"].iloc[0]
trades_prx = trades_prx[trades_prx["date"] > trades_prx_last_date].reset_index(drop=True)
if len(trades_prx) > 0:
    insert_to_database(dataframe=trades_prx, database_table="[nooredenadb].[trades]")

####################################################################################################

dividend = pd.read_sql("SELECT portfolio_id, asset, meeting_date AS date, eps, dps, amount AS volume, value "
                       "FROM [nooredenadb].[sigma].[dividend] WHERE value > 0 ORDER BY meeting_date", db_conn)

dividend["asset"].replace({"همراه اول(غیر بورسی) ": "ارتباطات سیار (همراه)"}, inplace=True, regex=False)
dividend['symbol'] = dividend['asset'].str.extract(r'\(([^()]*)\)\s*$')
dividend = dividend[['date', 'portfolio_id', 'symbol', 'eps', 'dps', 'volume', 'value']]

dividend_last_date = pd.read_sql("SELECT MAX(date) AS date FROM [nooredenadb].[company].[dividend]",
                                 db_conn)["date"].iloc[0]
dividend = dividend[dividend["date"] > dividend_last_date].reset_index(drop=True)
if len(dividend) > 0:
    insert_to_database(dataframe=dividend, database_table="[nooredenadb].[company].[dividend]")

