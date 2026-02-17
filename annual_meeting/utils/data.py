import pandas as pd
import warnings, jdatetime
from typing import Literal, Optional

from utils import database


warnings.filterwarnings("ignore")
db_conn = database.make_connection()


def add_one_day_j(date: str):
    return (jdatetime.datetime.strptime(date, "%Y/%m/%d") + jdatetime.timedelta(days=1)).strftime("%Y/%m/%d")

def get_last_board_meeting_date():
    last_date = pd.read_sql("SELECT MAX(date) AS date FROM [nooredenadb].[extra].[board_meeting_dates]", db_conn)
    last_date = last_date["date"].iloc[0]
    return last_date


def get_last_trade_date():
    last_date = pd.read_sql("SELECT TOP(1) date FROM [nooredenadb].[brokers].[trades_last]", db_conn)
    if last_date.empty:
        last_date = pd.read_sql("SELECT MAX(date) as date FROM [nooredenadb].[company].[trades]", db_conn)
    last_date = last_date["date"].iloc[0]
    return last_date


def check_basket(l: set):
    check_1, check_2 =  1 in l, bool(l & {25, 26, 27, 28})
    if check_1 and check_2: return "هر دو"
    elif check_1: return "اصلی"
    elif check_2: return "prx"
    else: return "نامشخص"


def get_avalhami_data(date: str = "last") -> pd.DataFrame:
    query_avalhami_raw = ("SELECT [JalaliDate] date, [SellNAVPerShare] final_price, ISNULL(TEMP.funds_unit, 0) amount, "
                          "ISNULL(TEMP.cost, 0) total_cost FROM [nooredenadb].[extra].[avalhami_nav] LEFT JOIN (SELECT date,"
                          " SUM((CASE WHEN type=1 THEN funds_unit ELSE funds_unit * -1 END)) funds_unit, SUM((CASE WHEN "
                          "type=1 THEN value ELSE cost * -1 END)) AS cost FROM [nooredenadb].[extra].[avalhami_trades] "
                          "GROUP BY date) AS TEMP ON avalhami_nav.JalaliDate=TEMP.date ORDER by avalhami_nav.JalaliDate")
    avalhami_df = pd.read_sql(query_avalhami_raw, db_conn)
    if date != "last":
        avalhami_df = avalhami_df[avalhami_df["date"] <= date].reset_index(drop=True)
    avalhami_df = pd.DataFrame([{
        "portfolio_id": 1, "symbol": "حامی اول", "amount": avalhami_df["amount"].sum(),
        "total_cost": avalhami_df["total_cost"].sum(), "total_cost_sep": avalhami_df["total_cost"].sum(),
        "final_price": avalhami_df["final_price"].iloc[-1]}])
    return avalhami_df


def portfolio_date_time() -> (str, str):
    q_ = "SELECT TOP(1) date, time FROM [nooredenadb].[portfolio].[portfolio_daily_report_diff] WHERE date IS NOt NULL"
    date_time = pd.read_sql(q_, db_conn)
    date, time = date_time["date"].iloc[0].replace('-', '/'), date_time["time"].iloc[0]
    return date, time


def get_symbols_data() -> pd.DataFrame:
    query_symbols = ("select symbol, total_share, sector_name from [nooredenadb].[tsetmc].[symbols]"
                     " ORDER BY symbol, active, final_last_date")
    symbols = pd.read_sql(query_symbols, db_conn)
    symbols.drop_duplicates(subset="symbol", keep="last", ignore_index=True, inplace=True)
    return symbols


def get_fix_income_symbols() -> pd.DataFrame:
    query_fix_income_symbols = "SELECT [symbol] FROM [nooredenadb].[tsetmc].[symbols] WHERE subsector='6812'"
    fix_income_symbols = pd.read_sql(query_fix_income_symbols, db_conn)
    return fix_income_symbols

########################################################################################################################


def get_trades_value(
        start_date: str,
        end_date: str,
        trade_type: Optional[Literal[1, 2]] = None,
        include_options: bool = False,
        include_fix_funds: bool = False,
        main_portfolio: bool = True,
        prx_portfolio: bool = True

) -> dict[str: pd.DataFrame]:

    if not((trade_type in [1, 2]) or (trade_type is None)):
        raise ValueError("trade_type must be either 1 or 2 or None")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    query_trades = ("SELECT date, portfolio_id, symbol, type, SUM(value) value FROM [nooredenadb].[brokers].[trades] "
                    f"WHERE date >= '{start_date}' AND date <= '{end_date}' GROUP BY date, portfolio_id, symbol, type")
    trades = pd.read_sql(query_trades, db_conn)
    if not include_options:
        trades = trades[~trades["symbol"].str[:1].isin(["ض", "ط"])]
    if not include_fix_funds:
        trades = trades[~trades["symbol"].isin(get_fix_income_symbols()["symbol"])]
    if main_portfolio and (not prx_portfolio):
        trades = trades[trades["portfolio_id"] == 1]
    if (not main_portfolio) and prx_portfolio:
        trades = trades[trades["portfolio_id"] != 1]
    if trade_type is not None:
        trades = trades[trades["type"] == trade_type]
    trades = trades[["date", "value"]].groupby(by=["date"], as_index=False).sum()
    trades.sort_values(by="value", ascending=True, inplace=True, ignore_index=True)
    return trades


def pie_chart_data(trades_df: pd.DataFrame, agg_percent: float = 0.01) -> pd.DataFrame:
    trades_df = trades_df[["symbol", "sector_name", "volume", "value", "share_percent"]].groupby(
        by=["symbol", "sector_name"], as_index=False).sum()
    trades_df["mean_price"] = round(trades_df["value"] / trades_df["volume"]).astype(int)
    trades_pie_small = trades_df[trades_df["share_percent"] < agg_percent]
    if len(trades_pie_small) > 0:
        trades_small_total = pd.DataFrame([{
            "symbol": "سایر", "volume": trades_pie_small["volume"].sum(), "value": trades_pie_small["value"].sum(),
            "share_percent": trades_pie_small["share_percent"].sum()}])
        pie_chart_df = pd.concat(
            [trades_df[trades_df["share_percent"] >= 0.01], trades_small_total], axis=0, ignore_index=True)
    else:
        pie_chart_df = trades_df
    return pie_chart_df

########################################################################################################################


def get_avalhami_history() -> pd.DataFrame:
    q_ = ("SELECT JalaliDate date, SellNAVPerShare final_price, ISNULL(TEMP.funds_unit,0) amount,ISNULL(TEMP.cost, 0) "
          "total_cost FROM [nooredenadb].[extra].[avalhami_nav] FULL JOIN (SELECT date,SUM((CASE WHEN type=1 THEN "
          "funds_unit ELSE funds_unit*-1 END)) funds_unit,SUM((CASE WHEN type=1 THEN value ELSE cost * -1 END)) AS cost"
          " FROM [nooredenadb].[extra].[avalhami_trades] GROUP BY date) TEMP ON avalhami_nav.JalaliDate=TEMP.date")
    avalhami_history = pd.read_sql(q_, db_conn)
    avalhami_history.sort_values("date", inplace=True, ignore_index=True)
    avalhami_history["volume"] = avalhami_history["amount"].cumsum()
    avalhami_history["total_cost"] = avalhami_history["total_cost"].cumsum()
    avalhami_history["total_cost_sep"] = avalhami_history["total_cost"]
    avalhami_history["value"] = avalhami_history["volume"] * avalhami_history["final_price"]
    avalhami_history = avalhami_history[["date", "value", "total_cost"]].groupby(by="date", as_index=False).sum()
    return avalhami_history

def get_avalhami_profitloss() -> pd.DataFrame:
    q_ = "SELECT date, (value - cost) profitloss FROM [nooredenadb].[extra].[avalhami_trades] WHERE type=2"
    avalhami_profitloss = pd.read_sql(q_, db_conn)
    avalhami_profitloss["date_"] = avalhami_profitloss["date"].str[:7]
    avalhami_profitloss = avalhami_profitloss[["date_", "profitloss"]].groupby(by="date_", as_index=False).sum()
    return avalhami_profitloss

def get_portfolio_history(start_date: str, end_date: str, include_avalhami: bool = True,
                          main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")
    if (not main_portfolio) and include_avalhami:
        raise ValueError("both main and avalham could not be False!!! main bust be True for (include_avalhami=True).")

    q_ = "SELECT * FROM [nooredenadb].[company].[portfolio_history] WHERE date>='{}' AND date<='{}'"
    portfolio_history = pd.read_sql(q_.format(start_date, end_date), db_conn)

    if main_portfolio:
        portfolio_history.drop(labels=["total_cost_sep"], axis=1, inplace=True)
        if not prx_portfolio:
            portfolio_history = portfolio_history[portfolio_history["portfolio_id"] == 1].reset_index(drop=True)

    if (not main_portfolio) and prx_portfolio:
        portfolio_history = portfolio_history[portfolio_history["portfolio_id"] != 1].reset_index(drop=True)
        portfolio_history.drop(labels=["total_cost"], axis=1, inplace=True)
        portfolio_history.rename({"total_cost_sep": "total_cost"}, axis=1, inplace=True)

    portfolio_history["value"] = portfolio_history["volume"] * portfolio_history["final_price"]
    portfolio_history = portfolio_history[["date", "value", "total_cost"]].groupby(by="date", as_index=False).sum()

    if main_portfolio and include_avalhami:
        avalhami_df = get_avalhami_history()
        avalhami_df.rename(columns={"total_cost": "total_cost_avalhami", "value": "value_avalhami"}, inplace=True)
        portfolio_history = portfolio_history.merge(avalhami_df, on="date", how="left")
        portfolio_history["value_avalhami"].ffill(inplace=True)
        portfolio_history["total_cost_avalhami"].ffill(inplace=True)
        portfolio_history["value"] += portfolio_history["value_avalhami"]
        portfolio_history["total_cost"] += portfolio_history["total_cost_avalhami"]
        portfolio_history = portfolio_history[["date", "value", "total_cost"]]
    else:
        pass
    return portfolio_history

def get_dividend(start_date: str, end_date: str, main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    q_ = ("SELECT date, dividend, portfolio_id FROM [nooredenadb].[portfolio].[portfolio_dividend]"
          " WHERE date>'{}' AND date<='{}'")
    dividend = pd.read_sql(q_.format(start_date, end_date), db_conn)

    if main_portfolio:
        if not prx_portfolio:
            dividend = dividend[dividend["portfolio_id"] == 1].reset_index(drop=True, inplace=False)
    if (not main_portfolio) and prx_portfolio:
        dividend = dividend[dividend["portfolio_id"] != 1].reset_index(drop=True, inplace=False)

    dividend["date_"] = dividend["date"].str[:7]
    dividend = dividend[["date_", "dividend"]].groupby(by=["date_"], as_index=False).sum()
    return dividend

def get_profitloss(start_date: str, end_date: str, include_avalhami: bool = True,
                   main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")
    if (not main_portfolio) and include_avalhami:
        raise ValueError("both main and avalham could not be False!!! main bust be True for (include_avalhami=True).")

    q_ = ("SELECT date, portfolio_id, value, total_cost, total_cost_sep FROM (SELECT date, portfolio_id, type, "
          "value, total_cost, total_cost_sep FROM [nooredenadb].[company].[trades] UNION SELECT date, portfolio_id,"
          " type, value, total_cost, total_cost_sep FROM [nooredenadb].[brokers].[trades_last] WHERE date > (SELECT"
          " MAX(date) FROM [nooredenadb].[company].[trades]) AND SUBSTRING(symbol, 1, 1) NOt IN ('ض', 'ط')) AS TEMP"
          " WHERE type=2 AND date>'{}' AND date<='{}'")
    profitloss = pd.read_sql(q_.format(start_date, end_date), db_conn)

    if main_portfolio:
        if not prx_portfolio:
            profitloss = profitloss[profitloss["portfolio_id"] == 1].reset_index(drop=True, inplace=False)
        profitloss.drop(labels=["total_cost_sep"], axis=1, inplace=True)

    if (not main_portfolio) and prx_portfolio:
        profitloss = profitloss[profitloss["portfolio_id"] != 1].reset_index(drop=True, inplace=False)
        profitloss.drop(labels=["total_cost"], axis=1, inplace=True)
        profitloss.rename({"total_cost_sep": "total_cost"}, axis=1, inplace=True)

    profitloss["profitloss"] = profitloss["value"] - profitloss["total_cost"]
    profitloss["date_"] = profitloss["date"].str[:7]
    profitloss = profitloss[["date_", "profitloss"]].groupby(by=["date_"], as_index=False).sum()

    if main_portfolio and include_avalhami:
        avalhami_profitloss = get_avalhami_profitloss()
        avalhami_profitloss.rename(columns={"profitloss": "profitloss_avalhami"}, inplace=True)
        profitloss = profitloss.merge(avalhami_profitloss, on="date_", how="left")
        profitloss["profitloss"] += profitloss["profitloss_avalhami"].fillna(0)
        profitloss = profitloss[["date_", "profitloss"]]
    else:
        pass
    return profitloss

def get_index(index_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    q_ = (f"SELECT TEMP1.close_price,TEMP2.date FROM (SELECT TRY_CONVERT(INT,REPLACE(Miladi,'-','')) date_m,"
          f"Jalali_1 date FROM [nooredenadb].[extra].[dim_date]) TEMP2 LEFT JOIN (SELECT * FROM "
          f"[nooredenadb].[tsetmc].[indices_history] WHERE indices_id='{index_id}') TEMP1 "
          f"ON TEMP1.date=TEMP2.date_m ORDER BY TEMP2.date")
    index_df = pd.read_sql(q_, db_conn)
    index_df["close_price"].ffill(inplace=True)
    index_df["date_"] = index_df["date"].str[:7]
    index_df.drop_duplicates(subset="date_", keep="last", inplace=True, ignore_index=True)
    index_df = index_df[(index_df["date"] >= start_date) & (index_df["date"] <= end_date)][["date_", "close_price"]]
    return index_df

def get_indices(start_date: str, end_date: str) -> pd.DataFrame:
    index_names_mapper = {"100": "total_index", "104": "price_index_eq", "56": "investment_index"}
    q_ = "SELECT indices,indices_name,indices_id FROM [nooredenadb].[tsetmc].[indices] WHERE indices IN ('100','104','56')"
    indices = pd.read_sql(q_, db_conn)
    indices.sort_values(by="indices_name", ascending=False, ignore_index=True, inplace=True)
    indices_df = pd.DataFrame(columns=["date_"])
    for idx_ in range(len(indices)):
        index_name = index_names_mapper[indices["indices"].iloc[idx_]]
        tmp = get_index(indices["indices_id"].iloc[idx_], start_date, end_date)
        q__ = (f"SELECT SUBSTRING(REPLACE(date,'-','/'), 1, 7) date_,close_price FROM "
               f"[nooredenadb].[tsetmc].[indices_data_today] WHERE indices_id='{indices["indices_id"].iloc[idx_]}'")
        tmp_today = pd.read_sql(q__, db_conn)
        tmp = pd.concat([tmp, tmp_today], axis=0, ignore_index=True)
        tmp.drop_duplicates(subset="date_", keep="last", inplace=True, ignore_index=True)
        tmp["return"] = tmp["close_price"].diff(periods=1) / tmp["close_price"].shift(periods=1)
        tmp.rename({"close_price": index_name, "return": "return_" + index_name}, axis=1, inplace=True)
        indices_df = indices_df.merge(tmp, on="date_", how="outer")
    return indices_df

