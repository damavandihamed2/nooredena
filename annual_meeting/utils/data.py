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



def get_credits(
        start_date: str,
        end_date: str,
        main_portfolio: bool = True,
        prx_portfolio: bool = True

) -> dict[str: pd.DataFrame]:

    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    start_date_m = jdatetime.datetime.strptime(start_date, "%Y/%m/%d").togregorian().strftime("%Y-%m-%d")
    credits_rayan = pd.read_sql("SELECT tr.transactionDate AS [date], tr.remaining, tr.broker_id, tr.portfolio_id FROM"
                                " nooredenadb.brokers.trades_rayan AS tr JOIN (SELECT MAX(row_) AS row_ FROM "
                                f"[nooredenadb].[brokers].[trades_rayan] WHERE transactionDate >= '{start_date}' "
                                "GROUP BY transactionDate, broker_id, portfolio_id ) AS last_rows "
                                "ON tr.row_ = last_rows.row_ ORDER BY tr.transactionDate, tr.portfolio_id, "
                                "tr.broker_id;", db_conn)
    credits_tadbir = pd.read_sql("WITH last_rows AS (SELECT MAX(row_) AS row_ FROM "
                                 f"[nooredenadb].[brokers].[trades_tadbir_ledger] WHERE "
                                 f"TransactionDate >= '{start_date_m}' AND Description NOT "
                                 f"IN (N'سند افتتاحیه مورخ {0}', N'سند اختتامیه') GROUP BY "
                                 "substring(TransactionDate, 1, 10), broker_id, portfolio_id), d AS (SELECT Miladi,"
                                 f" Jalali_1 FROM [nooredenadb].[extra].[dim_date] WHERE Jalali_1 >= '{start_date}' AND"
                                 " Miladi <= CAST(GETDATE() AS date)) SELECT tr.broker_id, tr.portfolio_id, tr.Remain "
                                 "AS remaining, d.Jalali_1 AS [date] FROM [nooredenadb].[brokers].[trades_tadbir_ledger]"
                                 " AS tr JOIN last_rows lr ON tr.row_ = lr.row_ JOIN d ON substring(TransactionDate,"
                                 " 1, 10) = d.Miladi ORDER BY d.Jalali_1, tr.portfolio_id, tr.broker_id;", db_conn)
    credits = pd.concat([credits_rayan, credits_tadbir], axis=0, ignore_index=True)

    credits_index = pd.MultiIndex.from_product(
        [credits["date"].unique(), credits["broker_id"].unique(), credits["portfolio_id"].unique()],
        names=['date', 'broker_id', "portfolio_id"])
    credits_df = pd.DataFrame(index=credits_index).reset_index(drop=False, inplace=False)
    credits_df = credits_df.merge(credits, on=["date", "broker_id", "portfolio_id"], how="left").sort_values(
        by=["portfolio_id", "broker_id", "date"], inplace=False)

    credits_df["remaining"] = credits_df.groupby(["portfolio_id", "broker_id"])["remaining"].ffill()
    credits_df["remaining"] = (credits_df["remaining"] < 0) * credits_df["remaining"]

    if main_portfolio and (not prx_portfolio):
        credits_df = credits_df[credits_df["portfolio_id"] == 1]
    if (not main_portfolio) and prx_portfolio:
        credits_df = credits_df[credits_df["portfolio_id"] != 1]

    credits_df = credits_df[["date", "remaining"]].groupby(by="date", as_index=False).sum()
    credits_df = credits_df[credits_df["date"] <= end_date].sort_values(by="date", ascending=True, ignore_index=True)
    credits_df["remaining"] *= -1
    return credits_df

