import pandas as pd
import warnings, jdatetime
from typing import Literal, Optional

from utils import database


warnings.filterwarnings("ignore")
db_conn = database.make_connection()


def get_portfolio_specs() -> pd.DataFrame:
    portfolio_specs = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[portfolio_specifics]", db_conn)
    return portfolio_specs

def check_basket(l: set):
    p_s = get_portfolio_specs()
    prx = set(p_s[p_s["portfolio_type"] != "main"]["portfolio_id"].values.tolist())
    check_1, check_2 =  1 in l, bool(l & prx)
    if check_1 and check_2: return "هر دو"
    elif check_1: return "اصلی"
    elif check_2: return "prx"
    else: return "نامشخص"

def get_fix_income_symbols() -> pd.DataFrame:
    query_fix_income_symbols = "SELECT [symbol] FROM [nooredenadb].[tsetmc].[symbols] WHERE subsector='6812'"
    fix_income_symbols = pd.read_sql(query_fix_income_symbols, db_conn)
    return fix_income_symbols

########################################################################################################################


def get_trades_value(start_date: str, end_date: str, trade_type: Optional[Literal[1, 2]] = None,
                     include_options: bool = False, include_fix_funds: bool = False, main_portfolio: bool = True,
                     prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:

    if not((trade_type in [1, 2]) or (trade_type is None)): raise ValueError("trade_type must be either 1 or 2 or None")
    if (not main_portfolio) and (not prx_portfolio): raise ValueError("both main and prx could not be False!")

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

########################################################################################################################

def get_credits(start_date: str, end_date: str, main_portfolio: bool = True, prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:

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

