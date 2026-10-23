import jdatetime
import numpy as np
import pandas as pd
from pyodbc import Connection
from pandas.core.frame import DataFrame



def get_closed_dates(db_connection: Connection) -> DataFrame:
    closed_dates = pd.read_sql(
        "SELECT REPLACE([date], '-', '/') as date FROM [nooredenadb].[extra].[closed_days]",
        db_connection
    )
    return closed_dates


def get_last_date(date_: str, db_connection: Connection) -> str:
    query = "SELECT MAX(Jalali_1) AS date FROM [nooredenadb].[extra].[dim_date] WHERE MWeekDay NOT IN " \
            "('Thursday', 'Friday') AND Jalali_1 NOT IN (SELECT REPLACE([date], '-', '/') FROM " \
            f"[nooredenadb].[extra].[closed_days]) AND Jalali_1 < '{date_}'"
    d_ = pd.read_sql(query, db_connection)["date"].iloc[0]
    return d_


def get_next_date(date_: str, db_connection: Connection) -> str:
    query = "SELECT MIN(Jalali_1) AS date FROM [nooredenadb].[extra].[dim_date] WHERE " \
            "MWeekDay != 'Friday' AND Jalali_1 NOT IN (SELECT REPLACE([date], '-', '/') " \
            f"FROM [nooredenadb].[extra].[closed_days]) AND Jalali_1 > '{date_}'"
    d_ = pd.read_sql(query, db_connection)["date"].iloc[0]
    return d_


def get_fundamental_data(db_connection: Connection) -> dict[str, DataFrame]:
    dps_funds = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[dps_funds]", db_connection)
    dps_funds["date"] = dps_funds.apply(lambda row: jdatetime.datetime.fromgregorian(
        year=int(row["date"][:4]), month=int(row["date"][5:7]), day=int(row["date"][8:10])
    ).strftime("%Y/%m/%d") if row["date"] is not None else None, axis=1)

    dps = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[dps]", db_connection)
    for c in ['fiscal_year', 'date_time', 'announcement_date']:
        dps[c] = dps.apply(lambda row: jdatetime.datetime.fromgregorian(
            year=int(row[c][:4]), month=int(row[c][5:7]), day=int(row[c][8:10])
        ).strftime("%Y/%m/%d") if row[c] is not None else None, axis=1)

    capital_changes = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[capital_changes]", db_connection)
    for c in ['date', 'underwriting_end_date', 'registration_date', 'stock_certificate_receive_date',
              'warrant_sell_date']:
        capital_changes[c] = capital_changes.apply(lambda row: jdatetime.datetime.fromgregorian(
            year=int(row[c][:4]), month=int(row[c][5:7]), day=int(row[c][8:10])
        ).strftime("%Y/%m/%d") if row[c] is not None else None, axis=1)

    return {
        "dps": dps,
        "dps_funds": dps_funds,
        "capital_changes": capital_changes,
    }


def get_cashflow(start_date: str, db_connection: Connection, include_avalhami_redemption: bool = False) -> DataFrame:
    query_cashflow = (f"SELECT * FROM [nooredenadb].[company].[cashflow] where date > '{start_date}' "
                      f"and (meeting_date <= '{start_date}' or debtor > 0) order by date")
    cashflow = pd.read_sql(query_cashflow, db_connection)

    if include_avalhami_redemption:
        query_avalhami_raw = ("SELECT [jalaliDate] date, [sellNAVPerShare] final_price, ISNULL(TEMP.funds_unit, 0) amount, "
                              "ISNULL(TEMP.cost, 0) total_cost FROM [nooredenadb].[extra].[avalhami_nav] LEFT JOIN (SELECT date,"
                              " SUM((CASE WHEN type=1 THEN funds_unit ELSE funds_unit * -1 END)) funds_unit, SUM((CASE WHEN "
                              "type=1 THEN value ELSE cost * -1 END)) AS cost FROM [nooredenadb].[extra].[avalhami_trades] "
                              "GROUP BY date) AS TEMP ON avalhami_nav.jalaliDate=TEMP.date")
        avalhami_df = pd.read_sql(query_avalhami_raw, db_connection)
        avalhami_df = avalhami_df[(avalhami_df["date"] > start_date) & (avalhami_df["amount"] < 0)]
        avalhami_df["creditor"] = avalhami_df["amount"] * avalhami_df["final_price"] * -1
        avalhami_df = avalhami_df[["date", "creditor"]]
        avalhami_df["debtor"] = 0
        avalhami_df["source"] = "حامی اول"
        avalhami_df["description"] = "واریز به حساب بابت ابطال واحدهای صندوق بازارگردانی حامی اول (حامی اول)"
        cashflow = pd.concat([cashflow, avalhami_df], axis=0, ignore_index=True)

    cashflow["due_date"] = [
        get_last_date(date_=cashflow["date"].iloc[i], db_connection=db_connection)
        if ~cashflow["date"].isna().iloc[i] else np.nan for i in range(len(cashflow))
    ]
    cashflow["paid"] = False

    return cashflow


def get_embedded_put_options(db_connection: Connection) -> DataFrame:
    put_options = pd.read_sql("SELECT * FROM [nooredenadb].[sigma].[put_option]", db_connection)
    put_options["payday"] = [
        get_next_date(date_=put_options["maturity_date"].iloc[i], db_connection=db_connection) for i in
        range(len(put_options))
    ]
    put_options["done"] = False
    return put_options


def get_rahavard_symbols(db_connection: Connection) -> DataFrame:
    rahavard_symbols = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[symbols]", db_connection)
    rahavard_symbols.replace({"symbol": {"ک": "ك", "ی": "ي"}}, regex=True, inplace=True)
    rahavard_symbols.loc[rahavard_symbols['symbol'] == "دارايكم", 'symbol'] = "دارا يكم"
    return rahavard_symbols


def get_symbols(db_connection: Connection) -> DataFrame:
    symbols = pd.read_sql(
        "SELECT symbol tse_symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols]",
        db_connection
    )
    return symbols


def get_symbols_ros(db_connection: Connection) -> DataFrame:
    symbols_ros = pd.read_sql(
        "SELECT symbol_ros tse_symbol , symbol_id FROM [nooredenadb].[tsetmc].[symbols_ros]",
        db_connection
    )
    return symbols_ros


def get_start_portfolio(start_date: str, db_connection: Connection, capital_changes_df: DataFrame) -> dict[str, DataFrame | int]:

    portfolio = pd.read_sql(
        f"SELECT symbol, SUM(volume) amount, SUM(total_cost) total_cost, final_price, date, is_ros ros, "
        f"is_paid_ros paid_ros FROM [nooredenadb].[company].[portfolio_history] WHERE date = '{start_date}' "
        f"GROUP BY date, symbol, is_ros, is_paid_ros, final_price",
        db_connection
    )

    portfolio["ros"] = portfolio["ros"] == 1
    portfolio["paid_ros"] = portfolio["paid_ros"] == 1
    portfolio["stock"] = portfolio["ros"] != 1
    portfolio["bonus_share"] = False
    portfolio.loc[portfolio['symbol'] == 'آكنتور', 'bonus_share'] = True
    portfolio["normal_ros"] = portfolio["ros"]
    portfolio["normal_share"] = (~portfolio["ros"]) & (~portfolio["paid_ros"]) & (~portfolio["bonus_share"])
    portfolio['original_symbol'] = portfolio.apply(
        lambda row: row['symbol'][:len(row["symbol"]) + (row["normal_ros"] * -1)],
        axis=1
    )

    rahavard_symbols = get_rahavard_symbols(db_connection=db_connection)
    portfolio = portfolio.merge(
        rahavard_symbols[["symbol", "asset_id"]].rename(
            {"symbol": "original_symbol"},
            axis=1,
            inplace=False
        ),
        on="original_symbol",
        how="left"
    )

    portfolio["tse_symbol"] = portfolio["symbol"]

    portfolio_value_start = int((portfolio["amount"] * portfolio["final_price"]).sum())

    portfolio[["ros_payday", "registration_date"]] = [np.nan, np.nan]
    for i in range(len(portfolio)):
        if ~portfolio["normal_share"].iloc[i]:
            temp = capital_changes_df[capital_changes_df["asset_id"] == portfolio["asset_id"].iloc[i]]
            temp = temp[temp["date"] <= start_date]
            temp = temp[~temp["registration_date"].isnull()].sort_values("date", ascending=False, inplace=False)
            portfolio["registration_date"].iloc[i] = temp["registration_date"].iloc[0]
            if portfolio["normal_ros"].iloc[i]:
                portfolio["ros_payday"].iloc[i] = temp["underwriting_end_date"].iloc[0]
    portfolio["ros_due_date"] = [
        get_last_date(date_=portfolio["ros_payday"].iloc[i], db_connection=db_connection)
        if ~portfolio["ros_payday"].isna().iloc[i] else np.nan for i in range(len(portfolio))
    ]

    return {
        "portfolio": portfolio,
        "value": portfolio_value_start
    }
