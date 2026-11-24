import warnings
import jdatetime
import pandas as pd
from pyodbc import Connection
from pandas.core.frame import DataFrame

from portfolio_return.tools.cashflow import get_cashflow
from portfolio_return.tools.credit_history import get_credits_history
from portfolio_return.tools.portfolio import get_portfolio, get_dividend, get_profitloss



warnings.filterwarnings("ignore")


def get_portfolio_return_twrr(
        start_date: str,
        end_date: str,
        timeframe: str,
        db_connection: Connection,
        save_to_excel: bool = False,
) -> DataFrame:

    portfolio_df = get_portfolio(start_date, end_date, timeframe, db_connection)
    dividend_df = get_dividend(start_date, end_date, timeframe, db_connection)
    cashflow = get_cashflow(start_date, end_date, timeframe, db_connection)
    credits = get_credits_history(start_date, end_date, db_connection)

    df = portfolio_df.merge(
        dividend_df, on="date", how="outer"
    ).merge(
        cashflow, on="date", how="outer"
    ).merge(
        credits, on="date", how="outer"
    )


    df["value"].ffill(inplace=True)
    df["total_cost"].ffill(inplace=True)
    df.fillna(0, inplace=True)
    df["cashflow"] = df["in"] - df["out"]

    df["return1"] = (df["value"] - df["credit"] + df["cash"] + df["dividend"]) / (
                df["value"].shift(1) - df["credit"].shift(1) + df["cash"].shift(1) + df["cashflow"])

    df["return2"] = (df["value"] - df["credit"] + df["cash"] + df["dividend"] - df["cashflow"]) / (
                df["value"].shift(1) - df["credit"].shift(1) + df["cash"].shift(1))

    df.dropna(subset=["return1", "return2"], inplace=True)
    r1 = df["return1"].cumprod().iloc[-1] - 1
    r2 = df["return2"].cumprod().iloc[-1] - 1

    df = pd.concat([df, pd.DataFrame(
        [{"date": "1بازده", "value": r1}, {"date": "2بازده", "value": r2}]
    )], axis=0, ignore_index=True)

    if save_to_excel:
        df.to_excel("c:/users/h.damavandi/desktop/df.xlsx", index=False)

    return df



def get_portfolio_return_md(
        start_date: str,
        end_date: str,
        timeframe: str,
        db_connection: Connection,
        save_to_excel: bool = False,
) -> DataFrame:


    portfolio_df = get_portfolio(start_date, end_date, timeframe, db_connection)
    dividend_df = get_dividend(start_date, end_date, timeframe, db_connection)
    cashflow = get_cashflow(start_date, end_date, timeframe, db_connection)
    credits = get_credits_history(start_date, end_date, db_connection)

    df = portfolio_df.merge(
        dividend_df, on="date", how="outer"
    ).merge(
        cashflow, on="date", how="outer"
    ).merge(
        credits, on="date", how="outer"
    )


    df["value"].ffill(inplace=True)
    df["total_cost"].ffill(inplace=True)
    df.fillna(0, inplace=True)

    df["cashflow"] = df["in"] - df["out"] - df["dividend"]
    df["days"] = [
        jdatetime.datetime.strptime(df["date"].iloc[-1], "%Y/%m/%d") - jdatetime.datetime.strptime(df["date"].iloc[i], "%Y/%m/%d")
        for i in range(len(df))
    ]
    df["weight"] = df["days"] / df["days"].iloc[0]
    df["weighted_cashflow"] = df["cashflow"] * df["weight"]

    r = (df["value"].iloc[-1] - df["value"].iloc[0] - df["cashflow"].sum()
        ) / (df["value"].iloc[0] + df["weighted_cashflow"].sum())

    df = pd.concat([df, pd.DataFrame([{"date": "بازده", "value":r}])], axis=0, ignore_index=True)

    if save_to_excel:
        df.to_excel("c:/users/h.damavandi/desktop/df.xlsx", index=False)

    return df


# def get_portfolio_return_st(
#         start_date: str,
#         end_date: str,
#         timeframe: str,
#         db_connection: Connection,
#         save_to_excel: bool = False,
# ) -> DataFrame:
#
#     import warnings
#     import jdatetime
#     import pandas as pd
#     from pyodbc import Connection
#     from pandas.core.frame import DataFrame
#
#     from portfolio_return.tools.cashflow import get_cashflow
#     from portfolio_return.tools.credit_history import get_credits_history
#     from portfolio_return.tools.portfolio import get_portfolio, get_dividend, get_profitloss
#
#     warnings.filterwarnings("ignore")
#
#     from utils.database import make_connection
#
#     db_connection = make_connection()
#     timeframe = "daily"
#     start_date = "1404/10/30"
#     end_date = "1405/07/30"
#     db_connection = db_connection
#     save_to_excel = False
#
#
#     portfolio_df = get_portfolio(start_date, end_date, timeframe, db_connection)
#     dividend_df = get_dividend(start_date, end_date, timeframe, db_connection)
#     profitloss = get_profitloss(start_date, end_date, timeframe, db_connection)
#     # credits = get_credits_history(start_date, end_date, db_connection)
#
#     df = portfolio_df.merge(dividend_df, on="date", how="outer").merge(profitloss, on="date", how="outer")
#     df.fillna(0, inplace=True)
#
#     df["return"] = (df["value"] - df["total_cost"] + df["net_profit"] + df["dividend"] - (df["value"].shift(1) - df["total_cost"].shift(1))) / df["value"].shift(1)
#     df["return"] = df["return"] + 1
#     df["return"].cumprod()
#
#
#
#     df = pd.concat([df, pd.DataFrame([{"date": "بازده", "value":r}])], axis=0, ignore_index=True)
#
#     if save_to_excel:
#         df.to_excel("c:/users/h.damavandi/desktop/df.xlsx", index=False)
#
#     return df


if __name__ == "__main__":

    from utils.database import make_connection

    db_connection = make_connection()


    if db_connection:
        portfo_return_twrr = get_portfolio_return_twrr(
            timeframe="daily",
            start_date = "1404/10/30",
            end_date = "1405/08/30",
            db_connection = db_connection,
            save_to_excel = False,
        )
        portfo_return_twrr.to_excel("c:/users/h.damavandi/desktop/return_twrr.xlsx", index=False)

        portfo_return_md = get_portfolio_return_md(
            timeframe="daily",
            start_date = "1404/10/30",
            end_date = "1405/08/30",
            db_connection = db_connection,
            save_to_excel = False,
        )
        portfo_return_md.to_excel("c:/users/h.damavandi/desktop/return_md.xlsx", index=False)
