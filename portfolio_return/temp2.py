import warnings
import pandas as pd
from pyodbc import Connection

from utils.database import make_connection
from portfolio_return.credit.credit_history import get_credits_history



def handle_timeframe(timeframe: str) -> None:
    if timeframe == "daily":
        pass
    elif timeframe == "weekly":
        pass
    elif timeframe == "monthly":
        pass
    else:
        raise ValueError(f"timeframe {timeframe} is not supported, timeframe should be 'daily' or 'monthly'")


def get_dim_date(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    if timeframe == "monthly":
        q_ = (f"SELECT Jalali_1 date, MAX(Jalali_1) OVER (PARTITION BY LEFT(Jalali_1, 7)) AS {timeframe} FROM "
              f"[nooredenadb].[extra].[dim_date] WHERE Jalali_1 > '{start_date}' AND Jalali_1 <= '{end_date}';")
    else:
        q_ = (f"SELECT Jalali_1 date, MAX(Jalali_1) OVER (PARTITION BY jyear, JWeekNum) AS {timeframe} FROM "
              f"[nooredenadb].[extra].[dim_date] WHERE Jalali_1 > '{start_date}' AND Jalali_1 <= '{end_date}';")

    dim_date = pd.read_sql(q_, db_connection)

    return dim_date


def get_portfolio(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    portfolio = pd.read_sql(
        "SELECT date,SUM(gross_value_final_price) value,SUM(total_cost) total_cost "
        "FROM [nooredenadb].[sigma].[portfolio] WHERE "
        "type NOT IN ('اختیار معامله | موقعیت خرید', 'اختیار معامله | موقعیت فروش') AND "
        # f"type != 'گواهی سپرده کالایی' AND "
        f"date >= '{start_date}' AND date <= '{end_date}' GROUP BY date",
        db_connection
    )

    if timeframe != "daily":
        dim_date = get_dim_date(
            start_date=start_date, end_date=end_date, timeframe=timeframe, db_connection=db_connection
        )
        portfolio = portfolio.merge(dim_date, on="date", how="left")
        portfolio.fillna({timeframe: portfolio["date"]}, inplace=True)
        portfolio = portfolio[portfolio[timeframe] == portfolio["date"]]
        portfolio.drop(columns=[timeframe], inplace=True)

    return portfolio


def get_profitloss(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    profitloss = pd.read_sql(
        "SELECT date, SUM(net_profit) net_profit FROM [nooredenadb].[sigma].[profitloss] WHERE "
        # f"type != 'گواهی سپرده کالایی' AND "
        f"date > '{start_date}' AND date <= '{end_date}' GROUP BY date",
        db_connection
    )

    if timeframe != "daily":
        dim_date = get_dim_date(
            start_date=start_date, end_date=end_date, timeframe=timeframe, db_connection=db_connection
        )
        profitloss = profitloss.merge(dim_date, on="date", how="left")
        profitloss = profitloss.groupby(by=timeframe, as_index=False).sum(numeric_only=True)
        profitloss = profitloss.rename(columns={timeframe: "date"}, inplace=False)

    return profitloss


def get_dividend(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    dividend = pd.read_sql(
        "SELECT meeting_date date, SUM(value) dividend FROM [nooredenadb].[sigma].[dividend] "
        f"WHERE meeting_date > '{start_date}' AND meeting_date <= '{end_date}' GROUP BY meeting_date",
        db_connection
    )

    if timeframe != "daily":
        dim_date = get_dim_date(
            start_date=start_date, end_date=end_date, timeframe=timeframe, db_connection=db_connection
        )
        dividend = dividend.merge(dim_date, on="date", how="left")
        dividend = dividend.groupby(by=timeframe, as_index=False).sum(numeric_only=True)
        dividend = dividend.rename(columns={timeframe: "date"}, inplace=False)

    return dividend





warnings.filterwarnings("ignore")

db_conn = make_connection()

timeframe = "weekly"
start_date = "1404/10/30"
end_date = "1405/04/31"


credits_history = get_credits_history(start_date=start_date, end_date=end_date, db_connection=db_conn)


portfolio_df = get_portfolio(start_date, end_date, timeframe, db_conn)
profitloss_df = get_profitloss(start_date, end_date, timeframe, db_conn)
dividend_df = get_dividend(start_date, end_date, timeframe, db_conn)


df = portfolio_df.merge(profitloss_df, how="outer", on="date").merge(dividend_df, how="outer", on="date")
df.fillna(0, inplace=True)
df["surplus"] = df["value"] - df["total_cost"] + df["dividend"] + df["net_profit"]

df["surplus_yesterday"] = df["value"].shift(1) - df["total_cost"].shift(1)

df["return"] = (df["surplus"] - df["surplus_yesterday"]) / df["value"].shift(1)
df.dropna(subset=["return"], inplace=True)

(df["return"] + 1).prod()

df.to_excel("c:/users/h.damavandi/desktop/df.xlsx", index=False)


"""

1405/06/31
===============
daily: 54.74 %
weekly: 54.60 %
monthly: 53.84 %


1405/04/31
===============

daily: 12.37 %
weekly: 12.32 %
monthly: 11.90 %



"""



