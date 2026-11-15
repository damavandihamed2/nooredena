import pandas as pd
from pyodbc import Connection



def get_portfolio(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    portfolio = pd.read_sql(
        "SELECT date,SUM(gross_value_final_price) value,SUM(total_cost) total_cost "
        "FROM [nooredenadb].[sigma].[portfolio] WHERE "
        "type NOT IN ('اختیار معامله | موقعیت خرید', 'اختیار معامله | موقعیت فروش') AND "
        # f"type != 'گواهی سپرده کالایی' AND "
        f"date >= '{start_date}' AND date <= '{end_date}' GROUP BY date",
        db_connection
    )

    return portfolio


def get_dividend(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    dividend = pd.read_sql(
        "SELECT meeting_date date, SUM(value) dividend FROM [nooredenadb].[sigma].[dividend] "
        f"WHERE meeting_date > '{start_date}' AND meeting_date <= '{end_date}' GROUP BY meeting_date",
        db_connection
    )

    return dividend


def get_profitloss(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    profitloss = pd.read_sql(
        "SELECT date, SUM(net_profit) net_profit FROM [nooredenadb].[sigma].[profitloss] WHERE "
        # f"type != 'گواهی سپرده کالایی' AND "
        f"date > '{start_date}' AND date <= '{end_date}' GROUP BY date",
        db_connection
    )

    return profitloss
