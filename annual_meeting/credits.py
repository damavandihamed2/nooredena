import warnings
import pandas as pd

from utils.database import make_connection
from annual_meeting.utils.data import get_credits

start_day = "1403/10/30"
warnings.filterwarnings("ignore")
db_conn = make_connection()

##################################################

credits_df = get_credits(start_date="1403/10/30", end_date="1404/10/30")


sigma_portfolio = pd.read_sql("SELECT date, sum(total_cost) as cost, sum(gross_value_final_price) as value "
                              f"FROM [nooredenadb].[sigma].[portfolio] where date>='{start_day}' and "
                              "type in ('صندوق', 'گواهی سپرده کالایی', 'حق تقدم', 'سهام') "
                              "group by date order by date", db_conn)
sigma_dividend = pd.read_sql("SELECT meeting_date as date, sum(value) as dividend FROM "
                             f"[nooredenadb].[sigma].[dividend] where meeting_date >= '{start_day}' and "
                             "value > 0 group by meeting_date order by meeting_date", db_conn)
sigma_profit = pd.read_sql("SELECT date, sum(net_profit) as profit FROM [nooredenadb].[sigma].[buysell] where "
                           f"date >= '{start_day}' and action='فروش' and type!='اختیار معامله' group by date "
                           "order by date", db_conn)

dataframe = sigma_portfolio.merge(sigma_dividend, on="date", how="outer")
dataframe = dataframe.merge(sigma_profit, on="date", how="outer")
dataframe = dataframe.merge(credits_df, on="date", how="outer")

dataframe["remaining"].ffill(inplace=True)
dataframe["remaining_yesterday"] = dataframe["remaining"].shift(1)
dataframe["value_yesterday"] = dataframe["value"].shift(1)
dataframe["cost_yesterday"] = dataframe["cost"].shift(1)
dataframe.fillna({"dividend": 0, "profit": 0}, inplace=True)
dataframe["portfo_return"] = ((dataframe["value"] + dataframe["dividend"] + dataframe["profit"] -
                               dataframe["cost"] - (dataframe["value_yesterday"] - dataframe["cost_yesterday"])
                               ) / dataframe["value_yesterday"]) + 1
dataframe["credit_profit"]  = (dataframe["portfo_return"] - 1) * dataframe["remaining_yesterday"]
dataframe.to_excel("c:/users/h.damavandi/desktop/dataframe.xlsx", index=False)
