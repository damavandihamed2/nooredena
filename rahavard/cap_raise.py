import jdatetime, warnings
import pandas as pd
from dateutil import parser
from utils.database import make_connection


warnings.filterwarnings("ignore")
def date_parser(d: str | None) -> str:
    if d:
        d_ = parser.parse(d)
        return  jdatetime.datetime.fromgregorian(year=d_.year, month=d_.month, day=d_.day).strftime("%Y/%m/%d")
    else:
        return ""

db_conn = make_connection()
df_portfolio = pd.read_sql("SELECT DISTINCT(union_portfolio.symbol) symbol FROM (SELECT * FROM "
                           "[nooredenadb].[portfolio].[portfolio] UNION SELECT * FROM "
                           "[emtiazeaval].[portfolio].[portfolio]) union_portfolio", db_conn)
cap_raise_df = pd.read_sql("SELECT cc.*, s.symbol FROM nooredenadb.rahavard.capital_changes AS cc LEFT JOIN "
                           "nooredenadb.rahavard.symbols AS s ON cc.asset_id = s.asset_id WHERE "
                           "date > '2026-02-20' AND date < '2026-05-22'", db_conn)
db_conn.close()

df_portfolio.replace({'symbol': {"ي": "ی", "ك": "ک"}}, regex=True, inplace=True)
df_portfolio["in_portfolio"] = True

cap_raise_df_ = cap_raise_df.merge(df_portfolio, on="symbol", how="left")
cap_raise_df_.fillna({"in_portfolio": False}, inplace=True)
cap_raise_df_ = cap_raise_df_[cap_raise_df_["in_portfolio"]].reset_index(drop=True, inplace=False)
cap_raise_df_ = cap_raise_df_[["date", "symbol", "underwriting_end_date", "registration_date",
                               "stock_certificate_receive_date", "comments", 'previous_capital',
                               'new_capital', 'contribution', 'premium', 'reserve' ]]

cap_raise_df_["date"] = cap_raise_df_["date"].apply(date_parser)
cap_raise_df_["underwriting_end_date"] = cap_raise_df_["underwriting_end_date"].apply(date_parser)
cap_raise_df_["registration_date"] = cap_raise_df_["registration_date"].apply(date_parser)
cap_raise_df_["stock_certificate_receive_date"] = cap_raise_df_["stock_certificate_receive_date"].apply(date_parser)

cap_raise_df_.to_excel("c:/users/h.damavandi/desktop/cap_raise_df.xlsx", index=False)
