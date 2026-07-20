import warnings
import pandas as pd
from utils.database import make_connection
from tgju.tgju import get_symbol_data_table


warnings.filterwarnings("ignore")

db_conn = make_connection()
dim_date = pd.read_sql(
    "SELECT TRY_CONVERT(VARCHAR, Miladi) date, Jalali_1 date_jalali FROM nooredenadb.extra.dim_date", db_conn)
db_conn.close()
df = get_symbol_data_table("geram18")
df = df.merge(dim_date, on='date', how='left')

df.to_excel("./tgju/geram18.xlsx", index=False)
