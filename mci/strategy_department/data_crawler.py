import warnings
import jdatetime
import pandas as pd

from utils.database import make_connection
from tgju.tgju import get_symbol_data_chart



warnings.filterwarnings('ignore')
db_conn = make_connection()
dim_date_query = ("SELECT TRY_CONVERT(varchar, Miladi) date, REPLACE(Jalali_1, '/', '-') date_jalali "
                  "FROM [nooredenadb].[extra].[dim_date]")
dim_data = pd.read_sql(dim_date_query, db_conn)



# fetching Currencies prices from tgju
currencies_df = pd.DataFrame(columns=["date"])
currency_list = ["bank_cny", "price_dollar_rl", "price_eur", "price_aed"]
for c in currency_list:
    print(f"Try {c}")
    data = get_symbol_data_chart(c)
    data = data[["date", "close_price"]].rename({"close_price": c}, axis=1, inplace=False)
    currencies_df = currencies_df.merge(data, on="date", how="outer")
currencies_df = currencies_df.merge(dim_data, on="date", how="left")
currencies_df = currencies_df[["date", "date_jalali"] + currency_list]



# fetching Metals prices from tgju
metals_df = pd.DataFrame(columns=["date"])
metals_list = ["basemetal-tin", "silver", "basemetal-copper", "basemetal-aluminum", "ons"]
for m in metals_list:
    print(f"Try {m}")
    data = get_symbol_data_chart(m)
    data = data[["date", "close_price"]].rename({"close_price": m}, axis=1, inplace=False)
    metals_df = metals_df.merge(data, on="date", how="outer")
metals_df = metals_df.merge(dim_data, on="date", how="left")
metals_df = metals_df[["date", "date_jalali"] + metals_list]



# querying CPI from database
cpi_data_query = ("SELECT date, date_jalali, commodity cat, reference subCat, price value, unit "
                                     "FROM [nooredenadb].[economic].[commodities_amar_centre]")
cpi_df = pd.read_sql(cpi_data_query, db_conn)



# querying Indices from database
indices_list = [32, 64, 72, 73]
indices_query = f"SELECT * FROM nooredenadb.tsetmc.indices WHERE indices IN {tuple(indices_list)}"
indices = pd.read_sql(indices_query, db_conn)

indices_df = pd.DataFrame(columns=["dEven"])
index_history_query = ("SELECT date dEven, close_price FROM [nooredenadb].[tsetmc].[indices_history] "
                       "WHERE indices_id = '{indices_id}'")
for row in indices.iterrows():
    name, id_ = row[1]["indices_name"], row[1]["indices_id"]
    index_history = pd.read_sql(index_history_query.format(indices_id=id_), db_conn)
    index_history.rename({"close_price": name.replace("\\u200c", " ")}, axis=1, inplace=True)
    indices_df = indices_df.merge(index_history, on="dEven", how="outer")

dim_data["dEven"] = dim_data["date"].str.replace("-", "").astype(int)
indices_df = indices_df.merge(dim_data, on="dEven", how="left")
indices_df = indices_df[["date", "date_jalali"] + [c for c in indices_df.columns.tolist() if "d" not in c]]


# writing all data into one Excel
now_ = jdatetime.datetime.now().strftime("%Y-%m-%d %H-%M")
writer = pd.ExcelWriter(f"c:/users/h.damavandi/desktop/mci_strategy_department ({now_}).xlsx", engine="xlsxwriter")
currencies_df.to_excel(writer, sheet_name="currencies", index=False)
metals_df.to_excel(writer, sheet_name="metals", index=False)
cpi_df.to_excel(writer, sheet_name="cpi", index=False)
indices_df.to_excel(writer, sheet_name="indices", index=False)
writer.close()

