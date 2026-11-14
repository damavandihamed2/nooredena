import warnings
import numpy as np
import pandas as pd
from typing import Union, Optional
from pandas .core.frame import DataFrame

from utils.database import make_connection



warnings.filterwarnings('ignore')


def get_company_names_list() -> list:
    db_conn = make_connection()
    companies = pd.read_sql("SELECT * FROM nooredenadb.fda.selected_companies ORDER BY company_name", db_conn)
    db_conn.close()
    company_names = companies["company_name"].values.tolist()
    return company_names

def get_company_price_history(company: str) -> DataFrame:
    db_conn = make_connection()
    query_medicines_history = ("SELECT mh.date, mh.medicine_id, mh.price FROM nooredenadb.fda.medicines m INNER JOIN "
                               "nooredenadb.fda.medicines_history mh ON mh.medicine_id =m.medicine_id WHERE "
                               "m.producer_id = (SELECT p.producer_id FROM nooredenadb.fda.producers p WHERE "
                               f"p.producer = '{company}') OR m.license_owner_id = (SELECT lo.license_owner_id "
                               f"FROM nooredenadb.fda.license_owners lo WHERE lo.license_owner = '{company}')")
    medicines_history = pd.read_sql(query_medicines_history, db_conn)
    medicines_history = medicines_history.pivot(
        index='medicine_id', columns='date', values='price').reset_index(drop=False, inplace=False)
    db_conn.close()
    return medicines_history

def get_company_medicines(company: str) -> DataFrame:
    db_conn = make_connection()
    query_medicines = ("SELECT m.medicine_id, m.link, m.name, m.general_name 'generic name' FROM "
                       "nooredenadb.fda.medicines m WHERE m.producer_id = (SELECT p.producer_id FROM "
                       f"nooredenadb.fda.producers p WHERE p.producer = '{company}') OR "
                       "m.license_owner_id = (SELECT lo.license_owner_id FROM "
                       f"nooredenadb.fda.license_owners lo WHERE lo.license_owner = '{company}')")
    medicines = pd.read_sql(query_medicines, db_conn)
    db_conn.close()
    return medicines

def get_dates(get_last_month_full: bool = False, get_last_tow_days: bool = True) -> DataFrame:
    thrsh_date = "1405/01/01"
    if get_last_month_full:
        q_ = ("SELECT MIN(date) date FROM [nooredenadb].[fda].[medicines_history_selected] UNION ALL SELECT MAX(date) "
              "date FROM (SELECT distinct(date) FROM [nooredenadb].[fda].[medicines_history_selected] WHERE date < "
              f"'{thrsh_date}') TEMP1 GROUP BY SUBSTRING(date, 1, 5) UNION ALL SELECT MAX(date) date FROM (SELECT distinct"
              f"(date) FROM [nooredenadb].[fda].[medicines_history_selected] WHERE date>='{thrsh_date}') TEMP1 WHERE "
              "SUBSTRING(date, 1, 7) != (SELECT top(1) SUBSTRING(date, 1, 7) FROM [nooredenadb].[fda].[medicines]) "
              "GROUP BY SUBSTRING(date, 1, 7) UNION ALL SELECT date FROM (SELECT distinct(date) FROM "
              "[nooredenadb].[fda].[medicines_history_selected]) TEMP2 WHERE SUBSTRING(date, 1, 7) = (SELECT top(1) "
              "SUBSTRING(date, 1, 7) FROM [nooredenadb].[fda].[medicines])")
    else:
        if get_last_tow_days:
            q_ = ("SELECT MIN(date) date FROM [nooredenadb].[fda].[medicines_history_selected] UNION ALL SELECT "
                  "MAX(date) date FROM (SELECT distinct(date) FROM [nooredenadb].[fda].[medicines_history_selected]"
                  f" WHERE date < '{thrsh_date}') TEMP1 GROUP BY SUBSTRING(date, 1, 5) UNION ALL SELECT MAX(date) date "
                  "FROM (SELECT distinct(date) FROM [nooredenadb].[fda].[medicines_history_selected] WHERE "
                  f"date>='{thrsh_date}') TEMP1 WHERE SUBSTRING(date, 1, 7) != SUBSTRING((SELECT MAX(date) FROM "
                  "[nooredenadb].[fda].[medicines_history_selected]), 1, 7) GROUP BY SUBSTRING(date, 1, 7) UNION ALL "
                  "SELECT date FROM (SELECT DISTINCT TOP 2 date FROM [nooredenadb].[fda].[medicines_history_selected] "
                  "WHERE SUBSTRING(date, 1, 7) = SUBSTRING((SELECT MAX(date) FROM "
                  "[nooredenadb].[fda].[medicines_history_selected]), 1, 7) ORDER BY date Desc) AS ld")
        else:
            q_ = ("SELECT MIN(date) date FROM [nooredenadb].[fda].[medicines_history_selected] UNION ALL SELECT "
                  "MAX(date) date FROM (SELECT distinct(date) FROM [nooredenadb].[fda].[medicines_history_selected] "
                  f"WHERE date < '{thrsh_date}') TEMP1 GROUP BY SUBSTRING(date, 1, 5) UNION ALL SELECT MAX(date) date "
                  f"FROM (SELECT distinct(date) FROM [nooredenadb].[fda].[medicines_history_selected] "
                  f"WHERE date >= '{thrsh_date}') TEMP1 GROUP BY SUBSTRING(date, 1, 7)")
    db_conn = make_connection()
    months = pd.read_sql(q_, db_conn)
    db_conn.close()
    return months

def get_last_weight_link(symbol: Optional[str] = None) -> DataFrame:
    if symbol is None:
        q_ = ("SELECT TRY_CONVERT(INT, RIGHT(mw.link, CHARINDEX('/', REVERSE(mw.link)) - 1)) AS medicine_id, mw.symbol,"
              "mw.general_name, mw.weight, mw.date FROM [nooredenadb].[fda].[medicines_weight] mw JOIN (SELECT symbol, "
              "MAX([date]) AS max_date FROM [nooredenadb].[fda].[medicines_weight] GROUP BY symbol) latest ON mw.symbol"
              " = latest.symbol AND mw.[date] = latest.max_date ORDER BY mw.symbol")
    else:

        q_ = (f"SELECT TRY_CONVERT(INT, RIGHT(link, CHARINDEX('/', REVERSE(link)) - 1)) medicine_id, general_name, weight "
              f"FROM [nooredenadb].[fda].[medicines_weight] WHERE symbol='{symbol}' AND date = (SELECT MAX(date) as date "
              f"FROM [nooredenadb].[fda].[medicines_weight] WHERE symbol = '{symbol}')")
    db_conn = make_connection()
    weight_link = pd.read_sql(q_, db_conn)
    db_conn.close()
    return weight_link

def get_links_detail() -> DataFrame:
    q_ = ("SELECT m.medicine_id,m.name_persian,df.dosage_form,r.roa,a.ATC_1,m.IRC FROM [nooredenadb].[fda].[medicines]"
          " m LEFT JOIN [nooredenadb].[fda].[ATCs] a ON m.ATC_id=a.ATC_id LEFT JOIN [nooredenadb].[fda].[dosage_forms] "
          "df ON m.dosage_form_id = df.dosage_form_id LEFT JOIN [nooredenadb].[fda].[ROAs] AS r ON m.roa_id = r.roa_id")
    db_conn = make_connection()
    links_detail_date = pd.read_sql(q_, db_conn)
    links_detail_date["name_persian"] = links_detail_date["name_persian"].str.split("(", expand=True)[0]
    db_conn.close()
    return links_detail_date

def get_link_date_prices(
        symbol: str,
        get_last_month_full: bool = False
) -> dict[str, Union[DataFrame, list, str]]:

    months = get_dates(get_last_month_full)
    links = get_last_weight_link(symbol)
    q_ = ("SELECT medicine_id, price, date FROM [nooredenadb].[fda].[medicines_history_selected] "
          f"WHERE medicine_id in {str(tuple(links["medicine_id"].values.tolist()))} and "
          f"date in {str(tuple(months["date"].values.tolist()))} ORDER BY date")
    db_conn = make_connection()
    link_date_prices = pd.read_sql(q_, db_conn)
    db_conn.close()
    link_date_prices = link_date_prices.pivot(index='medicine_id', columns='date', values='price').reset_index()
    detail_data = get_links_detail()
    links = links.merge(detail_data, on='medicine_id', how='left')
    link_date_prices = links.merge(link_date_prices, on='medicine_id', how='left')
    link_date_prices.sort_values(by=["weight"], inplace=True, ignore_index=True, ascending=False)

    chart_data = link_date_prices.drop(
        columns=[col for col in link_date_prices.columns if (("/" not in col) and (col != "weight"))], inplace=False)
    chart_data.fillna(0, inplace=True)
    weighted_avg = []
    for column in chart_data.columns[1:]:
        weighted_avg.append({"date": column, "price": round(sum(chart_data[column] * chart_data["weight"]))})
    weighted_avg = pd.DataFrame(weighted_avg)

    table_data = link_date_prices.drop(
        columns=["medicine_id", "general_name", "dosage_form", "roa", "ATC_1"], inplace=False)
    table_data.replace({c: {0: np.nan} for c in table_data.columns if "/" in c}, inplace=True)
    table_data = table_data.bfill(axis=1, inplace=False)

    date_columns = [c for c in table_data.columns if "/" in c]
    table_data = table_data[["name_persian", "weight"] + date_columns]

    pct = table_data[date_columns].pct_change(axis=1)
    weighted_returns = (pct.mul(table_data["weight"], axis=0)).sum()
    sum_df = pd.DataFrame([["رشد"] + weighted_returns.tolist()], columns=["name_persian"] + date_columns)
    sum_data = sum_df.to_dict(orient="records")

    cumulative = (1 + sum_df[date_columns].iloc[0]).cumprod() - 1
    new_row2 = pd.DataFrame([["رشد تجمعی"] + cumulative.tolist()], columns=["name_persian"] + date_columns)
    sum_data_cum = new_row2.to_dict(orient="records")

    return {"symbol": symbol, "data": link_date_prices, "weighted_avg": weighted_avg, "table_data": table_data,
            "date_columns": date_columns, "sum_data": sum_data, "sum_data_cum": sum_data_cum}

def get_cumulative_date_prices(
        get_last_month_full: bool = False
) -> dict[str, DataFrame]:

    months = get_dates(get_last_month_full)
    links = get_last_weight_link()
    q_ = ("SELECT medicine_id, price, date FROM [nooredenadb].[fda].[medicines_history_selected] "
          f"WHERE medicine_id in {str(tuple(links["medicine_id"].values.tolist()))} and "
          f"date in {str(tuple(months["date"].values.tolist()))} ORDER BY date")
    db_conn = make_connection()
    link_date_prices = pd.read_sql(q_, db_conn)
    db_conn.close()
    link_date_prices = link_date_prices.pivot(index='medicine_id', columns='date', values='price').reset_index()

    link_date_prices = links[["symbol", "weight", "medicine_id"]].merge(link_date_prices, on='medicine_id', how='left')

    link_date_prices.drop(columns=["medicine_id"], inplace=True)
    link_date_prices.replace({c: {0: np.nan} for c in link_date_prices.columns if "/" in c}, inplace=True)
    link_date_prices = link_date_prices.bfill(axis=1, inplace=False)

    date_columns = [c for c in link_date_prices.columns if "/" in c]
    pct = link_date_prices[date_columns].pct_change(axis=1)
    weights = link_date_prices["weight"]
    weighted_pct = pct.mul(weights, axis=0)
    symbol_returns = weighted_pct.join(link_date_prices["symbol"]).groupby("symbol").sum().div(
        link_date_prices.groupby("symbol")["weight"].sum(), axis=0)
    cumulative_returns = (1 + symbol_returns).cumprod(axis=1) - 1

    symbol_returns.reset_index(inplace=True)
    cumulative_returns.reset_index(inplace=True)

    return {"weighted_return": symbol_returns, "weighted_cum_return": cumulative_returns}


def get_symbols_labels() -> dict[str, Union[str, list[dict[str, str]]]]:
    query_symbols = "SELECT DISTINCT(symbol) FROM [nooredenadb].[fda].[medicines_weight]"
    symbols = pd.read_sql(query_symbols, make_connection()).rename(mapper={"symbol": "label"}, axis=1, inplace=False)
    symbols["label"] = symbols["label"].replace({"ك": "ک", "ي": "ی"}, inplace=False, regex=True)
    value = symbols["label"].iloc[0]
    symbols["value"] = symbols["label"]
    data = symbols.to_dict(orient="records")
    return {"data": data, "value": value}


if __name__ == "__main__":

    symbol_ = "دلر"
    data = get_link_date_prices(symbol=symbol_, get_last_month_full=False)

    all_symbols_data = get_cumulative_date_prices(get_last_month_full = False)
    weighted_return = all_symbols_data["weighted_return"]
    weighted_cum_return = all_symbols_data["weighted_cum_return"]

