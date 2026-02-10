import warnings
import numpy as np
import pandas as pd
from tqdm import tqdm
from utils.database import make_connection

warnings.filterwarnings("ignore")
db_conn = make_connection()

dim_date = pd.read_sql("SELECT TRY_CONVERT(INT, REPLACE(Miladi, '-', '')) date, Jalali_1 date_j FROM "
                       "[nooredenadb].[extra].[dim_date]", db_conn)
top500_last_cap = pd.read_sql("SELECT TOP (500) symbol_id, total_share FROM [nooredenadb].[tsetmc].[symbols] WHERE"
                              " active = 1 AND sector <> '68' AND symbol_code NOT LIKE 'IRO1OS%' ORDER BY "
                              "(total_share * final_price) DESC, symbol_id", db_conn)
top500_history = pd.read_sql("SELECT sh.date, s.symbol_id, sh.final_price, sh.trade_amount FROM (SELECT TOP (500) "
                             "symbol_id FROM [nooredenadb].[tsetmc].[symbols] WHERE active = 1 AND sector <> '68' AND"
                             " symbol_code NOT LIKE 'IRO1OS%' ORDER BY (total_share * final_price) DESC, symbol_id) s"
                             " LEFT JOIN [nooredenadb].[tsetmc].[symbols_history] AS sh ON sh.symbol_id = s.symbol_id"
                             " ORDER BY s.symbol_id, sh.date;", db_conn)
capital_change = pd.read_sql("SELECT scc.date, s.symbol_id, scc.capital_old, scc.capital_new FROM (SELECT TOP (500) "
                             "symbol_id FROM [nooredenadb].[tsetmc].[symbols] WHERE active = 1 AND sector <> '68' AND"
                             " symbol_code NOT LIKE 'IRO1OS%' ORDER BY (total_share * final_price) DESC, symbol_id) AS"
                             " s INNER JOIN [nooredenadb].[tsetmc].[symbols_capital_change] AS scc ON scc.symbol_id = "
                             "s.symbol_id ORDER BY s.symbol_id, scc.[date];", db_conn)


top500_history_merged = top500_history.merge(capital_change, on=["symbol_id", "date"], how="outer")
top500_history_merged.sort_values(by=["symbol_id", "date"], ascending=True, inplace=True, ignore_index=True)
cap_new_ffill = top500_history_merged.groupby("symbol_id")["capital_new"].ffill()
first_cap_old = top500_history_merged.groupby("symbol_id")["capital_old"].transform(
    lambda s: s.dropna().iloc[0] if s.notna().any() else np.nan)
top500_history_merged["capital"] = cap_new_ffill.fillna(first_cap_old)
top500_history_merged = top500_history_merged.merge(top500_last_cap, on="symbol_id", how="left")
top500_history_merged["capital"].fillna(top500_history_merged["total_share"], inplace=True)
top500_history_merged["market_cap"] = top500_history_merged["final_price"] * top500_history_merged["capital"]
top500_history_merged = top500_history_merged.merge(dim_date, on="date", how="left")[
    ["date_j", "symbol_id", "market_cap"]].rename({"date_j": "date"}, axis=1, inplace=False)
top500_history_merged.sort_values(by=["date", "symbol_id"], inplace=True, ignore_index=True)

all_dates = pd.Index(top500_history_merged["date"].unique(), name="date").sort_values()
all_symbols = pd.Index(top500_history_merged["symbol_id"].unique(), name="symbol_id").sort_values()
skeleton = (pd.MultiIndex.from_product([all_dates, all_symbols], names=["date", "symbol_id"]).to_frame(index=False))
top500_history_merged = skeleton.merge(top500_history_merged[["date", "symbol_id", "market_cap"]],
                                       on=["date", "symbol_id"], how="left")

top500_history_merged.sort_values(by=["symbol_id", "date"], inplace=True, ignore_index=True)
top500_history_merged["market_cap"] = top500_history_merged.groupby("symbol_id")["market_cap"].ffill()
top500_history_merged.sort_values(by=["date", "symbol_id"], inplace=True, ignore_index=True)
top500_history_merged.dropna(subset=["market_cap"], inplace=True, ignore_index=True)
top500_history_merged = top500_history_merged[
    (top500_history_merged["date"] <= "1404/10/30") &
(top500_history_merged["date"] >= "1399/12/27")
].reset_index(drop=True, inplace=False)

top500_history_merged["year-month"] = top500_history_merged["date"].str[:7]
month_last = top500_history_merged[["year-month", "date"]].groupby("year-month", as_index=False).max()


top30_df = pd.DataFrame()
return_df = pd.DataFrame()
for i in tqdm(range(len(month_last) - 1)):
    d_ = month_last["date"].iloc[i]
    d_next = month_last["date"].iloc[i + 1]
    ym_ = month_last["year-month"].iloc[i]
    ym_next = month_last["year-month"].iloc[i + 1]

    df_temp = top500_history_merged[top500_history_merged["date"] == d_].sort_values(by="market_cap", ascending=False)
    df_temp = df_temp.iloc[:30].reset_index(drop=True)
    df_temp_next = top500_history_merged[top500_history_merged["date"] == d_next].sort_values(
        by="market_cap", ascending=False, ignore_index=True)[["symbol_id", "market_cap"]].rename({"market_cap": "market_cap_next"}, axis=1, inplace=False)
    df_temp = df_temp.merge(df_temp_next, on="symbol_id", how="left")
    df_temp["from"] = d_
    df_temp["to"] = d_next
    df_temp["return"] = (df_temp["market_cap_next"] / df_temp["market_cap"]) - 1
    df_temp = df_temp[["from", "to", "symbol_id", "market_cap", "market_cap_next", "return"]]
    top30_df = pd.concat([top30_df, df_temp], axis=0, ignore_index=True)

    return_temp = pd.DataFrame([{"from": d_, "to": d_next}])
    return_temp["return"] = df_temp["return"].mean()
    return_temp["return_weighted"] = ((df_temp["market_cap"] / df_temp["market_cap"].sum()) * df_temp["return"]).sum()
    return_df = pd.concat([return_df, return_temp], axis=0, ignore_index=True)

((1 + -0.00382) * (1 + -0.00078) * (1 + 0.15099) * (1 + -0.03530) * (1 + -0.04719) * (1 + -0.05302) * (1 + -0.16110) * (1 + 0.01986) * (1 + 0.21595) * (1 + -0.04647) * (1 + 0.26122) * (1 + 0.18353)) - 1



