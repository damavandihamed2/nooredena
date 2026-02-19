import pandas as pd
import requests as rq
import warnings, jdatetime

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
res = rq.get("https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/68635710163497089/0",
             headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"})
res = pd.DataFrame(res.json()["closingPriceDaily"])
res["date"] = [jdatetime.datetime.fromgregorian(
    year=int(res["dEven"].iloc[i] // 10000),
    month=int(res["dEven"].iloc[i] % 10000 // 100),
    day=int(res["dEven"].iloc[i] % 100)).strftime(format("%Y/%m/%d")) for i in range(len(res))]

dates = pd.read_sql("SELECT DISTINCT(date) FROM [nooredenadb].[extra].[sigma_portfolio] ORDER BY date", db_conn)
dates["d"] = [dates["date"].iloc[i][:-3] for i in range(len(dates))]
months = dates.groupby(by="d", as_index=False).max()

stock_df = pd.DataFrame()
for m in range(len(months)):

    date = months["date"].iloc[m]
    portfolio = pd.read_sql(f"SELECT * FROM [nooredenadb].[extra].[sigma_portfolio] WHERE date='{date}'", db_conn)
    portfolio = portfolio[portfolio["sector"] != "ابزار پزشکی"]
    portfolio = portfolio[~portfolio["type"].isin(["اختیار معامله", "اختیار معامله | موقعیت خرید", "اختیار معامله | موقعیت فروش"])]
    portfolio = portfolio[["symbol", "sector", "amount", "total_cost", "final_price", "date"]]
    portfolio.reset_index(drop=True, inplace=True)

    if date >= "1402/06/05":
        if "همراه" in portfolio["symbol"].values:
            idx_ = portfolio.index[portfolio["symbol"] == "همراه"].values[0]
            portfolio["amount"].iloc[idx_] = 639366979
            portfolio["total_cost"].iloc[idx_] = 5744067993019
        else:
            tmp = pd.DataFrame()
            tmp["symbol"] = ["همراه"]
            tmp["sector"] = ["مخابرات"]
            tmp["amount"] = [639366979]
            tmp["total_cost"] = [5744067993019]
            tmp["date"] = [date]
            res_ = res[res["date"] <= date]
            res_.sort_values("date", ascending=False, ignore_index=True, inplace=True)
            tmp["final_price"] = [int(res_["pClosing"].iloc[0])]
            portfolio = pd.concat([portfolio, tmp], axis=0, ignore_index=True)

    portfolio["value"] = portfolio["amount"] * portfolio["final_price"]
    portfolio["symbol"] = [
        portfolio["symbol"].iloc[i][:-1] if portfolio["symbol"].iloc[i][-1] == "ح" else portfolio["symbol"].iloc[i]
        for i in range(len(portfolio))]

    portfolio = portfolio.groupby(by="symbol", as_index=False).sum(numeric_only=True)
    portfolio = portfolio[["symbol", "total_cost", "value"]]
    portfolio["date"] = [date] * len(portfolio)
    portfolio["value"] = portfolio["value"] / 1e9
    portfolio = portfolio[["symbol", "value", "date"]]
    # portfolio.rename(mapper={"value": months["d"].iloc[m]}, axis=1, inplace=True)

    stock_df = pd.concat([stock_df, portfolio], axis=0, ignore_index=True)
    # if m == 0:
    #     stock_df = pd.concat([stock_df, portfolio], axis=0, ignore_index=True)
    # else:
    #     stock_df = stock_df.merge(portfolio, on="symbol", how="outer")
# stock_df.fillna(value=0, inplace=True)
stock_df.to_excel("C:/Users/damavandi/Desktop/stock_df_raw.xlsx", index=False)

