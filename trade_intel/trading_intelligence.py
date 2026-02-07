import warnings
import jdatetime
import pandas as pd
import requests as rq

from utils.database import make_connection

warnings.filterwarnings("ignore")
powerbi_database = make_connection()
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36"}
indices = pd.DataFrame(data={
    "index": ["total_index", "price_index_eq", "investment_sector_index", "top_30_index"],
    "index_id": ["32097828799138957", "8384385859414435", "34295935482222451", "10523825119011581"]})

data_ = {
    "start_dates": ["1403/10/30"],
    "files": ["1403-10-30"],
    "months": [
        ["1403/11/30", "1403/12/30", "1404/01/31"]
    ]
}

df_table = pd.DataFrame()
for d in range(len(data_["start_dates"])):
    d = 0
    dates = data_["months"][d]
    portfolio = pd.read_sql(
        sql=f"SELECT [symbol], [type], [amount], [final_price], [total_cost] FROM "
            f"[nooredenadb].[sigma].[sigma_portfolio] WHERE date = '{data_['start_dates'][d]}' AND "
            f"type NOT IN ('گواهی سپرده کالایی', 'اختیار معامله | موقعیت خرید', 'اختیار معامله | موقعیت فروش')",
        con=powerbi_database)

    portfolio["total_cost"] += ((portfolio["type"] == "حق تقدم") * 1000) * portfolio["amount"]
    portfolio["symbol"] = portfolio["symbol"].str.rstrip("ح")
    portfolio_ = portfolio[["symbol", "final_price"]].sort_values(
        by="symbol", ascending=True, inplace=False).drop_duplicates(
        subset="symbol", keep="first", inplace=False, ignore_index=True)
    portfolio = portfolio[["symbol",  "amount", "total_cost"]].groupby(by="symbol", as_index=False).sum()
    portfolio = portfolio.merge(portfolio_, on="symbol", how="left")
    begin_value = (portfolio["final_price"] * portfolio["amount"]).sum()

    portfolio["symbol"].replace("دارایکم", "دارا یکم", inplace=True)
    portfolio.reset_index(drop=True, inplace=True)
    portfolio["value"] = portfolio["amount"] * portfolio["final_price"]
    portfolio["share_of_portfo"] = portfolio["value"] / sum(portfolio["value"])

    df_returns = pd.read_excel(f"D:/database/trading_inteligence/return({data_['files'][d]}).xlsx")
    df_returns = df_returns[['نماد', 'تاریخ', 'حق تقدم', 'سهام جایزه', 'DPS', 'قیمت پایانی سهم', 'تفاوت قیمت',
                             'قیمت پایانی حق‌تقدم', '% بازدهی']]
    df_returns.rename(mapper={"نماد": "symbol", "تاریخ": "date", "حق تقدم": "share_right", "سهام جایزه": "share_bonus",
                              "DPS": "dps", "قیمت پایانی سهم": "final_price", "تفاوت قیمت": "price_diff",
                              "قیمت پایانی حق‌تقدم": "share_right_price", "% بازدهی": "return"},
                      axis=1, inplace=True)
    df_returns.dropna(subset="date", axis=0, inplace=True, ignore_index=True)

    for i in range(len(dates)):
        portfolio["return_" + dates[i]] = [0] * len(portfolio)

    for i in range(len(portfolio)):
        df_returns_temp = df_returns[df_returns["symbol"] == portfolio["symbol"].iloc[i]]
        df_returns_temp.reset_index(drop=True, inplace=True)
        for j in range(len(dates)):
            df_returns_temp_ = df_returns_temp[df_returns_temp["date"] <= dates[j]]
            df_returns_temp_.sort_values(by="date", ascending=False, ignore_index=True, inplace=True)
            if len(df_returns_temp_) > 0:
                portfolio["return_" + dates[j]].iloc[i] = df_returns_temp_["return"].iloc[0]
            else:
                portfolio["return_" + dates[j]].iloc[i] = 0


    portfolio = pd.concat([portfolio,
                           pd.DataFrame([{"symbol": "inactivity_return"}])],
                          axis=0, ignore_index=True)

    idx = portfolio.index[portfolio["symbol"] == "inactivity_return"].values[0]

    for i in range(len(dates)):
        portfolio["return_" + dates[i]].iloc[idx] = sum(
            (portfolio["return_" + dates[i]] * portfolio["share_of_portfo"])[:idx])

    bazdehi_ = pd.DataFrame(data={"date": dates})
    for i in range(len(indices)):
        res = rq.get(f"https://cdn.tsetmc.com/api/Index/GetIndexB2History/{indices['index_id'].iloc[i]}",
                     headers=header)
        res = pd.DataFrame(res.json()["indexB2"])
        res["dEven"] = [jdatetime.datetime.fromgregorian(day=int(res["dEven"].iloc[j] % 100),
                                                         month=int((res["dEven"].iloc[j] % 10000) // 100),
                                                         year=int(res["dEven"].iloc[j] // 10000)).strftime("%Y/%m/%d")
                        for j in range(len(res))]
        res["return"] = [0] + [
            (res["xNivInuClMresIbs"].iloc[i] / res["xNivInuClMresIbs"].iloc[i-1]) for i in range(1, len(res))]
        res = res[res["dEven"] > data_['start_dates'][d]]
        bazdehi_[indices["index"].iloc[i] + "_return"] = [
            (res[res["dEven"] <= bazdehi_["date"].iloc[j]]["return"].prod()) - 1 for j in range(len(bazdehi_))]

    bazdehi_active = pd.read_excel(f"D:/database/trading_inteligence/bazdehi_active.xlsx")
    bazdehi_active["return"] = bazdehi_active["return"] + 1
    bazdehi_active["return_aggregate"] = bazdehi_active[["return"]].cumprod(axis=0)
    bazdehi_active["return_aggregate"] = bazdehi_active["return_aggregate"] - 1
    bazdehi_active.rename(mapper={"return_aggregate": "portfolio_return_active"}, axis=1, inplace=True)
    bazdehi_ = bazdehi_.merge(bazdehi_active, on="date", how="left")

    # bazdehi_passive = pd.read_excel(f"D:/database/trading_inteligence/bazdehi_passive.xlsx")
    # bazdehi_passive["return"] = bazdehi_passive["return"] + 1
    # bazdehi_passive["return_aggregate"] = bazdehi_passive[["return"]].cumprod(axis=0)
    # bazdehi_passive["return_aggregate"] = bazdehi_passive["return_aggregate"] - 1
    # bazdehi_passive.rename(mapper={"return_aggregate": "portfolio_return_passive"}, axis=1, inplace=True)
    # bazdehi_ = bazdehi_.merge(bazdehi_passive, on="date", how="left")


    bazdehi_["inactivity_return"] = [
        portfolio["return_" + bazdehi_["date"].iloc[i]].iloc[idx]/100 for i in range(len(bazdehi_))]
    bazdehi_ = bazdehi_[["date",
                         "portfolio_return_active",
                         # "portfolio_return_passive",
                         "total_index_return",
                         "price_index_eq_return",
                         "investment_sector_index_return",
                         "top_30_index_return",
                         "inactivity_return"]]
    bazdehi_["start_date"] = [data_["start_dates"][d].replace("-", "/")] * len(bazdehi_)

    bazdehi_["value_diff_inactivity"] = round(
        (bazdehi_["portfolio_return_active"] - bazdehi_["inactivity_return"]) * begin_value)
    # bazdehi_["value_diff_passive"] = round(
    #     (bazdehi_["portfolio_return_active"] - bazdehi_["portfolio_return_passive"]) * begin_value)

    tmp = pd.DataFrame(data={"date": [data_['start_dates'][d].replace("-", "/")],
                             "start_date": [data_['start_dates'][d].replace("-", "/")]})
    bazdehi_ = pd.concat([bazdehi_, tmp], axis=0, ignore_index=True)
    bazdehi_.fillna(value=0, inplace=True)
    bazdehi_.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)

    df_table = pd.concat([df_table, bazdehi_], axis=0, ignore_index=True)

df_table.to_excel("D:/database/trading_inteligence/trading_inteligence.xlsx", index=False)
