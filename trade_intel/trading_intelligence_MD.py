import warnings
import jdatetime
import pandas as pd
import requests as rq


warnings.filterwarnings("ignore")
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36"}
indices = pd.DataFrame(data={"index": ["total_index", "price_index_eq", "investment_sector_index", "top_30_index"],
                             "index_id": ["32097828799138957", "8384385859414435", "34295935482222451", "10523825119011581"]})

####################################################################################################

cashflow = pd.read_excel(r"D:\database\trading_inteligence\cashflow.xlsx")
d_ = ["1402/11/30", "1402/12/29", "1403/01/31", "1403/02/31", "1403/03/31",
      "1403/04/31", "1403/05/31", "1403/06/31", "1403/07/30"]
cashflow_df = pd.DataFrame()
for day in d_:
    cashflow_tmp = cashflow[cashflow["date"] <= day]

    cashflow_tmp["date_diff"] = [(jdatetime.datetime.strptime(day, "%Y/%m/%d") -
                                  jdatetime.datetime.strptime(cashflow_tmp["date"].iloc[i], "%Y/%m/%d")).days
                                 for i in range(len(cashflow_tmp))]
    cashflow_tmp["date_diff_ratio"] = cashflow_tmp["date_diff"] / (
            jdatetime.datetime.strptime(day, "%Y/%m/%d") - jdatetime.datetime.strptime("1402/11/01", "%Y/%m/%d")).days
    cashflow_tmp["end_date"] = [day] * len(cashflow_tmp)
    cashflow_df = pd.concat([cashflow_df, cashflow_tmp], axis=0, ignore_index=True)
cashflow_df["weighted_cashflow"] = cashflow_df["cashflow"] * cashflow_df["date_diff_ratio"]
cashflow_df["date_"] = cashflow_df["date"].str[:-3]
cashflow_df = cashflow_df[["cashflow", "weighted_cashflow", "end_date"]].groupby(by="end_date", as_index=False).sum()
cashflow_df.to_excel(r"D:\database\trading_inteligence\cashflow_df.xlsx", index=False)
####################################################################################################

data_ = {"start_dates": [
    # "1401/04/31",
    "1402/10/30"],
         "files": [
             # "1401-04-31",
             "1402-10-30"],
         "months": [
             # ["1401/05/31", "1401/06/31", "1401/07/30", "1401/08/30", "1401/09/30", "1401/10/30", "1401/11/30",
             #  "1401/12/30", "1402/01/31", "1402/02/31", "1402/03/31", "1402/04/31", "1402/05/31", "1402/06/31",
             #  "1402/07/30", "1402/08/30", "1402/09/30", "1402/10/30"],
                    ["1402/11/30", "1402/12/30", "1403/01/31", "1403/02/31", "1403/03/31", "1403/04/31", "1403/05/31",
                     "1403/06/31", "1403/07/30",
                     # "1403/08/30", "1403/09/30", "1403/10/30"
                     ]
         ],
         "portfolio_value": [
             # 53928473161818,
             80248834273432]}

df_table = pd.DataFrame()
for d in range(len(data_["start_dates"])):
    dates = data_["months"][d]
    begin_value = data_["portfolio_value"][d]

    portfolio = pd.read_excel(f"D:/database/trading_inteligence/portfolio({data_['files'][d]}).xlsx")
    portfolio.columns.tolist()
    portfolio = portfolio[['دارایی', 'نماد', 'صنعت', 'نوع', 'تعداد', 'قیمت پایانی', 'بهای تمام شده کل']]
    portfolio.rename(mapper={"دارایی": "name", "نماد": "symbol", 'صنعت': "sector", 'نوع': "type",
                               'تعداد': "amount", "قیمت پایانی": "final_price", 'بهای تمام شده کل': "total_cost"},
                     axis=1, inplace=True)
    portfolio["symbol"].replace("دارایکم", "دارا یکم", inplace=True)
    # portfolio_1["symbol"].replace("ی", "ي", inplace=True, regex=True)
    # portfolio_1["symbol"].replace("ک", "ك", inplace=True, regex=True)
    portfolio = portfolio.iloc[:-1, :]
    portfolio = portfolio[~portfolio["type"].isin(["گواهی سپرده کالایی",
                                                         "اختیار معامله | موقعیت خرید",
                                                         "اختیار معامله | موقعیت فروش"])]
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
            portfolio["return_" + dates[j]].iloc[i] = df_returns_temp_["return"].iloc[0]

    portfolio = pd.concat([portfolio, pd.DataFrame(data={"name": ["inactivity_return"],
                                                             "symbol": ["inactivity_return"]})],
                          axis=0, ignore_index=True)
    idx = portfolio.index[portfolio["name"] == "inactivity_return"].values[0]
    for i in range(len(dates)):
        portfolio["return_" + dates[i]].iloc[idx] = sum((portfolio["return_" + dates[i]] * portfolio["share_of_portfo"])[:idx])

    bazdehi_ = pd.DataFrame(data={"date": dates})
    for i in range(len(indices)):
        res = rq.get(f"https://cdn.tsetmc.com/api/Index/GetIndexB2History/{indices['index_id'].iloc[i]}", headers=header)
        res = pd.DataFrame(res.json()["indexB2"])
        res["dEven"] = [jdatetime.datetime.fromgregorian(day=int(res["dEven"].iloc[j] % 100),
                                                         month=int((res["dEven"].iloc[j] % 10000) // 100),
                                                         year=int(res["dEven"].iloc[j] // 10000)).strftime("%Y/%m/%d")
                        for j in range(len(res))]
        res["return"] = [0] + [(res["xNivInuClMresIbs"].iloc[i] / res["xNivInuClMresIbs"].iloc[i-1]) for i in range(1, len(res))]
        res = res[res["dEven"] > data_['start_dates'][d]]
        bazdehi_[indices["index"].iloc[i] + "_return"] = [(res[res["dEven"] <= bazdehi_["date"].iloc[j]]["return"].prod()) - 1 for j in range(len(bazdehi_))]

    modified_dietz = pd.read_excel(f"D:/database/trading_inteligence/modified_dietz({data_['files'][d]}).xlsx")
    modified_dietz.dropna(subset="بازدهی", inplace=True, ignore_index=True)
    modified_dietz = modified_dietz[["تاریخ", "بازدهی"]]
    modified_dietz.rename(mapper={"تاریخ": "date", "بازدهی": "portfolio_return"}, axis=1, inplace=True)
    modified_dietz.replace("-", "/", regex=True, inplace=True)
    bazdehi_ = bazdehi_.merge(modified_dietz, on="date", how="left")

    bazdehi_["inactivity_return"] = [
        portfolio["return_" + bazdehi_["date"].iloc[i]].iloc[idx]/100 for i in range(len(bazdehi_))]
    bazdehi_ = bazdehi_[["date", "portfolio_return", "total_index_return", "price_index_eq_return",
                         "investment_sector_index_return", "top_30_index_return", "inactivity_return"]]
    bazdehi_["start_date"] = [data_["start_dates"][d].replace("-", "/")] * len(bazdehi_)
    bazdehi_["value_diff"] = round((bazdehi_["portfolio_return"] - bazdehi_["inactivity_return"]) * begin_value)


    tmp = pd.DataFrame(data={"date": [data_['start_dates'][d].replace("-", "/")],
                             "start_date": [data_['start_dates'][d].replace("-", "/")]})
    bazdehi_ = pd.concat([bazdehi_, tmp], axis=0, ignore_index=True)
    bazdehi_.fillna(value=0, inplace=True)
    bazdehi_.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)

    df_table = pd.concat([df_table, bazdehi_], axis=0, ignore_index=True)

df_table.to_excel("D:/database/trading_inteligence/trading_inteligence.xlsx", index=False)

