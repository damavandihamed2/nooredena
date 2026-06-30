import warnings
import pandas as pd



warnings.filterwarnings("ignore")

symbols_market_cap = pd.read_excel("./mci/racing_bar_chart/data.xlsx")
symbols_market_cap = symbols_market_cap[["symbol", "date", "market_cap"]]




dates = symbols_market_cap["date"].unique().tolist()
df = pd.DataFrame()
for date in dates:
    tmp = symbols_market_cap[symbols_market_cap["date"] == date].sort_values(by="market_cap", ascending=False, inplace=False, ignore_index=True)
    n = 15
    if "اخابر" not in tmp["symbol"][:15].values.tolist():
        n -= 1
    if "همراه" not in tmp["symbol"][:15].values.tolist():
        n -= 1
    tmp_ = tmp[tmp["symbol"].isin(["اخابر", "همراه"])]
    tmp = tmp[: n]
    tmp = pd.concat([tmp, tmp_], ignore_index=True, axis=0).drop_duplicates(keep="first")
    df = pd.concat([df, tmp], ignore_index=True, axis=0)

df.sort_values(by=["date", "market_cap"], ascending=[True, False], inplace=True , ignore_index=True)
df = df.pivot(columns="date", values="market_cap", index="symbol")
df.to_excel("./mci/racing_bar_chart/df.xlsx")




