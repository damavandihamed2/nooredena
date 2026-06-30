import pandas as pd

symbols_1404 = pd.read_excel("./mci/racing_bar_chart/symbols_1404.xlsx")
symbols_1404 = symbols_1404[["symbol", "market_cap", "rank"]].rename(
    {"market_cap": "market_cap_1404", "rank": "rank_1404"}, axis=1, inplace=False)

symbols_1405 = pd.read_excel("./mci/racing_bar_chart/symbols_1405.xlsx")
symbols_1405 = symbols_1405[["symbol", "market_cap", "rank"]].rename(
    {"market_cap": "market_cap_1405", "rank": "rank_1405"}, axis=1, inplace=False)


symbols = symbols_1404.merge(symbols_1405, on="symbol", how="outer")
symbols["symbol"] = symbols["symbol"] + "1"
symbols.to_excel("./mci/racing_bar_chart/symbols.xlsx", index=False)
