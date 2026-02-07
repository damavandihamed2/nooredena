import warnings
import pandas as pd

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.database import make_connection


warnings.filterwarnings("ignore")
powerbi_database = make_connection()
(fnt, color1, color2) = ("B Nazanin", "#56c4cd", "#f8a81d")

market_start_date = 20240121
portfolio_start_date = "1402/11/01"
portfolio_end_date = "1403/11/01"
query_symbols = ("SELECT market_trade_value, date FROM (SELECT date_, SUM(trade_value) as market_trade_value FROM "
                 "(SELECT date as date_, [symbol_id], [trade_value] FROM [nooredenadb].[tsetmc].[symbols_history] WHERE "
                 f"date>={market_start_date}) AS TEMP1 RIGHT JOIN (SELECT symbol_id FROM [nooredenadb].[tsetmc].[symbols] "
                 "WHERE sector!='68') AS TEMP2 ON TEMP1.symbol_id=TEMP2.symbol_id WHERE date_ IS NOT NULL GROUP BY "
                 "date_) AS TEMP3 LEFT JOIN (SELECT TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) AS date_, Jalali_1 "
                 "AS date FROM [nooredenadb].[extra].[dim_date] WHERE Miladi>='2024-01-01' AND Miladi<'2026') AS TEMP4 "
                 "ON TEMP3.date_=TEMP4.date_")
query_symbols_ros = ("SELECT market_trade_value, date FROM (SELECT date as date_, SUM(trade_value) as market_trade_value"
                     f" FROM [nooredenadb].[tsetmc].[symbols_ros_history] WHERE date>={market_start_date} GROUP BY date) "
                     "AS TEMP1 LEFT JOIN (SELECT TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) AS date_, Jalali_1 AS "
                     "date FROM [nooredenadb].[extra].[dim_date] WHERE Miladi>='2024-01-01' AND Miladi<'2026') AS TEMP2 "
                     "ON TEMP1.date_=TEMP2.date_")
query_portfolio_trade_value = ("SELECT date,action,SUM(value) as portfolio_trade_value FROM "
                               f"[nooredenadb].[sigma].[sigma_buysell] WHERE date>='{portfolio_start_date}' AND "
                               f"date<'{portfolio_end_date}' AND type!='اختیار معامله' AND symbol NOT IN "
                               "('اعتماد', 'کارا', 'افران', 'لبخند', 'سپر') GROUP BY date, action  order by date")

symbols_trade_value = pd.read_sql(query_symbols, powerbi_database)
symbols_ros_trade_value = pd.read_sql(query_symbols_ros, powerbi_database)
market_trade_value = pd.concat([symbols_trade_value, symbols_ros_trade_value], axis=0, ignore_index=True)
market_trade_value = market_trade_value.groupby(by="date", as_index=False).sum()
market_trade_value = market_trade_value[market_trade_value["market_trade_value"] > 0].reset_index(
    drop=True, inplace=False)

portfolio_trade_value = pd.read_sql(query_portfolio_trade_value, powerbi_database)
portfolio_trade_value["portfolio_trade_value"] = round(portfolio_trade_value["portfolio_trade_value"]).astype(int)
portfolio_trade_value = portfolio_trade_value[portfolio_trade_value["action"] == "فروش"].reset_index(
    drop=True, inplace=False)

dataframe = market_trade_value.merge(portfolio_trade_value[["date", "portfolio_trade_value"]], on="date", how="outer")
dataframe.fillna(0, inplace=True)
dataframe["portfolio_trade_value"] = dataframe["portfolio_trade_value"].astype(int)
dataframe["market_trade_value"] /= 1e9
dataframe["portfolio_trade_value"] /= 1e9
dataframe = dataframe[dataframe["date"] < portfolio_end_date].reset_index(drop=True, inplace=False)


dim_date = pd.read_sql("SELECT [Jalali_1] as date, [jyear], [JWeekNum] FROM [nooredenadb].[extra].[dim_date] WHERE Jalali_1 >='1402/11/01' AND Jalali_1<='1403/10/30'", powerbi_database)
dim_date["jyear"] = dim_date["jyear"].astype(int)
dataframe = dataframe.merge(dim_date, on="date", how="left")

dataframe_ = dataframe.groupby(by=["jyear", "JWeekNum"], as_index=False).max()[["date", "jyear", "JWeekNum"]]
dataframe__ = dataframe.groupby(by=["jyear", "JWeekNum"], as_index=False).sum(numeric_only=True)[["jyear", "JWeekNum", "market_trade_value", "portfolio_trade_value"]]
dataframe = dataframe_.merge(dataframe__, on=["jyear", "JWeekNum"], how="left")
dataframe = dataframe[["date", "market_trade_value", "portfolio_trade_value"]]
dataframe["ratio"] = dataframe["portfolio_trade_value"] / dataframe["market_trade_value"]
dataframe.rename({"date": "تاریخ", "market_trade_value": "ارزش معاملات خرد", "portfolio_trade_value": "فروش",
                  "ratio": "نسبت فروش پرتفوی به معاملات خرد"}, axis=1, inplace=True)


fig = make_subplots(rows=1, cols=1, specs = [[{"type": "xy", "secondary_y": True}]])

fig_market = go.Scatter(x=dataframe["تاریخ"], y=dataframe["ارزش معاملات خرد"], name="ارزش معاملات خرد (میلیارد ریال)")
fig_market.update(line=dict(color=color2, smoothing=1.3, shape="spline"), mode="lines")
fig.add_trace(fig_market, row=1, col=1)

fig_portfolio = go.Scatter(x=dataframe["تاریخ"], y=dataframe["فروش"], name="فروش (میلیارد ریال)")
fig_portfolio.update(line=dict(color=color1, smoothing=1.3, shape="spline"), mode="lines")
fig.add_trace(fig_portfolio, row=1, col=1, secondary_y=True)

fig.update_xaxes(
    showticklabels=True,
    tickfont=dict(family=fnt, size=16),
    calendar="jalali",
    tickangle=45,
    ticklabelstep=3,
    tickformat="%Y/%m/%d",
    showgrid=False,
    title=dict(text="تاریخ", font=dict(family=fnt, size=18, weight="bold")),
    row=1, col=1)

fig.update_yaxes(
    tickfont=dict(family=fnt, size=16),
    exponentformat="none",
    separatethousands=True,
    side="right",
    tickprefix=" ",
    fixedrange=False,
    showgrid=False,
    title=dict(text="ارزش معاملات خرد (میلیارد ریال)", font=dict(family=fnt, size=18, weight="bold")),
    row=1, col=1)
fig.update_yaxes(
    tickfont=dict(family=fnt, size=16),
    exponentformat="none",
    separatethousands=True,
    side="left",
    tickprefix=" ",
    fixedrange=False,
    showgrid=True,
    secondary_y=True,
    title=dict(text="فروش (میلیارد ریال)", font=dict(family=fnt, size=18, weight="bold")),
    row=1, col=1)

fig.update_layout(
    title=dict(text="روند ارزش معاملات خرد بازار و فروش سهام پرتفوی",
               font=dict(size=25, family=fnt, weight="bold"), xanchor="center", yanchor='middle', x=0.5, y=0.98),
    template="seaborn",
    margin=dict(l=80, r=20, t=100, b=80),
    showlegend=True,
    legend=dict(visible=True, orientation='h', x=0.48, y=1.035, xanchor='center', yanchor='middle',
                font=dict(family=fnt, size=16, weight="bold"), bgcolor="rgba(255, 255, 255, 0)")
)

fig.write_html("c:/users/h.damavandi/desktop/test.html", config={"displayModeBar": False})



fig_ratio = go.Scatter(x=dataframe["تاریخ"], y=dataframe["نسبت فروش پرتفوی به معاملات خرد"], name="نسبت فروش پرتفوی به معاملات خرد")
fig_ratio.update(line=dict(color=color2, smoothing=1.05, shape="spline"), mode="lines")
fig_ = go.Figure(fig_ratio)
fig_.update_xaxes(
    showticklabels=True,
    tickfont=dict(family=fnt, size=16),
    calendar="jalali",
    tickangle=30,
    ticklabelstep=2,
    tickformat="%Y/%m/%d",
    showgrid=False,
    title=dict(text="تاریخ", font=dict(family=fnt, size=18, weight="bold"))
)

fig_.update_yaxes(
    tickfont=dict(family=fnt, size=18, weight="bold"),
    exponentformat="none",
    tickformat=".1%",
    ticksuffix=" ",
    # tickprefix=" ",
    fixedrange=False,
    showgrid=True,
    title=dict(text="نسبت فروش پرتفوی به معاملات خرد", font=dict(family=fnt, size=18, weight="bold"))
)

fig_.update_layout(
    title=dict(text="روند نسبت فروش پرتفوی به معاملات خرد",
               font=dict(size=25, family=fnt, weight="bold"), xanchor="center", yanchor='middle', x=0.5, y=0.98),
    template="seaborn",
    margin=dict(l=80, r=20, t=50, b=80),
    # showlegend=True,
    # legend=dict(visible=True, orientation='h', x=0.48, y=1.035, xanchor='center', yanchor='middle',
    #             font=dict(family=fnt, size=16, weight="bold"), bgcolor="rgba(255, 255, 255, 0)")
)

fig_.write_html("c:/users/h.damavandi/desktop/fig_ratio.html", config={"displayModeBar": False})
