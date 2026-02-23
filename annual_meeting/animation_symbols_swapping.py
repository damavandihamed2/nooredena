import warnings
import numpy as np
import pandas as pd

import plotly.graph_objects as go

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()


fnt = "B Nazanin"
color_green, color_red, color1, color2 = "#00ff00", "#ff0000", "#56c4cd", "#f8a81d"
stock_swaps = [{"name": "swap1", "sell": "هرمز", "buy": "كچاد", "date": "1402/11/25", "yrange": [-0.4, 0.3]},
               {"name": "swap2", "sell": "كاوه", "buy": "كچاد", "date": "1402/12/05", "yrange": [-0.4, 0.3]},
               {"name": "swap3", "sell": "سرود", "buy": "سهرمز", "date": "1403/05/01", "yrange": [-0.3, 1.4]},
               {"name": "swap4", "sell": "وپخش", "buy": "دلر", "date": "1403/07/16", "yrange": [-0.2, 0.55]},
               {"name": "swap5", "sell": "دالبر", "buy": "دلر", "date": "1403/07/16", "yrange": [-0.25, 0.55]},
               {"name": "swap6", "sell": "كنور", "buy": "ومعادن", "date": "1403/08/14", "yrange": [-0.1, 0.55]},
               # {"name": "swap7", "sell": "تيپيكو", "buy": "دلر", "date": "1403/07/01", "yrange": [-0.4, 0.3]},
               # {"name": "swap8", "sell": "فاسمين", "buy": "فخوز", "date": "1403/08/20", "yrange": [-0.4, 0.3]}
               ]


def make_swap_figs(symbol_sell, symbol_buy, swap_date, yaxis_range, name):

    # symbol_sell = stock_swaps[2]["sell"]
    # symbol_buy = stock_swaps[2]["buy"]
    # swap_date = stock_swaps[2]["date"]
    # yaxis_range = stock_swaps[2]["yrange"]
    # name = stock_swaps[2]["name"]

    query_symbols = ("SELECT date,symbol,ROUND((final_price * exp(sum(log(alpha)) OVER (PARTITION BY symbol ORDER BY date"
                     " DESC))),0) AS adj_final_price FROM (SELECT TEMP3.date,TEMP2.symbol,TEMP1.yesterday_price,"
                     "TEMP1.final_price,(CASE WHEN ((CAST(LAG(TEMP1.yesterday_price) OVER (PARTITION BY symbol ORDER BY"
                     " date DESC) AS float)/CAST(TEMP1.final_price AS float)) IS NOT NULL) THEN (CAST(LAG("
                     "TEMP1.yesterday_price) OVER (PARTITION BY symbol ORDER BY date DESC) AS float)/CAST("
                     "TEMP1.final_price AS float)) ELSE 1 END) AS alpha FROM (SELECT [date] AS mdate,[symbol_id],"
                     "[yesterday_price],[final_price] FROM [nooredenadb].[tsetmc].[symbols_history] WHERE date > 20240121) "
                     "AS TEMP1 JOIN (SELECT symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols] WHERE symbol IN "
                     f"('{symbol_buy}', '{symbol_sell}') AND active=1) AS TEMP2 ON TEMP1.symbol_id=TEMP2.symbol_id JOIN (SELECT "
                     "TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) AS mdate, [Jalali_1] AS date FROM "
                     "[nooredenadb].[extra].[dim_date]) AS TEMP3 ON TEMP1.mdate=TEMP3.mdate) AS TEMP4")
    symbols = pd.read_sql(query_symbols, db_conn)
    symbols = symbols[symbols["date"] >= "1402/11/01"].sort_values(by=["symbol", "date"], ascending=True, ignore_index=True, inplace=False)
    symbols_base_price = pd.DataFrame({"symbol": [symbol_sell, symbol_buy], "date": [swap_date, swap_date]}).merge(symbols, on=["symbol", "date"], how="left").rename({"adj_final_price": "base_price"}, axis=1, inplace=False)[["symbol", "base_price"]]
    symbols_ = symbols.merge(symbols_base_price, on=["symbol"], how="left")
    symbols_["return"] = (symbols_["adj_final_price"] / symbols_["base_price"]) - 1
    symbols_["color"] = (symbols_["symbol"] == symbol_buy) * pd.Series(["green"] * len(symbols_)) + (symbols_["symbol"] == symbol_sell) * pd.Series(["red"] * len(symbols_))

    symbols_ = symbols_[~((symbols_["date"] == "1402/12/19") & (symbols_["symbol"] == "كچاد"))].reset_index(drop=True, inplace=False)
    # symbols_ = symbols_[~((symbols_["date"] == "1403/08/02") & (symbols_["symbol"] == "سهرمز"))].reset_index(drop=True, inplace=False)
    # symbols_ = symbols_[~((symbols_["date"] == "1403/08/02") & (symbols_["symbol"] == "سرود"))].reset_index(drop=True, inplace=False)
    # symbols_ = symbols_[~((symbols_["date"] == "1403/08/05") & (symbols_["symbol"] == "سهرمز"))].reset_index(drop=True, inplace=False)
    # symbols_ = symbols_[~((symbols_["date"] == "1403/08/05") & (symbols_["symbol"] == "سرود"))].reset_index(drop=True, inplace=False)

    days_ = sorted(symbols_["date"].unique())
    text = f"نقطه ی جابجایی {symbol_buy} و {symbol_sell}"
    annotation = go.layout.Annotation(
        x=swap_date, y=0, text=text, showarrow=True, align="center", arrowhead=6, arrowsize=2, arrowwidth=2,
        font=dict(family=fnt, size=14, weight="bold", color="black"), ax=-30, ay=-200, arrowcolor="DarkSlateGray",
        bordercolor="Gainsboro", borderwidth=3, borderpad=3, bgcolor=color1, opacity=0.8)

    xaxes_ = dict(tickfont=dict(family=fnt, size=18), calendar="jalali", tickformat="%Y/%m/%d", tickangle=30,
                  ticklabelstep=3, showgrid=False,
                  # range=[days_[0], days_[-1]],
                  title=dict(text="تاریخ", font=dict(family=fnt, size=20, weight="bold")))

    yaxes_ = dict(tickfont=dict(family=fnt, size=18), exponentformat="none", tickformat=",.0%", side="right",
                  tickprefix=" ",
                  title=dict(text="درصد بازدهی", font=dict(family=fnt, size=20, weight="bold")), fixedrange=False)

    def make_fig(date_: str):

        if date_ < swap_date:

            # symbol_temp = symbols_[symbols_["date"] <= date_]
            symbol_temp = symbols_.copy()
            symbol_temp.loc[symbol_temp['date'] > date_, 'return'] = np.nan

            # tmp_sell_1 = symbol_temp[(symbol_temp["symbol"] == symbol_sell) & (symbol_temp["date"] <= swap_date)].reset_index(drop=True, inplace=False)
            tmp_sell_1 = symbol_temp[(symbol_temp["symbol"] == symbol_sell)].reset_index(drop=True, inplace=False)
            fig_sell_1 = go.Scatter(x=tmp_sell_1["date"], y=tmp_sell_1["return"], name=symbol_sell,
                                    line=dict(color="red", width=2))

            # tmp_buy_1 = symbol_temp[(symbol_temp["symbol"] == symbol_buy) & (symbol_temp["date"] <= swap_date)].reset_index(drop=True, inplace=False)
            tmp_buy_1 = symbol_temp[(symbol_temp["symbol"] == symbol_buy)].reset_index(drop=True, inplace=False)
            fig_buy_1 = go.Scatter(x=tmp_buy_1["date"], y=tmp_buy_1["return"], name=symbol_buy,
                                   line=dict(color="green", width=2))
            # fig_buy_1 = go.Scatter()

            annotation_ = []
            data_ = [fig_sell_1, fig_buy_1, go.Scatter(), go.Scatter()]

            return {
                "frame": go.Frame(data=data_, layout=go.Layout(annotations=annotation_, xaxis=xaxes_, yaxis=yaxes_)),
                "data": data_,
                "annotation": annotation_
            }

        else:

            # symbol_temp = symbols_[symbols_["date"] <= date_]
            symbol_temp = symbols_.copy()
            symbol_temp.loc[symbol_temp['date'] > date_, 'return'] = np.nan

            # tmp_sell_1 = symbol_temp[(symbol_temp["symbol"] == symbol_sell) & (symbol_temp["date"] <= swap_date)].reset_index(drop=True, inplace=False)
            tmp_sell_1 = symbol_temp[(symbol_temp["symbol"] == symbol_sell)].reset_index(drop=True, inplace=False)
            fig_sell_1 = go.Scatter(x=tmp_sell_1["date"], y=tmp_sell_1["return"], name=symbol_sell,
                                    line=dict(color="red", width=2))

            # tmp_buy_1 = symbol_temp[(symbol_temp["symbol"] == symbol_buy) & (symbol_temp["date"] <= swap_date)].reset_index(drop=True, inplace=False)
            tmp_buy_1 = symbol_temp[(symbol_temp["symbol"] == symbol_buy)].reset_index(drop=True, inplace=False)
            fig_buy_1 = go.Scatter(x=tmp_buy_1["date"], y=tmp_buy_1["return"], name=symbol_buy,
                                   line=dict(color="green", width=2))
            # fig_buy_1 = go.Scatter()

            tmp_sell_2 = symbol_temp[
                (symbol_temp["symbol"] == symbol_sell) & (symbol_temp["date"] > swap_date)].reset_index(drop=True,
                                                                                                        inplace=False)
            # tmp_sell_2 = symbol_temp[(symbol_temp["symbol"] == symbol_sell)].reset_index(drop=True, inplace=False)
            fig_sell_2 = go.Scatter(x=tmp_sell_2["date"], y=tmp_sell_2["return"], name=symbol_sell,
                                    line=dict(color="red", width=2))

            tmp_buy_2 = symbol_temp[
                (symbol_temp["symbol"] == symbol_buy) & (symbol_temp["date"] > swap_date)].reset_index(drop=True,
                                                                                                       inplace=False)
            # tmp_buy_2 = symbol_temp[(symbol_temp["symbol"] == symbol_buy)].reset_index(drop=True, inplace=False)
            fig_buy_2 = go.Scatter(x=tmp_buy_2["date"], y=tmp_buy_2["return"], name=symbol_buy,
                                   line=dict(color="green", width=2), fill="tonexty")

            data_ = [
                fig_sell_1, fig_buy_1,
                fig_sell_2, fig_buy_2
            ]
            annotation_ = [annotation]

            return {
                "frame": go.Frame(data=data_, layout=go.Layout(annotations=annotation_, xaxis=xaxes_, yaxis=yaxes_)),
                "data": data_,
                "annotation": annotation_
            }


    fig = go.Figure(
        data=make_fig(days_[0])["data"],

        layout=go.Layout(
            legend = dict(orientation='h', x=0.5, y=1.1, xanchor='center', yanchor='middle',
                          font=dict(family=fnt, size=16, weight=900), title=""),
            font_family=fnt,
            margin=dict(l=50, r=50, t=50, b=50),
            template="seaborn",
            # xaxis=dict(range=[days_[0], days_[-1]], autorange=False),
            yaxis=dict(range=yaxis_range, autorange=False),
            updatemenus=[
                dict(
                    type="buttons",
                    buttons=[
                        dict(
                            label="Play",
                            method="animate",
                            args=[
                                None,
                                {"frame": {"duration": 100, "redraw": True},
                                 "transition": {"duration": 0, "easing": "sin"}}
                            ]
                        )
                    ]
                )
            ]
        ),
        frames=[make_fig(days_[d])["frame"] for d in range(1, len(days_))]
    )

    fig.update_xaxes(
        tickfont=dict(family=fnt, size=18), calendar="jalali", tickformat="%Y/%m/%d", tickangle=30,ticklabelstep=3,
        showgrid=False, title=dict(text="تاریخ", font=dict(family=fnt, size=20, weight="bold")),
        range=[days_[0], days_[-1]]
    )
    fig.update_yaxes(tickfont=dict(family=fnt, size=18), exponentformat="none", tickformat=",.0%", side="right",
                     tickprefix=" ", fixedrange=False,
                     title=dict(text="درصد بازدهی", font=dict(family=fnt, size=20, weight="bold")))

    fig.write_html(
        f"c:/users/h.damavandi/desktop/symbol_trades_{name}.html", config={"displayModeBar": False}, auto_play=True,
        animation_opts=dict(frame=dict(duration=100, redraw=True), transition=dict(duration=0, easing="sin"))
    )

for ss in range(len(stock_swaps)):
    make_swap_figs(symbol_sell=stock_swaps[ss]["sell"],
                   symbol_buy=stock_swaps[ss]["buy"],
                   swap_date=stock_swaps[ss]["date"],
                   name=stock_swaps[ss]["name"],
                   yaxis_range=stock_swaps[ss]["yrange"])
