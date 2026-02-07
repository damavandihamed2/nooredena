import pandas as pd
import requests as rq
from time import sleep
import random, datetime, warnings, jdatetime

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.database import make_connection


warnings.filterwarnings("ignore")

closed_days = ["1401-12-17", "1401-12-29", "1402-01-01", "1402-01-02", "1402-01-12", "1402-01-13", "1402-01-23",
               "1402-02-02", "1402-02-03", "1402-02-26", "1402-03-14", "1402-03-15", "1402-06-15", "1402-06-25",
               "1402-07-02", "1402-07-11", "1402-09-26", "1402-11-22", "1402-12-06", "1402-12-29", "1403-01-01"]

today = jdatetime.datetime.today()
today_ = today.strftime("%Y-%m-%d")
today__ = today.strftime("%Y/%m/%d")
weekday = today.weekday()
fiscal_year = "1401-11-01"
one_week = (today - datetime.timedelta(days=today.weekday() + 3)).strftime("%Y-%m-%d")
one_month = (today - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
two_month = (today - datetime.timedelta(days=61)).strftime("%Y-%m-%d")
three_month = (today - datetime.timedelta(days=91)).strftime("%Y-%m-%d")
six_month = (today - datetime.timedelta(days=181)).strftime("%Y-%m-%d")
nine_month = (today - datetime.timedelta(days=271)).strftime("%Y-%m-%d")
one_year = (today - datetime.timedelta(days=366)).strftime("%Y-%m-%d")

# names = pd.read_excel("//filesrv/Public/گزارش روزانه/Backup/name_amount.xlsx")

fnt = "B Nazanin"
color1 = "#56c4cd"
color2 = "#f8a81d"
col_pos = "#018501"
col_neg = "#cc0202"
col_black = "#000000"
col_neu = "#858585"

clr = {"7": "#00ff00", "6": "#00ff00", "5": "#00ff00", "4": "#00c800", "3": "#009600",
       "2": "#007d00", "1": "#006400", "0": "#e8e8e8", "-1": "#640000", "-2": "#7d0000",
       "-3": "#960000", "-4": "#c80000", "-5": "#ff0000", "-6": "#ff0000", "-7": "#ff0000"}

clr_ = {"7": "#00ff00", "6": "#00ff00", "5": "#00ff00", "4": "#00c800", "3": "#009600",
        "2": "#007d00", "1": "#006400", "0": "#636363", "-1": "#640000", "-2": "#7d0000",
        "-3": "#960000", "-4": "#c80000", "-5": "#ff0000", "-6": "#ff0000", "-7": "#ff0000"}

clr_font = {"7": "#000000", "6": "#000000", "5": "#000000", "4": "#000000", "3": "#ffffff",
            "2": "#ffffff", "1": "#ffffff", "0": "#000000", "-1": "#ffffff", "-2": "#ffffff",
            "-3": "#ffffff", "-4": "#ffffff", "-5": "#ffffff", "-6": "#ffffff", "-7": "#ffffff"}


def adj_final_price_date(dataframe, date):
    dataframe_ = dataframe[dataframe["date"] <= date]
    if len(dataframe_) == 0:
        price = dataframe["final_price_adj"].iloc[dataframe.index[dataframe["date"] == min(dataframe["date"])].values[0]]
    else:
        price = dataframe["final_price_adj"].iloc[dataframe.index[dataframe["date"] == max(dataframe_["date"])].values[0]]
    return price


powerbi_database = make_connection()

#######################################################################################################################
#######################################################################################################################

print("loading today's market data and updating last price")
base_url = "http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0"
while True:
    try:
        response = rq.get(base_url)
        if (response.status_code == 200) & (len(response.text.split("@")) > 3):
            break
        sleep(random.randint(1, 4))
    except Exception as e:
        print(e)
response = response.text
first_part, second_part, symbols_data, order_book_data, *other = response.split("@")
symbols_data = symbols_data.split(";")
symbols_data = {
"id": [int(symbols_data[i].split(",")[0]) for i in range(len(symbols_data))],
"symbol": [symbols_data[i].split(",")[2] for i in range(len(symbols_data))],
"symbol_name": [symbols_data[i].split(",")[3] for i in range(len(symbols_data))],
"final_price": [int(symbols_data[i].split(",")[6]) for i in range(len(symbols_data))],
"yesterday_price": [int(symbols_data[i].split(",")[13]) for i in range(len(symbols_data))]
}
symbols_data = pd.DataFrame(symbols_data)



sector_detail_data = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols_detail_data]", powerbi_database)
sector_detail_data.drop_duplicates(subset="symbol", keep="first", inplace=True, ignore_index=True)

print("----------------------------------------------------------------------", "\n",
      "loading historical data of stocks", "\n",
      "----------------------------------------------------------------------")
market_history = pd.DataFrame()
for i in range(len(sector_detail_data)):
    if (sector_detail_data["sector"].iloc[i] != "59") &\
            (sector_detail_data["sector"].iloc[i] != "69") &\
            (sector_detail_data["symbol"].iloc[i][-5:] != "پذيره") &\
            (sector_detail_data["symbol"].iloc[i][:5] != "پذيره"):
        symbol = sector_detail_data["symbol"].iloc[i]
        while True:
            try:
                s = pd.read_sql(f"SELECT [date],[symbol],[final_price],[yesterday_price] FROM [nooredenadb].[tsetmc].[stock_historical_data] where symbol=('{symbol}')", powerbi_database)
                if s is None:
                    break
                if len(s) == 0:
                    break
                else:
                    s.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
                    if symbol in symbols_data["symbol"].values:
                        new_data = pd.DataFrame(data={
                            "date": [today_],
                            "symbol": symbol,
                            "final_price": [symbols_data["final_price"].iloc[
                                                symbols_data.index[symbols_data["symbol"] == symbol].values[0]]],
                            "yesterday_price": [symbols_data["yesterday_price"].iloc[
                                                    symbols_data.index[symbols_data["symbol"] == symbol].values[0]]]
                        })
                        s = pd.concat([s, new_data], axis=0, ignore_index=True)
                    s.drop_duplicates(subset="date", keep="first", inplace=True, ignore_index=True)
                    s["coef"] = (s["yesterday_price"].shift(-1) / s["final_price"]).fillna(1.0)
                    s["adj_coef"] = s.iloc[::-1]["coef"].cumprod().iloc[::-1]
                    s["final_price_adj"] = round(s["final_price"] * s["adj_coef"])

                    tmp = pd.DataFrame()
                    tmp["symbol"] = [symbol]
                    tmp["symbol_id"] = [sector_detail_data["symbol_id"].iloc[i]]
                    tmp["total_share"] = [sector_detail_data["total_share"].iloc[i]]
                    tmp["sector"] = [sector_detail_data["sector"].iloc[i]]
                    tmp["price_last"] = [adj_final_price_date(s, today_)]
                    tmp["price_week"] = [adj_final_price_date(s, one_week)]
                    tmp["price_one_month"] = [adj_final_price_date(s, one_month)]
                    tmp["price_two_month"] = [adj_final_price_date(s, two_month)]
                    tmp["price_three_month"] = [adj_final_price_date(s, three_month)]
                    tmp["price_six_month"] = [adj_final_price_date(s, six_month)]
                    tmp["price_nine_month"] = [adj_final_price_date(s, nine_month)]
                    tmp["price_year"] = [adj_final_price_date(s, one_year)]
                    tmp["price_fiscal_year"] = [adj_final_price_date(s, fiscal_year)]
                    market_history = pd.concat([market_history, tmp], axis=0, ignore_index=True)
                    break
            except Exception as e:
                print(e)
        print(round((i/len(sector_detail_data))*100, ndigits=1), "% has been loaded")

drp_list = []
for i in range(len(market_history)):
    if market_history["symbol"].iloc[i] in ["ودانا", "قاروم", "چنوپا", "رفاه", "كورز", "خليبل",
                                            "دتهران\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\u200c", "حذف -آرين"]:
        drp_list.append(i)
market_history.drop(index=drp_list, inplace=True)
market_history.reset_index(drop=True, inplace=True)

for i, j in [["return_week", "price_week"], ["return_one_month", "price_one_month"],
             ["return_two_month", "price_two_month"], ["return_three_month", "price_three_month"],
             ["return_six_month", "price_six_month"], ["return_nine_month", "price_nine_month"],
             ["return_year", "price_year"], ["return_fiscal_year", "price_fiscal_year"]]:
    market_history[i] = (market_history["price_last"] - market_history[j]) / market_history[j]

market_history["total_value"] = market_history["total_share"] * market_history["price_last"]
market_history.drop_duplicates(subset=["symbol"], inplace=True, ignore_index=True)

market_history["total_value_category"] = [
    "25" if market_history["total_value"].iloc[i] >= 25 * 1e13 else
    "10-25" if market_history["total_value"].iloc[i] >= 10 * 1e13 else
    "4-10" if market_history["total_value"].iloc[i] >= 4 * 1e13 else
    "1-4" if market_history["total_value"].iloc[i] >= 1 * 1e13 else "1"
    for i in range(len(market_history))
]

########################################################################################################################

for i in ["اهرم", "توان", "پتروآگاه", "دارا يكم", "پالايش"]:
    if i in market_history["symbol"].values:
        market_history["total_value_category"].iloc[market_history.index[market_history["symbol"] == i].values[0]] = "25"

########################################################################################################################

categories = [
    ["25", "market_return(25)", "بازدهی سهم های بازار برای شرکت های بالای 25 هزار میلیارد تومان"],
    ["10-25", "market_return(10-25)", "بازدهی سهم های بازار برای شرکت های بین 10 تا 25 هزار میلیارد تومان"],
    ["4-10", "market_return(4-10)", "بازدهی سهم های بازار برای شرکت های بین 4 تا 10 هزار میلیارد تومان"],
    ["1-4", "market_return(1-4)", "بازدهی سهم های بازار برای شرکت های بین 1 تا 4 هزار میلیارد تومان"],
    ["1", "market_return(1)", "بازدهی سهم های بازار برای شرکت های زیر 1 هزار میلیارد تومان"]
 ]

for n, m, l in categories:
    market_history_ = market_history[market_history["total_value_category"] == n]
    market_history_ = market_history_[["symbol", "price_last", "return_week", "return_one_month",
                                     "return_two_month", "return_three_month", "return_six_month",
                                     "return_nine_month", "return_year", "return_fiscal_year"]]
    indices_table = indices_table[["symbol", "price_last", "return_week", "return_one_month",
                                     "return_two_month", "return_three_month", "return_six_month",
                                     "return_nine_month", "return_year", "return_fiscal_year"]]
    market_history_ = pd.concat([market_history_, indices_table], axis=0, ignore_index=True)
    fig_1w = go.Bar(
        y=market_history_.sort_values("return_week", ascending=True)["symbol"],
        x=market_history_.sort_values("return_week", ascending=True)["return_week"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_week", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_week", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_week", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_week", ascending=True)["return_week"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_week", ascending=True)["return_week"].iloc[i] < 0 else col_neu for i in range(len(market_history_.sort_values("return_week", ascending=True)["return_week"]))]),
        orientation="h",
        showlegend=False,
        text=market_history_.sort_values("return_week", ascending=True)["return_week"],
        texttemplate="%{text:(.0%}",
        textfont=dict(family=fnt, size=25),
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_1m = go.Bar(
        y=market_history_.sort_values("return_one_month", ascending=True)["symbol"],
        x=market_history_.sort_values("return_one_month", ascending=True)["return_one_month"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_one_month", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_one_month", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_one_month", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_one_month", ascending=True)["return_one_month"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_one_month", ascending=True)["return_one_month"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_one_month", ascending=True)["return_one_month"]))]),
        orientation="h",
        showlegend=False,
        text=market_history_.sort_values("return_one_month", ascending=True)["return_one_month"],
        texttemplate="%{text:(.0%}",
        textfont=dict(family=fnt, size=25),
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_2m = go.Bar(
        y=market_history_.sort_values("return_two_month", ascending=True)["symbol"],
        x=market_history_.sort_values("return_two_month", ascending=True)["return_two_month"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_two_month", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_two_month", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_two_month", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_two_month", ascending=True)["return_two_month"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_two_month", ascending=True)["return_two_month"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_two_month", ascending=True)["return_two_month"]))]),
        orientation="h",
        showlegend=False,
        textfont=dict(family=fnt, size=25),
        text=market_history_.sort_values("return_two_month", ascending=True)["return_two_month"],
        texttemplate="%{text:(.0%}",
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_3m = go.Bar(
        y=market_history_.sort_values("return_three_month", ascending=True)["symbol"],
        x=market_history_.sort_values("return_three_month", ascending=True)["return_three_month"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_three_month", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_three_month", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_three_month", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_three_month", ascending=True)["return_three_month"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_three_month", ascending=True)["return_three_month"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_three_month", ascending=True)["return_three_month"]))]),
        orientation="h",
        showlegend=False,
        text=market_history_.sort_values("return_three_month", ascending=True)["return_three_month"],
        textfont=dict(family=fnt, size=25),
        texttemplate="%{text:(.0%}",
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_6m = go.Bar(
        y=market_history_.sort_values("return_six_month", ascending=True)["symbol"],
        x=market_history_.sort_values("return_six_month", ascending=True)["return_six_month"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_six_month", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_six_month", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_six_month", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_six_month", ascending=True)["return_six_month"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_six_month", ascending=True)["return_six_month"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_six_month", ascending=True)["return_six_month"]))]),
        orientation="h",
        showlegend=False,
        text=market_history_.sort_values("return_six_month", ascending=True)["return_six_month"],
        textfont=dict(family=fnt, size=25),
        texttemplate="%{text:(.0%}",
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_9m = go.Bar(
        y=market_history_.sort_values("return_nine_month", ascending=True)["symbol"],
        x=market_history_.sort_values("return_nine_month", ascending=True)["return_nine_month"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_nine_month", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_nine_month", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_nine_month", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_nine_month", ascending=True)["return_nine_month"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_nine_month", ascending=True)["return_nine_month"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_nine_month", ascending=True)["return_nine_month"]))]),
        orientation="h",
        showlegend=False,
        text=market_history_.sort_values("return_nine_month", ascending=True)["return_nine_month"],
        textfont=dict(family=fnt, size=25),
        texttemplate="%{text:(.0%}",
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_1y = go.Bar(
        y=market_history_.sort_values("return_year", ascending=True)["symbol"],
        x=market_history_.sort_values("return_year", ascending=True)["return_year"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_year", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_year", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_year", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_year", ascending=True)["return_year"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_year", ascending=True)["return_year"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_year", ascending=True)["return_year"]))]),
        orientation="h",
        showlegend=False,
        textfont=dict(family=fnt, size=25),
        text=[str(market_history_.sort_values("return_year", ascending=True)["return_year"].values[i]) for i in range(len(market_history_))],
        texttemplate="%{text:(.0%}",
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fig_fiscal_year = go.Bar(
        y=market_history_.sort_values("return_fiscal_year", ascending=True)["symbol"],
        x=market_history_.sort_values("return_fiscal_year", ascending=True)["return_fiscal_year"].abs(),
        marker=dict(color=[
            "#6e02fa" if market_history_.sort_values("return_fiscal_year", ascending=True)["symbol"].iloc[i] == "شاخص كل" else
            "#687507" if market_history_.sort_values("return_fiscal_year", ascending=True)["symbol"].iloc[i] == "شاخص هم وزن" else
            # "#04ff00" if market_history_.sort_values("return_fiscal_year", ascending=True)["symbol"].iloc[i] in names["نماد"].values else
            color1 if market_history_.sort_values("return_fiscal_year", ascending=True)["return_fiscal_year"].iloc[i] > 0 else
            color2 if market_history_.sort_values("return_fiscal_year", ascending=True)["return_fiscal_year"].iloc[i] < 0 else
            col_neu for i in range(len(market_history_.sort_values("return_fiscal_year", ascending=True)["return_fiscal_year"]))]),
        orientation="h",
        showlegend=False,
        text=market_history_.sort_values("return_fiscal_year", ascending=True)["return_fiscal_year"],
        textfont=dict(family=fnt, size=25),
        texttemplate="%{text:(.0%}",
        textposition="outside",
        textangle=0,
        hoverinfo="skip"
    )

    fg = make_subplots(
        rows=1,
        cols=1,
        print_grid=False,
        specs=[[{"type": "bar"}]]
    )

    fg.add_trace(fig_1w, row=1, col=1)
    fg.add_trace(fig_1m, row=1, col=1)
    fg.add_trace(fig_2m, row=1, col=1)
    fg.add_trace(fig_3m, row=1, col=1)
    fg.add_trace(fig_6m, row=1, col=1)
    fg.add_trace(fig_9m, row=1, col=1)
    fg.add_trace(fig_1y, row=1, col=1)
    fg.add_trace(fig_fiscal_year, row=1, col=1)


    labels = ["هفتگی", "یک ماهه", "دو ماهه", "سه ماهه", "شش ماهه", "نه ماهه", "سالانه", "از ابتدای سال مالی"]
    titles = [
        f"بازدهی هفتگی - از تاریخ {one_week.replace('-', '/')}",
        f"بازدهی ماهانه - از تاریخ {one_month.replace('-', '/')}",
        f"بازدهی 2 ماهه - از تاریخ {two_month.replace('-', '/')}",
        f"بازدهی 3 ماهه - از تاریخ {three_month.replace('-', '/')}",
        f"بازدهی 6 ماهه - از تاریخ {six_month.replace('-', '/')}",
        f"بازدهی 9 ماهه - از تاریخ {nine_month.replace('-', '/')}",
        f"بازدهی سالانه - از تاریخ {one_year.replace('-', '/')}",
        f"بازدهی از ابتدای سال مالی - از تاریخ {fiscal_year.replace('-', '/')}"
    ]
    buttons = []
    for b, label in enumerate(labels):
        visibility = [b == j for j in range(len(labels))]
        button = dict(label="<b>" + label + "</b>",
                      method="update",
                      args=[{"visible": visibility},
                            {"title": "<b>" + l + f" - {today__}" + "</b>" + "<br>" + titles[b]}])
        buttons.append(button)
    updatemenus = list([
        dict(active=0,
             x=-0.05,
             y=1.005,
             buttons=buttons)
    ])

    fg["layout"]["updatemenus"] = updatemenus
    fg.update_annotations(font=dict(size=20, family=fnt))
    fg.update_xaxes(showticklabels=False, showgrid=False)
    fg.update_yaxes(tickfont=dict(family=fnt, size=20), showgrid=False, ticksuffix=" ")
    fg.update_layout(
        title=dict(
            text="<b>" + l + f" - {today__}" + "</b>" + "<br>" + f"بازدهی هفتگی - از تاریخ {one_week.replace('-', '/')}",
            font=dict(size=25, family=fnt),
            xanchor="center"
        ),
        template="seaborn",
        font_family=fnt,
        margin=dict(l=100, r=100, t=100, b=80),
        height=40*len(market_history_),
        showlegend=False
    )
    Ld = len(fg.data)
    for k in range(1, Ld):
        fg.update_traces(visible=False, selector=k)
    fg.write_html(f"//192.168.1.7/data/{m}.html", config={"displayModeBar": False})