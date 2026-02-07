import os, warnings
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from board_meeting_report import weekly_report_data


warnings.filterwarnings("ignore")

avalhami = True

report_date = "1404-09-26"

buy_sell_start_date = "1404/09/08"
buy_sell_end_date = "1404/09/26"

bazdehi_start_date = "1403/08/30"
bazdehi_end_date = "1404/09/26"

fiscal_bazdehi_start_date = "1403/10/30"
fiscal_bazdehi_end_date = "1404/09/26"

folder_address = f"{'X' if os.path.exists('X:/H.Damavandi') else 'Z'}:/H.Damavandi/گزارش هیئت مدیره"


color1, color2, color3, color4, col_neu = "#7dc4cd", "#f8ab1d", "#329650", "#7d0000", "#b7b7b7"
fnt, color_white, color_sell, color_buy = "B Nazanin", "#ffffff", "#fad48e", "#a7effa"

########################################################################################################################

portfolio = weekly_report_data.get_portfolio(include_avalhami=avalhami, main_portfolio=True, prx_portfolio=False)
portfolio_total = pd.DataFrame([{
    "symbol": "<b>مجموع<b>", "amount": "-", "cost_per_share": "-", "total_cost": portfolio["total_cost"].sum(),
    "basket": "",  "final_price": "-", "ownership": "-", "value": portfolio["value"].sum(), "share_of_portfo": 1,
    "profitloss": portfolio["profitloss"].sum(),
    "profitloss_percent": portfolio["profitloss"].sum() / portfolio["total_cost"].sum()
}])
portfolio = pd.concat([portfolio, portfolio_total], axis=0, ignore_index=True)
portfolio_ = portfolio.copy()

########################################################################################################################

p = 50
portfolio = portfolio_.iloc[:p, :].reset_index(inplace=False)

portfolio["even_row"] = pd.Series(portfolio.index.to_list()) % 2 == 0
portfolio[["color_white", "color_sell"]] = color_white, color_sell
table_color = [((portfolio["even_row"] * portfolio["color_white"]) +
                (~portfolio["even_row"] * portfolio["color_sell"])).tolist()]
table_color = table_color * len(portfolio.columns)

figure_1 = go.Table(header=dict(values=["<b>درصد سود (زیان)<b>", "<b>سود (زیان) - میلیارد ریال<b>",
                                        "<b>سهم از پرتفوی<b>", "<b>ارزش روز - میلیارد ریال<b>",
                                        "<b>قیمت پایانی - ریال<b>", "<b>بهای تمام شده - میلیارد ریال<b>",
                                        "<b>بهای هر سهم - ریال<b>", "<b>تعداد<b>", "<b>سبد<b>", "<b>درصد مالکیت<b>",
                                        "<b>نماد<b>", "<b>ردیف<b>"],
                                font=dict(size=20, family=fnt),
                                align="center",
                                height=45),
                    cells=dict(
                        values=[
                            portfolio["profitloss_percent"].values.tolist(),
                            round(portfolio["profitloss"] / 1e9).values.tolist(),
                            portfolio["share_of_portfo"].values.tolist(),
                            round(portfolio["value"] / 1e9).values.tolist(),
                            portfolio["final_price"].values.tolist(),
                            round(portfolio["total_cost"] / 1e9).values.tolist(),
                            round(portfolio["cost_per_share"]).values.tolist(),
                            portfolio["amount"].values.tolist(),
                            portfolio["basket"].values.tolist(),
                            portfolio["ownership"].values.tolist(),
                            portfolio["symbol"].values.tolist(),
                            list(range(1, len(portfolio) + 1))
                        ],
                        font=dict(size=28, family=fnt),
                        align="center",
                        format=[
                            ["(,.1%"], ["(,"], [".1%"], [","], [","], [","], [",.0f"], [","], [""], [".1%"], [""], [""]
                        ],
                        height=44,
                        fill=dict(color=table_color),
                        line=dict(width=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                    ),
                    columnwidth=[0.8, 0.75, 0.75, 1, 1, 1, 1, 1.25, 0.75, 0.75, 1, 0.25])

fig = make_subplots(rows=1, cols=1, print_grid=False, specs=[[{"type": "table"}]],
                    subplot_titles=["<b>" + f"آخرین وضعیت پرتفوی بورسی - {report_date.replace('-', '/')}" + "<b>"])

fig.add_trace(figure_1, row=1, col=1)

fig.update_annotations(font=dict(size=28, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=2500,
                  margin=dict(autoexpand=False, l=100, r=100, t=50, b=80))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/main/report ({report_date})_1.html",
               config={"displayModeBar": False})

########################################################################################################################

portfolio = portfolio_.iloc[p:, :].reset_index(inplace=False)

portfolio["even_row"] = pd.Series(portfolio.index.to_list()) % 2 == 0
portfolio[["color_white", "color_sell"]] = color_white, color_sell
table_color = [((portfolio["even_row"] * portfolio["color_white"]) +
                (~portfolio["even_row"] * portfolio["color_sell"])).tolist()]
table_color[0][-1] = col_neu
table_color = table_color * len(portfolio.columns)

figure_2_1 = go.Table(header=dict(values=["<b>درصد سود (زیان)<b>", "<b>سود (زیان) - میلیارد ریال<b>",
                                         "<b>سهم از پرتفوی<b>", "<b>ارزش روز - میلیارد ریال<b>",
                                         "<b>قیمت پایانی - ریال<b>", "<b>بهای تمام شده - میلیارد ریال<b>",
                                         "<b>بهای هر سهم - ریال<b>", "<b>تعداد<b>", "<b>سبد<b>", "<b>درصد مالکیت<b>",
                                         "<b>نماد<b>", "<b>ردیف<b>"],
                                font=dict(size=20, family=fnt),
                                align="center",
                                height=45),
                    cells=dict(
                         values=[
                             portfolio["profitloss_percent"].values.tolist(),
                             round(portfolio["profitloss"] / 1e9).values.tolist(),
                             portfolio["share_of_portfo"].values.tolist(),
                             round(portfolio["value"] / 1e9).values.tolist(),
                             portfolio["final_price"].values.tolist(),
                             round(portfolio["total_cost"] / 1e9).values.tolist(),
                             round(portfolio["cost_per_share"]).values.tolist(),
                             portfolio["amount"].values.tolist(),
                             portfolio["basket"].values.tolist(),
                             portfolio["ownership"].values.tolist(),
                             portfolio["symbol"].values.tolist(),
                             list(range(1 + p, 1 + p + len(portfolio) - 1)) + [""] * 1
                         ],
                         font=dict(size=28, family=fnt),
                         align="center",
                         format=[["(,.1%"], ["(,"], [".1%"], [","], [","] * (len(portfolio) - 1) + [""], [","],
                                 [",.0f"] * (len(portfolio) - 1) + [""], [","] * (len(portfolio) - 1) + [""], [""],
                                 [".2%"] * (len(portfolio) - 1) + [""], [""], [""]],
                         height=43,
                         fill=dict(color=table_color),
                         line=dict(width=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                     ),
                    columnwidth=[0.8, 0.75, 0.75, 1, 1, 1, 1, 1.25, 0.75, 0.75, 1, 0.25])

########################################################################################################################


top_10_value = portfolio_[["symbol", "total_cost", "value", "profitloss"]].iloc[:10].copy()
top_10_value[["color3", "color4"]] = color3, color4

figure_2_2 = go.Bar(name="ارزش روز", x="<b>" + top_10_value["symbol"] + "<b>", y=top_10_value["value"],
                    customdata=round(top_10_value["value"] / 1e9).values.tolist(), hoverinfo="skip",
                    marker=dict(color="#64c4cd"), textfont=dict(family=fnt, size=16), legendgroup=1, showlegend=True,
                    texttemplate="<b>%{customdata:,}<b>", textangle=0, textposition="outside")

figure_2_3 = go.Bar(name="بهای تمام شده کل", x="<b>" + top_10_value["symbol"] + "<b>", y=top_10_value["total_cost"],
                    customdata=round(top_10_value["total_cost"] / 1e9).values.tolist(), textfont=dict(family=fnt, size=16),
                    texttemplate="<b>%{customdata:,}<b>", textangle=0, textposition="outside",
                    hoverinfo="skip", marker=dict(color=color2), showlegend=True, legendgroup=1)

figure_2_4 = go.Bar(name="مازاد(کاهش) ارزش", x="<b>" + top_10_value["symbol"] + "<b>", y=top_10_value["profitloss"],
                    customdata=round(top_10_value["profitloss"] / 1e9).values.tolist(), textfont=dict(family=fnt, size=16),
                    legendgroup=1, showlegend=True, texttemplate="<b>%{customdata:,}<b>", textangle=0,
                    textposition="outside", hoverinfo="skip", marker=dict(
        color=(((top_10_value["profitloss"] > 0) * top_10_value["color3"]) +
               ((top_10_value["profitloss"] <= 0) * top_10_value["color4"])).tolist()))

########################################################################################################################

fig = make_subplots(
    rows=2, cols=1, print_grid=False, vertical_spacing=0.005,
    specs=[[{"type": "table"}], [{"type": "bar", "t": -0.05, "b": 0.1}]], row_heights=[2, 1],
    subplot_titles=["<b>" + "آخرین وضعیت پرتفوی بورسی - " + f"{report_date.replace('-', '/')}" + "<b>",
                    "<b>" + "عملکرد 10 سهم بزرگ پرتفوی - " + f"{report_date.replace('-', '/')}" + "<b>"])

fig.add_trace(figure_2_1, row=1, col=1)
fig.add_trace(figure_2_2, row=2, col=1)
fig.add_trace(figure_2_3, row=2, col=1)
fig.add_trace(figure_2_4, row=2, col=1)

fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_xaxes(tickfont=dict(family=fnt, size=20), showgrid=False)
fig.update_yaxes(showticklabels=False, showgrid=False)
fig.update_layout(
    template="seaborn", font_family=fnt, height=2600,
    margin=dict(autoexpand=False, l=100, r=100, t=50, b=80),
    legend=dict(x=0.9, y=0.35, font=dict(size=16, weight="bold")))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/main/report ({report_date})_2.html",
               config={"displayModeBar": False})


########################################################################################################################

buy = weekly_report_data.get_trades_buy(start_date=buy_sell_start_date, end_date=buy_sell_end_date,
                                             fix_exclude=True, main_portfolio=True, prx_portfolio=False)["buy"]

buy_table = buy[["symbol", "volume", "value", "basket"]].rename(
    columns={"volume": "amount", "value": "cost"}, inplace=False)

buy_table.sort_values(by="cost", ascending=False, ignore_index=True, inplace=True)
buy_table["mean_price"] = round(buy_table["cost"] / buy_table["amount"]).astype(int)
buy_table["percent"] = buy_table["cost"] / buy_table["cost"].sum()

buy_total = pd.DataFrame([{"symbol": "<b>مجموع<b>", "basket": "", "amount": "-", "mean_price": "-",
                            "cost": buy_table["cost"].sum(), "percent": 1}])
buy_table = pd.concat([buy_table, buy_total], axis=0, ignore_index=True)

buy_table["even_row"] = pd.Series(buy_table.index.to_list()) % 2 == 0
buy_table[["color_white", "color_buy"]] = color_white, color_buy
buy_table_color = [((buy_table["even_row"] * buy_table["color_white"]) +
                    (~buy_table["even_row"] * buy_table["color_buy"])).tolist()]
buy_table_color[0][-1] = col_neu
buy_table_color = buy_table_color * len(buy_table.columns)

buy_table_pie = buy_table.iloc[:-1, :]
buy_table_pie_small = buy_table_pie[buy_table_pie["percent"] < 0.01]
if len(buy_table_pie_small) > 0:
    buy_table_pie_small_total = pd.DataFrame([
        {"name": "سایر", "symbol": "سایر", "amount": buy_table_pie_small["amount"].sum(), "mean_price": "-",
         "cost": buy_table_pie_small["cost"].sum(), "percent": buy_table_pie_small["percent"].sum()}])
    buy_table_pie = pd.concat(
        [buy_table_pie[buy_table_pie["percent"] >= 0.01], buy_table_pie_small_total], axis=0, ignore_index=True)

########################################################################################################################

sell = weekly_report_data.get_trades_sell(start_date=buy_sell_start_date, end_date=buy_sell_end_date,
                                               fix_exclude=True, main_portfolio=True, prx_portfolio=False)["sell"]

sell_table = sell[["symbol", "volume", "value", "total_cost", "basket"]].rename(
    columns={"volume": "amount", "total_cost": "cost"}, inplace=False)
sell_table = sell_table[~sell_table["symbol"].str[:1].isin(["ض", "ط"])].reset_index(drop=True)

sell_table.sort_values(by="value", ascending=False, ignore_index=True, inplace=True)
sell_table["mean_price"] = round(sell_table["value"] / sell_table["amount"]).astype(int)
sell_table["profitloss"] = sell_table["value"] - sell_table["cost"]
sell_table["profitloss_percent"] = sell_table["profitloss"] / sell_table["cost"]
sell_table["percent"] = sell_table["value"] / sell_table["value"].sum()
sell_total = pd.DataFrame([{
    "symbol": "<b>مجموع<b>", "basket": "", "amount": "-", "mean_price": "-", "value": sell_table["value"].sum(),
    "cost": sell_table["cost"].sum(), "profitloss": sell_table["profitloss"].sum(),
    "profitloss_percent": sell_table["profitloss"].sum() / sell_table["cost"].sum(), "percent": 1}])

sell_table = pd.concat([sell_table, sell_total], axis=0, ignore_index=True)

sell_table["even_row"] = pd.Series(sell_table.index.to_list()) % 2 == 0
sell_table[["color_white", "color_sell"]] = color_white, color_sell

sell_table_color = [((sell_table["even_row"] * sell_table["color_white"]) +
                    (~sell_table["even_row"] * sell_table["color_sell"])).tolist()]
sell_table_color[0][-1] = col_neu
sell_table_color = sell_table_color * len(sell_table.columns)

sell_table_pie = sell_table.iloc[:-1, :]
sell_table_pie_small = sell_table_pie[sell_table_pie["percent"] < 0.01]
if len(sell_table_pie_small) > 0:
    sell_table_pie_small_total = pd.DataFrame([
        {"name": "سایر", "symbol": "سایر", "amount": sell_table_pie_small["amount"].sum(), "mean_price": "-",
         "value": sell_table_pie_small["value"].sum(), "percent": sell_table_pie_small["percent"].sum()}])
    sell_table_pie = pd.concat(
        [sell_table_pie[sell_table_pie["percent"] >= 0.01], sell_table_pie_small_total], axis=0, ignore_index=True)

#######################################################################################################################
#######################################################################################################################

figure_3_1 = go.Table(
    header=dict(
        values=[
            "<b>مبلغ کل خالص - میلیارد ریال<b>", "<b>قیمت ناخالص - ریال<b>", "<b>تعداد<b>", "<b>دارایی<b>", "<b>سبد<b>", "<b>ردیف<b>"
        ],
        font=dict(size=20, family=fnt), align="center", height=40
    ),
    cells=dict(
        values=[
            round(buy_table["cost"] / 1e9).values.tolist(),
            buy_table["mean_price"].values.tolist(),
            buy_table["amount"].values.tolist(),
            buy_table["symbol"].values.tolist(),
            buy_table["basket"].values.tolist(),
            list(range(1 , 1 + len(buy_table) - 1)) + [""] * 1,
        ],
        font=dict(size=25, family=fnt),
        align="center",
        format=[[","], [","] * (len(buy_table) - 1) + [""], [","] * (len(buy_table) - 1) + [""], [""], [""], [""]],
        height=40,
        fill=dict(color=buy_table_color),
        line=dict(width=[0, 0, 0, 0, 0])
    ),
    columnwidth=[1, 1, 1, 1, 1, 0.5]
)

figure_3_2 = go.Pie(
    labels="<b>" + buy_table_pie["symbol"].values + "<b>", values=buy_table_pie["cost"].values,
    hoverinfo="skip", textinfo="label+percent", insidetextorientation="horizontal",
    insidetextfont=dict(size=18, family=fnt), outsidetextfont=dict(size=18, family=fnt),
    sort=False, showlegend=False, legendgroup=1, rotation=0)

fig = make_subplots(rows=2, cols=1, print_grid=False, row_heights=[0.05 * (len(buy_table) + 1), 0.5],
                    specs=[[{"type": "table"}], [{"type": "pie", "t": -0.25, "b": 0.15}]],
                    subplot_titles=["<b>" + f"خرید - {buy_sell_start_date} لغایت {buy_sell_end_date}" + "<b>", ""],
                    vertical_spacing=0.15)

fig.add_trace(figure_3_1, row=1, col=1)
fig.add_trace(figure_3_2, row=2, col=1)

fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=2300,
                  margin=dict(autoexpand=False, l=150, r=150, t=50, b=50))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/main/report ({report_date})_3.html",
               config={"displayModeBar": False})

########################################################################################################################

figure_4_1 = go.Table(
    header=dict(
        values=[
            "<b>درصد سود و زیان", "<b>سود و زیان - میلیارد ریال", "<b>بهای تمام شده ی کل - میلیارد ریال",
            "<b>مبلغ کل خالص - میلیارد ریال<b>", "<b>میانگین قیمت - ریال<b>", "<b>تعداد<b>", "<b>دارایی<b>",
            "<b>سبد<b>", "<b>ردیف<b>"
        ], font=dict(size=20, family=fnt), align="center", height=40),
cells=dict(
    values=[
        sell_table["profitloss_percent"].values.tolist(),
        round(sell_table["profitloss"] / 1e9).values.tolist(),
        round(sell_table["cost"] / 1e9).values.tolist(),
        round(sell_table["value"] / 1e9).values.tolist(),
        sell_table["mean_price"].values.tolist(),
        sell_table["amount"].values.tolist(),
        sell_table["symbol"].values.tolist(),
        sell_table["basket"].values.tolist(),
        list(range(1, 1 + len(sell_table) - 1)) + [""] * 1
    ],
    font=dict(size=25, family=fnt),
    align="center",
    format=[["(.1%"], ["(,"],
            [","], [","],
            [","] * (len(sell_table) - 1) + [""], [","] * (len(sell_table) - 1) + [""],
            [""], [""], [""]],
    height=38,
    fill=dict(color=sell_table_color),
    line=dict(width=[0, 0, 0, 0, 0, 0])
    ),
    columnwidth=[1, 1, 1, 1, 1, 0.8, 0.8, 0.8, 0.3]
)

figure_4_2 = go.Pie(
    labels="<b>" + sell_table_pie["symbol"].values + "<b>",
    values=sell_table_pie["value"].values,
    textinfo="label+percent", insidetextorientation="horizontal", insidetextfont=dict(size=18, family=fnt),
    hoverinfo="skip", outsidetextfont=dict(size=18, family=fnt), sort=False, showlegend=False, legendgroup=3,
    rotation=0)

fig = make_subplots(rows=2, cols=1, print_grid=False, row_heights=[0.05 * len(sell_table), 0.5],
                    specs=[[{"type": "table"}], [{"type": "pie", "t": -0.2, "b": 0.15}]],
                    subplot_titles=["<b>" + f"فروش - {buy_sell_start_date} لغایت {buy_sell_end_date}" + "<b>", ""],
                    vertical_spacing=0.15)

fig.add_trace(figure_4_1, row=1, col=1)
fig.add_trace(figure_4_2, row=2, col=1)

fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=2200,
                  margin=dict(autoexpand=False, l=150, r=150, t=50, b=50))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/main/report ({report_date})_4.html",
               config={"displayModeBar": False})

########################################################################################################################
########################################################################################################################

bazdehi = pd.read_excel(f"{folder_address}/data/bazdehi_main.xlsx")

bazdehi["ارقام به میلیارد ریال"].replace({"بازدهی پرتفوی بورسی": "بازدهی ماهانه", "بازدهی شاخص کل": "بازدهی ماهانه",
                                          "بازدهی شاخص قیمت هم وزن": "بازدهی ماهانه",
                                          "بازدهی شاخص صنعت": "بازدهی ماهانه"}, inplace=True)
bazdehi["ارقام به میلیارد ریال"].fillna(value="بازدهی سه ماهه", inplace=True)
bazdehi.fillna(value="-", inplace=True)

bazdehi_columns = bazdehi.columns.values.tolist()
bazdehi_columns.reverse()

bazdehi_cell_color = ([["#ffffff"] * 4 + [color1, color1] + ["#ffffff", color1, color1] * 3]) * len(bazdehi_columns)
bazdehi_font_color = ([["#000000"] * 4 + ["#000000", "#000000"] + ["#000000", "#000000", "#000000"] * 3]
                     ) * len(bazdehi_columns)

figure_5 = go.Table(
    header=dict(values=[f"<b>{bazdehi_columns[i]}<b>" for i in range(len(bazdehi_columns))], fill=dict(color=color2),
                font=dict(size=22, family=fnt), align="center", height=40, line=dict(color="#000000", width=1.5)),
    cells=dict(
        values=[[bazdehi[j].iloc[i] for i in range(len(bazdehi))] for j in bazdehi_columns],
        font=dict(size=26, family=fnt, color=bazdehi_font_color), align="center", height=70,
        fill=dict(color=bazdehi_cell_color), line=dict(color="#000000", width=1.5),
        format=[[""] * 3 + ([""] + ["(.1%"] + [""]) * 4] +
               [[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + ["(.1%"]) * 4] * (len(bazdehi_columns) - 15) +
               ([[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + ["(.1%"]) * 4] +
                [[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + [""]) * 4] * 2) * 4 +
               [[",.0f"]*2 + ([""] + [""] + [""] + [""]) + ([",.0f"] + [""] + [""])*3] +
               [[""]]),
    columnwidth=[0.75] + [1] * (len(bazdehi_columns) - 2) + [1.75]
)

fig = make_subplots(
    rows=1, cols=1, print_grid=False, specs=[[{"type": "table"}]], vertical_spacing=0.05, subplot_titles=[
        "<b>" + f"بازدهی پرتفوی بورسی از  {bazdehi_start_date}  تا تاریخ {bazdehi_end_date}" + "<b>"])
fig.add_trace(figure_5, row=1, col=1)
fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=1300,
                  margin=dict(autoexpand=False, l=100, r=100, t=50, b=50))
fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/main/report ({report_date})_5.html",
               config={"displayModeBar": False})

########################################################################################################################
########################################################################################################################

bazdehi = pd.read_excel(f"{folder_address}/data/bazdehi_main_fiscal.xlsx")

bazdehi["ارقام به میلیارد ریال"].replace({"بازدهی پرتفوی بورسی": "بازدهی ماهانه", "بازدهی شاخص کل": "بازدهی ماهانه",
                                          "بازدهی شاخص قیمت هم وزن": "بازدهی ماهانه",
                                          "بازدهی شاخص صنعت": "بازدهی ماهانه"}, inplace=True)
bazdehi["ارقام به میلیارد ریال"].fillna(value="بازدهی سه ماهه", inplace=True)
bazdehi.fillna(value="-", inplace=True)

bazdehi_columns = bazdehi.columns.values.tolist()
bazdehi_columns.reverse()

bazdehi_cell_color = ([["#ffffff"] * 4 + [color1, color1] + ["#ffffff", color1, color1] * 3]) * len(bazdehi_columns)
bazdehi_font_color = ([["#000000"] * 4 + ["#000000", "#000000"] + ["#000000", "#000000", "#000000"] * 3]
                     ) * len(bazdehi_columns)

figure_6 = go.Table(
    header=dict(values=[f"<b>{bazdehi_columns[i]}<b>" for i in range(len(bazdehi_columns))], fill=dict(color=color2),
                align="center", font=dict(size=22, family=fnt), height=40, line=dict(color="#000000", width=1.5)),
    cells=dict(values=[[bazdehi[j].iloc[i] for i in range(len(bazdehi))] for j in bazdehi_columns],
               font=dict(size=26, family=fnt, color=bazdehi_font_color), align="center", height=70,
               fill=dict(color=bazdehi_cell_color), line=dict(color="#000000", width=1.5),
               format=[[""] * 2 + [",.0f"] + ([",.0f"] + ["(.1%"] + [""]) + ([""] + ["(.1%"] + [""]) * 3] +
                      [[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + ["(.1%"]) * 4] * (((len(bazdehi_columns) - 3) % 3) > 0) +
                      [[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + [""]) * 4] * (((len(bazdehi_columns) - 3) % 3) // 2) +
                      ([[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + ["(.1%"]) * 4] +
                       [[",.0f"] * 3 + ([",.0f"] + ["(.1%"] + [""]) * 4] * 2) * ((len(bazdehi_columns) - 3) // 3) +
                      [[",.0f"]*2 + ([""] + [""] + [""] + [""]) + ([",.0f"] + [""] + [""])*3] + [[""]]),
    columnwidth=[0.75] + [1] * (len(bazdehi_columns) - 2) + [2]
)
fig = make_subplots(
    rows=1, cols=1, print_grid=False, specs=[[{"type": "table"}]], vertical_spacing=0.05, subplot_titles=[
        "<b>" + f"بازدهی پرتفوی بورسی از  {fiscal_bazdehi_start_date}  تا تاریخ {fiscal_bazdehi_end_date}" + "<b>"])
fig.add_trace(figure_6, row=1, col=1)

fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt,
                  margin=dict(autoexpand=False, l=100, r=100, t=50, b=50),
                  height=1300)
fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/main/report ({report_date})_6.html",
               config={"displayModeBar": False})
