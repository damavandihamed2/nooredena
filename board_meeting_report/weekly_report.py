import pandas as pd
import os, warnings, database
import plotly.graph_objects as go
from plotly.subplots import make_subplots


warnings.filterwarnings("ignore")
powerbi_database = database.db_conn
symbols = pd.read_sql("select symbol, total_share from [nooredenadb].[tsetmc].[symbols] where active=1", powerbi_database)

report_date = "1404-06-17"

buy_sell_start_date = "1404/06/08"
buy_sell_end_date = "1404/06/17"

bazdehi_start_date = "1403/05/31"
bazdehi_end_date = "1404/06/17"

fiscal_bazdehi_start_date = "1403/10/30"
fiscal_bazdehi_end_date = "1404/06/17"

proxy = "127.0.0.1:10808"
proxies = {'http': 'socks5://' + proxy,
           'https': 'socks5://' + proxy}

if os.path.exists("X:/OLD/گزارش هیئت مدیره"): folder_address = "X:/OLD/گزارش هیئت مدیره"
else: folder_address = "Z:/OLD/گزارش هیئت مدیره"

color1, color2, color3, color4, col_neu = "#7dc4cd", "#f8ab1d", "#329650", "#7d0000", "#b7b7b7"
fnt, color_white, color_sell, color_buy = "B Nazanin", "#ffffff", "#fad48e", "#a7effa"

sigma_report_raw = pd.read_excel(f"{folder_address}/data/sigma_Report.xlsx")
sigma_report = sigma_report_raw[["دارایی", "تعداد", "بهای تمام شده کل", "نوع", "نماد", "قیمت پایانی"]]
sigma_report.rename({"دارایی": "asset", "تعداد": "amount", "بهای تمام شده کل": "total_cost",
                     "نوع": "type", "نماد": "symbol", "قیمت پایانی": "final_price"}, axis=1, inplace=True)
sigma_report.dropna(subset="asset", inplace=True)
sigma_report["symbol"] = sigma_report["symbol"].replace(
    {"ی": "ي", "ک": "ك"}, inplace=False, regex=True).replace({"دارايكم": "دارا يكم"}, inplace=False)
sigma_report = sigma_report.groupby(by=["asset", "symbol", "type", "final_price"], as_index=False).sum()
sigma_report["cost_per_share"] = sigma_report["total_cost"] / sigma_report["amount"]

sigma_report_prx_raw = pd.read_excel(f"{folder_address}/data/sigma_Report_prx.xlsx")
sigma_report_prx = sigma_report_prx_raw[["دارایی", "تعداد", "بهای تمام شده کل", "نوع", "نماد", "قیمت پایانی"]]
sigma_report_prx.rename({"دارایی": "asset", "تعداد": "amount", "بهای تمام شده کل": "total_cost",
                     "نوع": "type", "نماد": "symbol", "قیمت پایانی": "final_price"}, axis=1, inplace=True)
sigma_report_prx.dropna(subset="asset", inplace=True)
sigma_report_prx["symbol"] = sigma_report_prx["symbol"].replace(
    {"ی": "ي", "ک": "ك"}, inplace=False, regex=True).replace({"دارايكم": "دارا يكم"}, inplace=False)
sigma_report_prx = sigma_report_prx.groupby(by=["asset", "symbol", "type", "final_price"], as_index=False).sum()
sigma_report_prx["cost_per_share"] = sigma_report_prx["total_cost"] / sigma_report_prx["amount"]

# sigma_report_total = pd.concat([sigma_report, sigma_report_prx], axis=0, ignore_index=True)
# sigma_report_total = sigma_report_total.groupby(by=["asset", "symbol", "type", "final_price"], as_index=False).sum()
# sigma_report_total["cost_per_share"] = sigma_report_total["total_cost"] / sigma_report_total["amount"]
# sigma_report_total = sigma_report_total.merge(symbols, on="symbol", how="left")
# sigma_report_total["ownership_percent"] = sigma_report_total["amount"] / sigma_report_total["total_share"]
# sigma_report_total = sigma_report_total[["symbol", "amount", "total_share", "ownership_percent"]]

########################################################################################################################

sigma_report["cost_per_share"] = sigma_report["total_cost"] / sigma_report["amount"]
sigma_report["value"] = sigma_report["amount"] * sigma_report["final_price"]
sigma_report.sort_values(["value"], ignore_index=True, inplace=True, ascending=False)

prx = pd.DataFrame([{"symbol": "-", "asset": "<b>prx سبد<b>", "amount": "-", "cost_per_share": "-",
                     "total_cost": sigma_report_prx["total_cost"].sum(), "final_price": "-",
                     "value": (sigma_report_prx["amount"] * sigma_report_prx["final_price"]).sum()}])
sigma_report = pd.concat([sigma_report, prx], axis=0, ignore_index=True)

sigma_report["share_of_portfo"] = sigma_report["value"] / sigma_report["value"].sum()
sigma_report["profit-loss"] = sigma_report["value"] - sigma_report["total_cost"]
sigma_report["profit-loss-percent"] = sigma_report["profit-loss"] / sigma_report["total_cost"]

total = pd.DataFrame([{"symbol": "-", "asset": "<b>مجموع<b>", "amount": "-", "cost_per_share": "-",
                       "total_cost": sigma_report["total_cost"].sum(), "final_price": "-",
                       "value": sigma_report["value"].sum(), "share_of_portfo": 1,
                       "profit-loss": sigma_report["profit-loss"].sum(),
                       "profit-loss-percent": sigma_report["profit-loss"].sum() / sigma_report["total_cost"].sum()}])
sigma_report = pd.concat([sigma_report, total], axis=0, ignore_index=True)
sigma_report_ = sigma_report.copy()

########################################################################################################################

new_names = {"بین المللی توسعه صنایع و معادن غدیر (وکغدیر)": "توسعه صنایع و معادن غدیر (وکغدیر)",
             "زعفران0210نگین وحدت جام(پ) (زعف0210پ20)": "زعفران0210نگین وحدت جام(پ)",
             "زعفران0210نگین سحرخیز(پ) (زعف0210پ11)": "زعفران0210نگین سحرخیز(پ)",
             "اقتصادی نگین گردشگری ایرانیان (گنگین)": "نگین گردشگری ایرانیان (گنگین)",
             "کشاورزی و دامپروری فجر اصفهان (زفجر)": "کشاورزی دامپروری فجر اصفهان (زفجر)",
             "صندوق س.پشتوانه سكه طلا كهربا (كهربا)": "صندوق سكه طلا كهربا (كهربا)",
             "سر. مالی سپهر صادرات (حق تقدم) (وسپهرح)": "سر. سپهر صادرات (حق تقدم)",
             "صبا فولاد خلیج فارس (حق تقدم) (فصباح)": "صبا فولاد خلیج فارس (حق تقدم)",
             "سرمایه گذاری سیمان تامین (حق تقدم) (سیتاح)": "سرمایه گذاری سیمان تامین (حق تقدم)",
             "توسعه معدنی و صنعتی صبانور (حق تقدم) (کنورح)": "ح.توسعه معدنی و صنعتی صبانور (کنورح)",
             "کشت و دام گلدشت نمونه اصفهان (زگلدشت)": "کشت و دام گلدشت اصفهان (زگلدشت)",
             "کشت و دام گلدشت نمونه اصفهان (حق تقدم) (زگلدشتح)": "کشت و دام گلدشت اصفهان (حق تقدم)",
             "سر. توسعه معادن و فلزات (حق تقدم) (ومعادنح)": "سر. توسعه معادن و فلزات (حق تقدم)",
             "زغال سنگ پروده طبس (حق تقدم) (کزغالح)": "زغال سنگ پروده طبس (حق تقدم)"}

sigma_report_["asset"].replace(new_names, inplace=True)

########################################################################################################################

p = 50
sigma_report = sigma_report_.iloc[:p, :].reset_index(inplace=False)

sigma_report["even_row"] = pd.Series(sigma_report.index.to_list()) % 2 == 0
sigma_report[["color_white", "color_sell"]] = color_white, color_sell
table_color = [((sigma_report["even_row"] * sigma_report["color_white"]) +
                (~sigma_report["even_row"] * sigma_report["color_sell"])).tolist()]
table_color = table_color * len(sigma_report.columns)

figure_1 = go.Table(header=dict(values=["<b>درصد سود (زیان)<b>", "<b>سود (زیان) - میلیارد ریال<b>",
                                        "<b>سهم از پرتفوی<b>", "<b>ارزش روز - میلیارد ریال<b>",
                                        "<b>قیمت پایانی - ریال<b>", "<b>بهای تمام شده - میلیارد ریال<b>",
                                        "<b>بهای هر سهم - ریال<b>", "<b>تعداد<b>", "<b>نام سهام<b>",
                                        "<b>ردیف<b>"],
                                font=dict(size=20, family=fnt),
                                align="center",
                                height=50),
                    cells=dict(
                        values=[
                            sigma_report["profit-loss-percent"].values.tolist(),
                            round(sigma_report["profit-loss"] / 1e9).values.tolist(),
                            sigma_report["share_of_portfo"].values.tolist(),
                            round(sigma_report["value"] / 1e9).values.tolist(),
                            sigma_report["final_price"].values.tolist(),
                            round(sigma_report["total_cost"] / 1e9).values.tolist(),
                            round(sigma_report["cost_per_share"]).values.tolist(),
                            sigma_report["amount"].values.tolist(),
                            sigma_report["asset"].values.tolist(),
                            list(range(1, len(sigma_report) + 1))
                        ],
                        font=dict(size=30, family=fnt),
                        align="center",
                        format=[
                            ["(,.1%"], ["(,"], [".1%"], [","], [","], [","], [",.0f"], [","], [""], [""]
                        ],
                        height=45,
                        fill=dict(color=table_color),
                        line=dict(width=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                    ),
                    columnwidth=[0.8, 0.75, 0.75, 1, 1, 1, 1, 1, 2.5, 0.25])

fig = make_subplots(rows=1, cols=1, print_grid=False, specs=[[{"type": "table"}]],
                    subplot_titles=["<b>" + f"آخرین وضعیت پرتفوی بورسی - {report_date.replace('-', '/')}" + "<b>"])

fig.add_trace(figure_1, row=1, col=1)

fig.update_annotations(font=dict(size=28, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=2500,
                  margin=dict(autoexpand=False, l=100, r=100, t=50, b=80))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_1.html",
               config={"displayModeBar": False})

########################################################################################################################

top_10 = sigma_report_[["asset", "symbol", "total_cost", "value", "profit-loss"]].iloc[:10].copy()
top_10[["color3", "color4"]] = color3, color4

figure_2_1 = go.Bar(name="ارزش روز", x="<b>" + top_10["symbol"] + "<b>", y=top_10["value"],
                    customdata=round(top_10["value"] / 1e9).values.tolist(), hoverinfo="skip",
                    marker=dict(color="#64c4cd"), textfont=dict(family=fnt, size=16), legendgroup=1, showlegend=True,
                    texttemplate="<b>%{customdata:,}<b>", textangle=0, textposition="outside")

figure_2_2 = go.Bar(name="بهای تمام شده کل", x="<b>" + top_10["symbol"] + "<b>", y=top_10["total_cost"],
                    customdata=round(top_10["total_cost"] / 1e9).values.tolist(), textfont=dict(family=fnt, size=16),
                    texttemplate="<b>%{customdata:,}<b>", textangle=0, textposition="outside",
                    hoverinfo="skip", marker=dict(color=color2), showlegend=True, legendgroup=1)

figure_2_3 = go.Bar(name="مازاد(کاهش) ارزش", x="<b>" + top_10["symbol"] + "<b>", y=top_10["profit-loss"],
                    customdata=round(top_10["profit-loss"] / 1e9).values.tolist(), textfont=dict(family=fnt, size=16),
                    legendgroup=1, showlegend=True, texttemplate="<b>%{customdata:,}<b>", textangle=0,
                    textposition="outside", hoverinfo="skip", marker=dict(
        color=(((top_10["profit-loss"] > 0) * top_10["color3"]) +
               ((top_10["profit-loss"] <= 0) * top_10["color4"])).tolist()))

########################################################################################################################

sigma_report = sigma_report_.iloc[p:, :].reset_index(inplace=False)

sigma_report["even_row"] = pd.Series(sigma_report.index.to_list()) % 2 == 0
sigma_report[["color_white", "color_sell"]] = color_white, color_sell
table_color = [((sigma_report["even_row"] * sigma_report["color_white"]) +
                (~sigma_report["even_row"] * sigma_report["color_sell"])).tolist()]
table_color[0][-1] = col_neu
table_color = table_color * len(sigma_report.columns)

figure_1_ = go.Table(header=dict(values=["<b>درصد سود (زیان)<b>", "<b>سود (زیان) - میلیارد ریال<b>",
                                        "<b>سهم از پرتفوی<b>", "<b>ارزش روز - میلیارد ریال<b>",
                                        "<b>قیمت پایانی - ریال<b>", "<b>بهای تمام شده - میلیارد ریال<b>",
                                        "<b>بهای هر سهم - ریال<b>", "<b>تعداد<b>", "<b>نام سهام<b>",
                                        "<b>ردیف<b>"],
                                 font=dict(size=20, family=fnt),
                                 align="center",
                                 height=50),
                     cells=dict(
                         values=[
                             sigma_report["profit-loss-percent"].values.tolist(),
                             round(sigma_report["profit-loss"] / 1e9).values.tolist(),
                             sigma_report["share_of_portfo"].values.tolist(),
                             round(sigma_report["value"] / 1e9).values.tolist(),
                             sigma_report["final_price"].values.tolist(),
                             round(sigma_report["total_cost"] / 1e9).values.tolist(),
                             round(sigma_report["cost_per_share"]).values.tolist(),
                             sigma_report["amount"].values.tolist(),
                             sigma_report["asset"].values.tolist(),
                             list(range(1 + p, 1 + p + len(sigma_report) - 1)) + [""] * 1
                         ],
                         font=dict(size=30, family=fnt),
                         align="center",
                         format=[["(,.1%"], ["(,"], [".1%"], [","], [","] * (len(sigma_report) - 2) + [""] * 2, [","],
                                 [",.0f"] * (len(sigma_report) - 2) + [""] * 2,
                                 [","] * (len(sigma_report) - 2) + [""] * 2, [""], [""]],
                         height=45,
                         fill=dict(color=table_color),
                         line=dict(width=[0, 0, 0, 0, 0, 0, 0, 0, 1, 1])
                     ),
                     columnwidth=[0.8, 0.75, 0.75, 1, 1, 1, 1, 1, 2.5, 0.25])

fig = make_subplots(rows=2, cols=1, print_grid=False, row_heights=[1.30, 0.40],
                    specs=[[{"type": "table", "b": -0.005}], [{"type": "bar", "t": -0.015, "b": 0.005}]],
                    subplot_titles=["<b>" + "آخرین وضعیت پرتفوی بورسی - " + f"{report_date.replace('-', '/')}" + "<b>",
                                    "<b>" + "عملکرد 10 سهم بزرگ پرتفوی - " + f"{report_date.replace('-', '/')}" + "<b>"],
                    vertical_spacing=0.0025)

fig.add_trace(figure_1_, row=1, col=1)
fig.add_trace(figure_2_1, row=2, col=1)
fig.add_trace(figure_2_2, row=2, col=1)
fig.add_trace(figure_2_3, row=2, col=1)

fig.update_annotations(font=dict(size=28, family=fnt))
fig.update_xaxes(tickfont=dict(family=fnt, size=20), showgrid=False)
fig.update_yaxes(showticklabels=False, showgrid=False)
fig.update_layout(template="seaborn", font_family=fnt, height=2400,
                  margin=dict(autoexpand=False, l=100, r=100, t=50, b=100),
                  legend=dict(x=0.9, y=0.210, font=dict(size=16, weight="bold")))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_2.html",
               config={"displayModeBar": False})

########################################################################################################################
########################################################################################################################

# sigma_report_prx["share_of_portfo"] = sigma_report_prx["value"] / sigma_report_prx["value"].sum()
# sigma_report_prx["profit-loss"] = sigma_report_prx["value"] - sigma_report_prx["total_cost"]
# sigma_report_prx["profit-loss-percent"] = sigma_report_prx["profit-loss"] / sigma_report_prx["total_cost"]
#
# sigma_report_prx_total = pd.DataFrame([{"symbol": "-", "asset": "<b>مجموع<b>", "amount": "-", "cost_per_share": "-",
#                        "total_cost": sigma_report["total_cost"].sum(), "final_price": "-",
#                        "value": sigma_report["value"].sum(), "share_of_portfo": 1,
#                        "profit-loss": sigma_report["profit-loss"].sum(),
#                        "profit-loss-percent": sigma_report["profit-loss-percent"].sum()}])
# sigma_report = pd.concat([sigma_report, total], axis=0, ignore_index=True)
# sigma_report_ = sigma_report.copy()
#
#
#
#
# sigma_report = sigma_report_.iloc[:p, :].reset_index(inplace=False)
#
# sigma_report["even_row"] = pd.Series(sigma_report.index.to_list()) % 2 == 0
# sigma_report[["color_white", "color_sell"]] = color_white, color_sell
# table_color = [((sigma_report["even_row"] * sigma_report["color_white"]) +
#                 (~sigma_report["even_row"] * sigma_report["color_sell"])).tolist()]
# table_color = table_color * len(sigma_report.columns)
#
# figure_1 = go.Table(header=dict(values=["<b>درصد سود (زیان)<b>", "<b>سود (زیان) - میلیارد ریال<b>",
#                                         "<b>سهم از پرتفوی<b>", "<b>ارزش روز - میلیارد ریال<b>",
#                                         "<b>قیمت پایانی - ریال<b>", "<b>بهای تمام شده - میلیارد ریال<b>",
#                                         "<b>بهای هر سهم - ریال<b>", "<b>تعداد<b>", "<b>نام سهام<b>",
#                                         "<b>ردیف<b>"],
#                                 font=dict(size=20, family=fnt),
#                                 align="center",
#                                 height=50),
#                     cells=dict(
#                         values=[
#                             sigma_report["profit-loss-percent"].values.tolist(),
#                             round(sigma_report["profit-loss"] / 1e9).values.tolist(),
#                             sigma_report["share_of_portfo"].values.tolist(),
#                             round(sigma_report["value"] / 1e9).values.tolist(),
#                             sigma_report["final_price"].values.tolist(),
#                             round(sigma_report["total_cost"] / 1e9).values.tolist(),
#                             round(sigma_report["cost_per_share"]).values.tolist(),
#                             sigma_report["amount"].values.tolist(),
#                             sigma_report["asset"].values.tolist(),
#                             list(range(1, len(sigma_report) + 1))
#                         ],
#                         font=dict(size=30, family=fnt),
#                         align="center",
#                         format=[
#                             ["(,.1%"], ["(,"], [".1%"], [","], [","], [","], [",.0f"], [","], [""], [""]
#                         ],
#                         height=45,
#                         fill=dict(color=table_color),
#                         line=dict(width=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
#                     ),
#                     columnwidth=[0.8, 0.75, 0.75, 1, 1, 1, 1, 1, 2.5, 0.25])
#
# fig = make_subplots(rows=1, cols=1, print_grid=False, specs=[[{"type": "table"}]],
#                     subplot_titles=["<b>" + f"آخرین وضعیت پرتفوی بورسی - {report_date.replace('-', '/')}" + "<b>"])
#
# fig.add_trace(figure_1, row=1, col=1)
#
# fig.update_annotations(font=dict(size=28, family=fnt))
# fig.update_layout(template="seaborn", font_family=fnt, height=2500,
#                   margin=dict(autoexpand=False, l=100, r=100, t=50, b=80))
#
# fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_1.html",
#                config={"displayModeBar": False})

########################################################################################################################
########################################################################################################################

fix_income_symbols = ["افرا نماد پایدار (افران)", "سرمایه گذاری لبخند فارابی (لبخند)", "یاقوت آگاه-ثابت (یاقوت)",
                      "اعتماد آفرین پارسیان (اعتماد)", "سپر سرمایه بیدار (سپر)", "ساحل سرمایه امن خلیج فارس (ساحل)",
                      "نهال ایرانیان (صنهال)",
                      "اعتماد", "صنهال"]


buy_table = pd.read_excel(f"{folder_address}/data/buy.xlsx")
buy_table = buy_table[~buy_table["symbol"].isin(fix_income_symbols)]
buy_table["basket"] = "اصلی"

buy_table_prx = pd.read_excel(f"{folder_address}/data/buy_prx.xlsx")
buy_table_prx = buy_table_prx[~buy_table_prx["symbol"].isin(fix_income_symbols)]
# buy_table_prx["basket"] = "<b>prx سبد<b>"
buy_table_prx["basket"] = "prx"

buy_table = pd.concat([buy_table, buy_table_prx], axis=0, ignore_index=True)
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

sell_table = pd.read_excel(f"{folder_address}/data/sell.xlsx")
sell_table = sell_table[~sell_table["symbol"].isin(fix_income_symbols)]
sell_table["basket"] = "اصلی"

sell_table_prx = pd.read_excel(f"{folder_address}/data/sell_prx.xlsx")
sell_table_prx = sell_table_prx[~sell_table_prx["symbol"].isin(fix_income_symbols)]
# sell_table_prx["basket"] = "<b>prx سبد<b>"
sell_table_prx["basket"] = "prx"

sell_table = pd.concat([sell_table, sell_table_prx], axis=0, ignore_index=True)
sell_table.sort_values(by="value", ascending=False, ignore_index=True, inplace=True)
sell_table["symbol"] = sell_table['symbol'].str.extract(r'\(([^()]*)\)\s*$')
sell_table["mean_price"] = round(sell_table["value"] / sell_table["amount"]).astype(int)
sell_table["profit-loss"] = sell_table["value"] - sell_table["cost"]
sell_table["profit-loss-percent"] = sell_table["profit-loss"] / sell_table["cost"]
sell_table["percent"] = sell_table["value"] / sell_table["value"].sum()
sell_total = pd.DataFrame([{"symbol": "<b>مجموع<b>", "basket": "", "amount": "-", "mean_price": "-",
                            "value": sell_table["value"].sum(), "cost": sell_table["cost"].sum(),
                            "profit-loss": sell_table["profit-loss"].sum(),
                            "profit-loss-percent": sell_table["profit-loss"].sum() / sell_table["cost"].sum(), "percent": 1}])

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
         "cost": sell_table_pie_small["cost"].sum(), "percent": sell_table_pie_small["percent"].sum()}])
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
                    specs=[[{"type": "table"}], [{"type": "pie", "t": -0.15, "b": 0.125}]],
                    subplot_titles=["<b>" + f"خرید - {buy_sell_start_date} لغایت {buy_sell_end_date}" + "<b>", ""],
                    vertical_spacing=0.15)

fig.add_trace(figure_3_1, row=1, col=1)
fig.add_trace(figure_3_2, row=2, col=1)

fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=2300,
                  margin=dict(autoexpand=False, l=150, r=150, t=50, b=50))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_3.html",
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
        sell_table["profit-loss-percent"].values.tolist(),
        round(sell_table["profit-loss"] / 1e9).values.tolist(),
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
    format=[["(.1%"], [","], [","], [","], [","] * (len(sell_table) - 1) + [""], [","] * (len(sell_table) - 1) + [""],
            [""], [""], [""]],
    height=38,
    fill=dict(color=sell_table_color),
    line=dict(width=[0, 0, 0, 0, 0, 0])
    ),
    columnwidth=[1, 1, 1, 1, 1, 0.8, 0.8, 0.8, 0.3]
)

figure_4_2 = go.Pie(
    labels="<b>" + sell_table_pie["symbol"].values + "<b>",
    values=sell_table_pie["cost"].values,
    textinfo="label+percent", insidetextorientation="horizontal", insidetextfont=dict(size=18, family=fnt),
    hoverinfo="skip", outsidetextfont=dict(size=18, family=fnt), sort=False, showlegend=False, legendgroup=3,
    rotation=0)

fig = make_subplots(rows=2, cols=1, print_grid=False, row_heights=[0.05 * len(sell_table), 0.5],
                    specs=[[{"type": "table"}], [{"type": "pie", "t": -0.2, "b": 0.06}]],
                    subplot_titles=["<b>" + f"فروش - {buy_sell_start_date} لغایت {buy_sell_end_date}" + "<b>", ""],
                    vertical_spacing=0.15)

fig.add_trace(figure_4_1, row=1, col=1)
fig.add_trace(figure_4_2, row=2, col=1)

fig.update_annotations(font=dict(size=25, family=fnt))
fig.update_layout(template="seaborn", font_family=fnt, height=2200,
                  margin=dict(autoexpand=False, l=150, r=150, t=50, b=50))

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_4.html",
               config={"displayModeBar": False})

########################################################################################################################
########################################################################################################################

bazdehi = pd.read_excel(f"{folder_address}/data/bazdehi_new.xlsx")

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
fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_5.html",
               config={"displayModeBar": False})

########################################################################################################################
########################################################################################################################

bazdehi = pd.read_excel(f"{folder_address}/data/bazdehi_new_fiscal.xlsx")

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

fig.write_html(f"{folder_address}/({report_date}) گزارش عملکرد/report ({report_date})_6.html",
               config={"displayModeBar": False})
