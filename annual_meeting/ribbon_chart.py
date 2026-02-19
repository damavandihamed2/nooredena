import numpy as np
import pandas as pd
import requests as rq
import warnings, jdatetime, itertools

import plotly.express as px
import plotly.graph_objects as go

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

sectors_df = pd.DataFrame()
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
    portfolio = portfolio.groupby(by="sector", as_index=False).sum(numeric_only=True)
    portfolio = portfolio[["sector", "total_cost", "value"]]
    portfolio["date"] = [date] * len(portfolio)
    portfolio["value"] = 100 * (portfolio["value"] / sum(portfolio["value"]))
    # portfolio["sector"] = ["سایر" if portfolio["value"].iloc[i] < 2.5 else
    #                        portfolio["sector"].iloc[i] for i in range(len(portfolio))]
    portfolio = portfolio[["sector", "value"]]
    # portfolio = portfolio.groupby(by="sector", as_index=False).sum()
    portfolio.rename(mapper={"value": months["d"].iloc[m]}, axis=1, inplace=True)

    if m == 0:
        sectors_df = pd.concat([sectors_df, portfolio], axis=0, ignore_index=True)
    else:
        sectors_df = sectors_df.merge(portfolio, on="sector", how="outer")
sectors_df.fillna(value=0, inplace=True)
sectors_df.to_excel("C:/Users/damavandi/Desktop/sector_df_raw.xlsx", index=False)
##################################################################################################################

cols = sectors_df.columns.tolist()
cols.remove("sector")

sectors_df_ = pd.DataFrame()
for c in range(len(cols)):
    temp = sectors_df[["sector", cols[c]]]
    temp["sector"] = ["سایر" if temp[cols[c]].iloc[i] < 2.5 else temp["sector"].iloc[i]for i in range(len(temp))]
    temp = temp.groupby(by="sector", as_index=False).sum()
    if c == 0:
        sectors_df_ = pd.concat([sectors_df_, temp], axis=0, ignore_index=True)
    else:
        sectors_df_ = sectors_df_.merge(temp, on="sector", how="outer")
sectors_df = sectors_df_.copy()
# sectors_df.fillna(value=0, inplace=True)
sectors_df_.replace({"استخراج کانه های فلزی": "کانه ها", "بانکها و موسسات اعتباری": "بانک ها",
                            "عرضه برق،گاز،بخار و آب گرم": "یوتیلیتی", "خودرو و قطعات": "خودرو",
                            "ماشین آلات و تجهیزات": "ماشین آلات", "چندرشته ای صنعتی": "چندرشته ای",
                            "سیمان آهک گچ": "سیمان", "لاستیک و پلاستیک": "لاستیک",
                            "صندوق سرمایه گذاری قابل معامله": "صندوق"}, inplace=True)
##################################################################################################################

# cols = sectors_df.columns.tolist()
# cols.remove("sector")
#
# sectors_df_ = pd.DataFrame()
# for c in range(len(cols)):
#
#     temp = sectors_df[["sector", cols[c]]]
#     temp = temp[temp[cols[c]] > 0]
#     temp.rename({cols[c]: "value"}, axis=1, inplace=True)
#     temp["date"] = [cols[c]] * len(temp)
#     temp.sort_values("value", inplace=True, ignore_index=True, ascending=False)
#     sectors_df_ = pd.concat([sectors_df_, temp], axis=0, ignore_index=True)
#
# sectors_df_.to_excel("C:/Users/damavandi/Desktop/sector_df.xlsx", index=False)

##################################################################################################################
##################################################################################################################


def get_val(x, sub=0.1, axis='x'):
    if axis != 'x':
        a = [[i, i]for i in x]
    else:
        a = [[i-sub, i+sub]for i in x]
    return list(itertools.chain.from_iterable(a))


def getupperlower(sec):
    ch1 = final.query('sector == @sec')
    upper_col = [i for i in ch1.columns if 'upper' in i]
    lower_col = [i for i in ch1.columns if 'lower' in i]
    upper_data = ch1[upper_col].values.tolist()[0]
    lower_data = ch1[lower_col].values.tolist()[0]
    annotate_place = ch1['y0'].values.tolist()[0]
    return upper_data, lower_data, annotate_place

final = pd.DataFrame(sectors_df.sector)
cols = months["d"].values.tolist()
for i in range(len(cols)):
    ch1 = sectors_df.loc[:, ['sector', cols[i]]]
    ch1.sort_values(cols[i], inplace=True)
    ch1[f'y{i}_upper'] = ch1[cols[i]].cumsum()
    ch1[f'y{i}_lower'] = ch1[f'y{i}_upper'].shift(1)
    ch1 = ch1.fillna(0)
    ch1[f'y{i}'] = ch1.apply(lambda x: (x[f'y{i}_upper']+x[f'y{i}_lower'])/2, axis=1)
    final = final.merge(ch1.iloc[:, [0, 2, 3, 4]], on='sector')

pxcolors = ['#AA0DFE', '#3283FE', '#85660D', '#782AB6', '#565656', '#1C8356', '#16FF32', '#F7E1A0', '#E2E2E2', '#1CBE4F', '#C4451C', '#DEA0FD', '#FE00FA', '#325A9B', '#FEAF16', '#F8A19F', '#90AD1C', '#F6222E', '#1CFFCE', '#2ED9FF', '#B10DA1', '#C075A6', '#FC1CBF', '#B00068', '#FBE426', '#FA0087']
colors = {sectors_df["sector"].iloc[i]: pxcolors[i] for i in range(len(sectors_df))}
colors2 = [i for i in colors.values()]

##################################################################################################################
##################################################################################################################

fig = go.Figure()

x = np.arange(1, sectors_df.shape[1]).tolist()
x = get_val(x, sub=0.05)
x_rev = x[::-1]

annotations = []
sectors = sectors_df.sector.to_list()
for i in range(len(sectors)):

    i = 0

    sector = sectors[i]
    upper_col, lower_col, annotate_place = getupperlower(sector)
    y_upper = get_val(upper_col, axis='y')
    y_lower = get_val(lower_col, axis='y')
    y_lower = y_lower[::-1]
    fig.add_trace(go.Scatter(
        x=x + [x[-1] + 1, x[-1] + 1] + x_rev,
        y=y_upper + [y_upper[-1], y_lower[0]] + y_lower,
        fill='toself',
        fillcolor=colors2[i],
        opacity=0.7,
        showlegend=False,
        name=sector,
        line=dict(shape='spline', color='rgba(0,0,0,0.2)',),
        mode='lines',
        hovertemplate=' '
    ))
    annotations.append(dict(xref='paper', yref='y',
                            x=0, y=annotate_place,
                            text="<b>" + sector, align="right", xanchor='right',
                            font=dict(family='B Nazanin', size=15,
                                      color=colors2[i]),
                            showarrow=False))

ch1 = sectors_df.set_index('sector')
for i, j in enumerate(months["d"].values.tolist()):

    # i = 0
    ch2 = pd.DataFrame(ch1.T.iloc[i])
    ch2.columns = [i + 1]
    ch2.sort_values(i + 1, ascending=True, inplace=True)
    ch3 = ch2.T
    fig_px = px.bar(ch3, color_discrete_map=colors, opacity=1, text_auto=True)
    fig_px.update_traces(hovertemplate='%{x}, %{value}%')
    fig_px.update_traces(textfont=dict(family="B Nazanin", size=10, color='black'), textposition='inside', cliponaxis=False,
                         texttemplate='%{value}%')
    for trace in fig_px['data']:
        fig.add_trace(trace)

fig.update_layout(barmode='stack', bargap=1, showlegend=False)

fig.update_xaxes(range=[0.85, len(months) + 0.15], tickmode='array', showticklabels=True,
                 ticktext=months["d"].values.tolist(),
                 tickvals=(np.arange(len(months)) + 1).tolist(), fixedrange=True,
                 tickfont=dict(family="B Nazanin", size=20))
fig.update_yaxes(range=[0, 101], showticklabels=False, showgrid=False, fixedrange=True,
                 tickfont=dict(family="B Nazanin", size=20))
fig.update_layout(annotations=annotations)

fig.update_layout(
        title=dict(text="<b>" + "جریان صنایع در پرتفوی" "<b>",
                   font=dict(size=25, family="B Nazanin"),
                   xanchor="center"),
        template="seaborn",
        font_family="B Nazanin",
        margin=dict(l=150, r=50, t=50, b=50),
        height=900,
        plot_bgcolor='#f2f3f4',
        paper_bgcolor='#f2f3f4',
        showlegend=False
    )

fig.write_html("D:/fig_6.html")


##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################

def get_val(x, sub=0.1, axis='x'):
    if axis != 'x':
        a = [[i, i]for i in x]
    else:
        a = [[i-sub, i+sub]for i in x]
    return list(itertools.chain.from_iterable(a))


def getupperlower(brand):
    ch1 = final.query('brands == @brand')
    upper_col = [i for i in ch1.columns if 'upper' in i]
    lower_col = [i for i in ch1.columns if 'lower' in i]
    upper_data = ch1[upper_col].values.tolist()[0]
    lower_data = ch1[lower_col].values.tolist()[0]
    annotate_place = ch1['y0'].values.tolist()[0]
    return upper_data, lower_data, annotate_place


text = """brands q2_2021 q3_2021 q4_2021 q1_2022 q2_2022 q3_2022
BYD 0.07 0.11 0.12 0.14 0.16 0.2
Tesla 0.15 0.15 0.14 0.16 0.12 0.13
Wuling 0.07 0.06 0.05 0.06 0.05 0.05
Volkswagen 0.07 0.06 0.05 0.04 0.04 0.04
GAC 0.02 0.02 0.02 0.02 0.03 0.03
Others 0.62 0.60 0.62 0.58 0.6 0.55"""
data = [i.split(' ') for i in text.split("\n")]
df = pd.DataFrame(data[1:], columns=data[0])
to_join = df.iloc[:, 1:].astype('float')*100
df = pd.concat([df['brands'], to_join], axis=1)

final = pd.DataFrame(df.brands)
for i, col in enumerate(['q2_2021', 'q3_2021', 'q4_2021', 'q1_2022', 'q2_2022', 'q3_2022']):
    ch1 = df.loc[:, ['brands', col]]
    ch1.sort_values(col, inplace=True)
    ch1[f'y{i}_upper'] = ch1[col].cumsum()
    ch1[f'y{i}_lower'] = ch1[f'y{i}_upper'].shift(1)
    ch1 = ch1.fillna(0)
    ch1[f'y{i}'] = ch1.apply(lambda x: (x[f'y{i}_upper']+x[f'y{i}_lower'])/2, axis=1)
    final = final.merge(ch1.iloc[:, [0, 2, 3, 4]], on='brands')

colors = {'BYD Auto': '#72c6e8', 'Tesla': '#E41A37', 'Wuling': '#5c606d', 'Volkswagen': '#12618F',
          'GAC Motor': '#d9871b', 'Others': 'rgba(0,0,0,0.1)'}
colors2 = [i for i in colors.values()]

fig = go.Figure()

x = (np.arange(df.shape[1] - 1) + 1).tolist()
x = get_val(x, sub=0.15)
x_rev = x[::-1]

annotations = []

for i, brand in enumerate(df.brands):
    upper_col, lower_col, annotate_place = getupperlower(brand)
    y_upper = get_val(upper_col, axis='y')
    y_lower = get_val(lower_col, axis='y')
    y_lower = y_lower[::-1]
    fig.add_trace(go.Scatter(
        x=x + [x[-1] + 1, x[-1] + 1] + x_rev,
        y=y_upper + [y_upper[-1], y_lower[0]] + y_lower,
        fill='toself',
        fillcolor=colors2[i],
        opacity=0.5,
        line_color='rgba(0,0,0,0.2)',
        showlegend=False,
        name=brand,
        line_shape='spline',
        mode='lines',
        hovertemplate=' '
    ))
    annotations.append(dict(xref='paper', yref='y',
                            x=-0.005, y=annotate_place,
                            text=brand, align="right", xanchor='right',
                            font=dict(family='Arial', size=14,
                                      color=colors2[i]),
                            showarrow=False))

ch1 = df.set_index('brands')
for i, j in enumerate(['q2_2021', 'q3_2021', 'q4_2021', 'q1_2022', 'q2_2022', 'q3_2022']):

    ch2 = pd.DataFrame(ch1.T.iloc[i])
    ch2.columns = [i + 1]
    ch2.sort_values(i + 1, ascending=True, inplace=True)
    fig_px = px.bar(ch2.T, color_discrete_map=colors, opacity=0.7, text_auto=True)
    fig_px.update_traces(hovertemplate='<b>%{x}</b>, %{value}%')
    fig_px.update_traces(textfont=dict(size=10, color='black'), textposition='auto', cliponaxis=False,
                         texttemplate='%{value}%')
    for trace in fig_px['data']:
        fig.add_trace(trace)

annotation = []

fig.update_layout(barmode='stack', bargap=0.7, showlegend=False)

fig.update_layout(plot_bgcolor='#f2f3f4', paper_bgcolor='#f2f3f4', margin=dict(l=100, b=10, r=100))
fig.update_xaxes(range=[0.85, 6.15], tickmode='array', showticklabels=True,
                 ticktext=['q2_2021', 'q3_2021', 'q4_2021', 'q1_2022', 'q2_2022', 'q3_2022'],
                 tickvals=[1, 2, 3, 4, 5, 6], fixedrange=True)
fig.update_yaxes(range=[0, 101], showticklabels=False, showgrid=False, fixedrange=True)
fig.update_layout(annotations=annotations)
fig.update_layout(title='Global Passenger Electric Vehicle Market Share, Q2 2021 - Q3 2022')

fig.write_html("D:/fig_4.html")

