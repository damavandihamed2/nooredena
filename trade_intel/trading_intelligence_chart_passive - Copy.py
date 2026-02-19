import numpy as np
import pandas as pd
import warnings, jdatetime

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
dataframe = pd.read_sql("SELECT * FROM [nooredenadb].[extra].[trading_inteligence]", db_conn)

dataframe.drop(["inactivity_return", "value_diff_inactivity"], axis=1, inplace=True)
dataframe["value_diff_passive"] /= 1e9
dataframe.rename(mapper={
    "date": "تاریخ", "portfolio_return_active": "بازدهی فعالیت", "portfolio_return_passive": "بازدهی منفعلانه",
    "total_index_return": "بازدهی شاخص کل", "price_index_eq_return": "بازدهی شاخص هم وزن",
    "investment_sector_index_return": "بازدهی شاخص سرمایه گذاری ها", "top_30_index_return": "بازدهی شاخص 30 شرکت بزرگ",
    "value_diff_passive": "نفع فعالیت (منفعلانه)"}, axis=1, inplace=True)

def area_line_fig(x, y, threshold):
    threshold = threshold
    x2=[]
    y2=[]
    for i in range(len(y)-1):
        x2 += [x[i]]
        y2  += [y[i]]
        if y[i] > threshold > y[i + 1] or y[i] < threshold < y[i + 1]:
            Xi = x[i] + ((threshold - y[i]) * (x[i + 1] - x[i]) / (y[i + 1] - y[i]))
            x2 += [Xi]
            y2 +=[threshold]
    x2 +=[x[-1]]
    y2 +=[y[-1]]
    x2 = np.array(x2)
    y2 = np.array(y2)
    mask = y2>=threshold
    mask2 = y2<=threshold
    fig_positive = go.Scatter(x=x2[mask], y=np.sin(x2)[mask], mode='lines', fill='tozeroy', fillcolor='green')
    fig_negative = go.Scatter(x=x2[mask2], y=y2[mask2], mode='lines', fill='tozeroy', fillcolor='red')
    return {"fig_positive": fig_positive, "fig_negative": fig_negative}


start_dates = "1402/10/30"

# y_range = []
figures = []
fig = make_subplots(rows=3, cols=2,
                    print_grid=True,
                    horizontal_spacing=0.025,
                    vertical_spacing=0.025,
                    row_heights=[1, 1, 1.5],
                    column_widths=[0.99, 0.01],
                    specs=[
                        [{"type": "xy", "secondary_y": True, "r": -0.01, "l": 0, "colspan": 1},
                         {}],
                        [{"type": "xy", "secondary_y": True, "r": -0.01, "l": 0, "colspan": 1},
                         {}],
                        [{"type": "table", "t": 0.1, "b": -0.009, "r": -0.02, "colspan": 1},
                         {"type": "indicator", "r": -5, "t": 0.1}]
                    ])


df = dataframe[dataframe["start_date"] == start_dates]


fig_line_1 = go.Scatter(x=df["تاریخ"], y=df["بازدهی فعالیت"]*100, name="فعالیت", line={"color": "#00ff00", "width": 5, "dash": "solid"})
fig_line_2 = go.Scatter(x=df["تاریخ"], y=df["بازدهی منفعلانه"]*100, name="فعالیت", line={"color": "#00ff00", "width": 5, "dash": "solid"})
fig_line_3 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص کل"]*100, name="شاخص کل", line={"color": "#ff8000", "width": 2, "dash": "longdashdot"})
fig_line_4 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص هم وزن"]*100, name="هم وزن", line={"color": "#0000ff", "width": 2, "dash": "longdashdot"})
fig_line_5 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص سرمایه گذاری ها"]*100, name="سرمایه گذاری ها", line={"color": "#000000", "width": 2, "dash": "dot"})
fig_line_6 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص 30 شرکت بزرگ"]*100, name="30 شرکت بزرگ", line={"color": "#ff00ff", "width": 2, "dash": "dot"})

df_area = df[["تاریخ", "نفع فعالیت (منفعلانه)"]]
df_area_new = pd.DataFrame()
for i in range(1, len(df_area) - 1):
    if df_area["نفع فعالیت (منفعلانه)"].iloc[i] * df_area["نفع فعالیت (منفعلانه)"].iloc[i + 1] < 0:
        total_days = (jdatetime.datetime.strptime(df_area["تاریخ"].iloc[i + 1], "%Y/%m/%d") - jdatetime.datetime.strptime(df_area["تاریخ"].iloc[i], "%Y/%m/%d")).days
        diff = abs(df_area["نفع فعالیت (منفعلانه)"].iloc[i] - df_area["نفع فعالیت (منفعلانه)"].iloc[i + 1])
        zero_day = round(total_days * (abs(df_area["نفع فعالیت (منفعلانه)"].iloc[i]) / diff))

        new_day = (jdatetime.datetime.strptime(df_area["تاریخ"].iloc[i], "%Y/%m/%d") + jdatetime.timedelta(days=zero_day)).strftime("%Y/%m/%d")
        df_area_new = pd.concat([df_area_new, pd.DataFrame(data={"تاریخ": [new_day], "نفع فعالیت (منفعلانه)": [0]})], axis=0, ignore_index=True)
df_area = pd.concat([df_area, df_area_new], axis=0)
df_area.sort_values(by="تاریخ", inplace=True, ignore_index=True)

# fig_chart_2 = go.Scatter(x=df["تاریخ"], y=df["نفع فعالیت (منفعلانه)"], name="نفع فعالیت (منفعلانه)",
#                          # text=df["نفع فعالیت (منفعلانه)"].astype("str").values.tolist(),
#                          # texttemplate="<b>{text:,.0f}<b>", textfont={"size": 12},
#                          fill="tozeroy", fillcolor="rgba(150, 114, 44, 0.35)",
#                          mode="none", legendgroup="نفع فعالیت (منفعلانه)"
#                          )

df["بازدهی فعالیت"] = (round(100 * df["بازدهی فعالیت"], ndigits=1)).astype("str")
df["بازدهی منفعلانه"] = round(100 * df["بازدهی منفعلانه"], ndigits=1).astype("str")
df["بازدهی شاخص کل"] = round(100 * df["بازدهی شاخص کل"], ndigits=1).astype("str")
df["بازدهی شاخص هم وزن"] = round(100 * df["بازدهی شاخص هم وزن"], ndigits=1).astype("str")
df["بازدهی شاخص سرمایه گذاری ها"] = round(100 * df["بازدهی شاخص سرمایه گذاری ها"], ndigits=1).astype("str")
df["بازدهی شاخص 30 شرکت بزرگ"] = round(100 * df["بازدهی شاخص 30 شرکت بزرگ"], ndigits=1).astype("str")
df["نفع فعالیت (منفعلانه)"] = round(df["نفع فعالیت (منفعلانه)"]).astype("int")
df["نفع فعالیت (منفعلانه)"] = [f"{df['نفع فعالیت (منفعلانه)'].iloc[i]:,}" for i in range(len(df['نفع فعالیت (منفعلانه)']))]

df.rename(mapper={
    "نفع فعالیت (منفعلانه)": "نفع فعالیت (منفعلانه) (میلیارد ریال)",
    "بازدهی فعالیت": "بازدهی فعالیت (درصد)",
    "بازدهی منفعلانه": "بازدهی منفعلانه (درصد)",
    "بازدهی شاخص کل": "بازدهی شاخص کل (درصد)",
    "بازدهی شاخص هم وزن": "بازدهی هم وزن (درصد)",
    "بازدهی شاخص سرمایه گذاری ها": "بازدهی سرمایه گذاری ها (درصد)",
    "بازدهی شاخص 30 شرکت بزرگ": "بازدهی شاخص 30 شرکت بزرگ (درصد)",
}, axis=1, inplace=True)

df.drop(columns="start_date", inplace=True)

df_ = df.T
df_.reset_index(drop=False, inplace=True)
df_.columns = df_.iloc[0, :]
df_ = df_.iloc[1:, :]
df_["تاریخ"] = "<b>" + df_["تاریخ"] + "<b>"
df_.drop(labels=start_dates, axis=1, inplace=True)

table_columns = df_.columns.tolist()
table_colors = []
for c in range(len(table_columns)):
    if df_[table_columns[c]].iloc[0].replace(",", "").replace(".", "").replace("-", "").isnumeric():
        tmp_clr = []
        for i in range(len(df_)):
            if float(df_[table_columns[c]].iloc[i].replace(",", "")) > 0:
                tmp_clr.append("#009600")
            elif float(df_[table_columns[c]].iloc[i].replace(",", "")) < 0:
                df_[table_columns[c]].iloc[i] = "(" + df_[table_columns[c]].iloc[i].replace("-", "") + ")"
                tmp_clr.append("#c80000")
            else:
                tmp_clr.append("#000000")
        table_colors.append(tmp_clr)
    else:
        table_colors.append(["#000000"] * len(df_))

# df_["نفع فعالیت (منفعلانه) (میلیارد ریال)"] = ["<b>" + df_["نفع فعالیت (منفعلانه) (میلیارد ریال)"].iloc[i] + "<b>" for i in range(len(df_))]
df_.iloc[-1, -1] = "<b>" + df_.iloc[-1, -1] + "<b>"

fig_indicator = go.Indicator(
    mode="number",
    value=int(len(df) - 1),
    title={"text": "تعداد ماه ها"}
)

fig_table = go.Table(header=dict(values=("<b>" + df_.columns + "<b>").tolist(),
                                 font=dict(size=16, family="B Nazanin"),
                                 align="center",
                                 fill=dict(color=["#b7b7bf"] + ["#e7e7f0"] * 6)),
                     cells=dict(values=[df_[df_.columns[i]].values.tolist() for i in range(len(df_.columns))],
                                font=dict(size=20, family="B Nazanin", color=table_colors),
                                format=[[""]
                                    # , [".1%"], [".1%"], [",.0f"], [".1%"], [".1%"], [".1%"]
                                        ],
                                align="center",
                                fill=dict(color=["#b7b7bf"] + ["#e7e7f0"] * 6),
                                height=40),
                     columnwidth=[4, 1, 1, 1, 1, 1, 1])

# fig.add_trace(fig_chart_2, row=1, col=1, secondary_y=True)

##################################################################################################################
##################################################################################################################


idx = 0
fig_number = 0
for i in range(1, len(df_area)):
    if df_area["نفع فعالیت (منفعلانه)"].iloc[i] == 0:

        fig_number += 1

        temp_df = df_area.iloc[idx: i + 1]
        temp_df.reset_index(drop=True, inplace=True)

        color_ = "rgba(255, 0, 0, 0.25)" if temp_df["نفع فعالیت (منفعلانه)"].iloc[1] < 0 else "rgba(0, 255, 0, 0.25)"
        legnd = True if idx == 0 else False
        fig_temp = go.Scatter(
            x=temp_df["تاریخ"],
            y=temp_df["نفع فعالیت (منفعلانه)"],
            name="نفع فعالیت (منفعلانه)",
            # text=df["نفع فعالیت (منفعلانه)"].astype("str").values.tolist(),
            # texttemplate="<b>{text:,.0f}<b>", textfont={"size": 12},
            fill="tozeroy",
            fillcolor=color_,
            mode="none",
            legendgroup="group1",
            showlegend=legnd
        )
        idx = i
        fig.add_trace(fig_temp, row=2, col=1, secondary_y=True)


    elif i == (len(df_area) - 1):

        fig_number += 1

        temp_df = df_area.iloc[idx:]
        temp_df.reset_index(drop=True, inplace=True)

        color_ = "rgba(255, 0, 0, 0.25)" if temp_df["نفع فعالیت (منفعلانه)"].iloc[1] < 0 else "rgba(0, 255, 0, 0.25)"
        legnd = True if idx == 0 else False
        fig_temp = go.Scatter(
            x=temp_df["تاریخ"],
            y=temp_df["نفع فعالیت (منفعلانه)"],
            name="نفع فعالیت (منفعلانه)",
            # text=df["نفع فعالیت (منفعلانه)"].astype("str").values.tolist(),
            # texttemplate="<b>{text:,.0f}<b>", textfont={"size": 12},
            fill="tozeroy",
            fillcolor=color_,
            mode="none",
            legendgroup="group1",
            showlegend=legnd
        )
        fig.add_trace(fig_temp, row=2, col=1, secondary_y=True)

fig.add_trace(fig_line_1, row=1, col=1, secondary_y=False)
fig.add_trace(fig_line_2, row=1, col=1, secondary_y=False)
fig.add_trace(fig_line_3, row=1, col=1, secondary_y=False)
fig.add_trace(fig_line_4, row=1, col=1, secondary_y=False)
fig.add_trace(fig_line_5, row=1, col=1, secondary_y=False)
fig.add_trace(fig_line_6, row=1, col=1, secondary_y=False)

fig.add_trace(fig_table, row=3, col=1)

fig.add_trace(fig_indicator, row=3, col=2)

# areas = area_line_fig(x=range(len(dataframe)), y=dataframe["نفع فعالیت (منفعلانه)"].tolist(), threshold=0)
# fig_positive = areas["fig_positive"]
# fig_negative = areas["fig_negative"]
#
# fig_positive["line"]["smoothing"] = 1.3
# fig_negative["line"]["smoothing"] = 1.3
#
# fig.add_trace(fig_positive, row=2, col=1)
# fig.add_trace(fig_negative, row=2, col=1)


fig.update_xaxes(tickfont=dict(family="B Nazanin", size=18), calendar="jalali", tickformat="%Y/%m/%d",
                 showgrid=False, tickangle=30)

fig.update_yaxes(tickfont=dict(family="B Nazanin", size=18),
                 # exponentformat="none", separatethousands=True,
                 # tickprefix=" ", ticksuffix="% ", showgrid=False,
                 zeroline=True,
                 zerolinewidth=4,
                 )
fig.update_layout(legend={"x": 0.95, "y": 1})
fig.update_layout(
    template="gridon",
    font_family="B Nazanin",
    margin=dict(l=50, r=25, t=50, b=50),
    height=1500,
    title=dict(text=f"<b>بازدهی تجمعی ماهانه از تاریخ  {start_dates}</b>", xanchor="center", font=dict(size=24)))
fig.show()
fig.write_html("D:/database/trading_inteligence/trading_inteligence_chart_passive.html", config={"displayModeBar": False})
