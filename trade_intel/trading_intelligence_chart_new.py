import warnings
import pandas as pd

import plotly.graph_objects as go
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots
from dash import Dash, html, dcc, callback, Input, Output, _dash_renderer

from utils.database import make_connection


warnings.filterwarnings("ignore")
_dash_renderer._set_react_version("18.2.0")
db_conn = make_connection()
dataframe = pd.read_sql("SELECT * FROM [nooredenadb].[extra].[trading_inteligence]", db_conn)
dataframe["value_diff_inactivity"] /= 1e9
dataframe["value_diff_passive"] /= 1e9
dataframe.rename(mapper={
    "date": "تاریخ", "portfolio_return_active": "بازدهی فعالیت", "portfolio_return_passive": "بازدهی منفعلانه",
    "total_index_return": "بازدهی شاخص کل", "price_index_eq_return": "بازدهی شاخص هم وزن",
    "inactivity_return": "بازدهی عدم فعالیت", "investment_sector_index_return": "بازدهی شاخص سرمایه گذاری ها",
    "top_30_index_return": "بازدهی شاخص 30 شرکت بزرگ", "value_diff_inactivity": "نفع فعالیت (عدم فعالیت)",
    "value_diff_passive": "نفع فعالیت (منفعلانه)"}, axis=1, inplace=True)
start_dates = dataframe["start_date"].unique().tolist()

df1 = dataframe.rename({"تاریخ": "date"}, axis=1, inplace=False).drop(columns=["نفع فعالیت (عدم فعالیت)", "start_date"], inplace=False)
df2 = dataframe.rename({"تاریخ": "date"}, axis=1, inplace=False)[["date", "نفع فعالیت (عدم فعالیت)"]]
app = Dash(__name__)
app.__favicon = ("assets/favicon.ico")
app.title = "هوشمندی معاملات"
app.layout = html.Div([
    dbc.Row(
        [
            dbc.Col(html.H2(id="title-component", children="هوشمندی معاملات از ابتدای سال مالی",
                            style={'textAlign': 'center', 'font-family': 'B Nazanin', "alignItems": "center"}),
                    style={"align": "center"},
                    width=4
                    )
        ],
        justify="center",
        align="center",
        style={"marginTop": 0, "marginButtom": 0, "height": 50}
    ),
    # dbc.Row(
    #     [
    #         dbc.Col(
    #             html.H2(
    #                 id="title-component",
    #                 children="هوشمندی معاملات از ابتدای سال مالی",
    #                 style={'textAlign': 'center', 'font-family': 'B Nazanin', "alignItems": "center"}
    #             ),
    #             style={"align": "center"},
    #             width=12
    #         )
    #     ],
    #     justify="center",
    #     align="center",
    #     style={"marginTop": 0, "marginButtom": 0, "height": 50}
    # ),
    dbc.Row(
        [
            dmc.MantineProvider(
                dmc.Stack(
                    [
                        dmc.Text("بازدهی", ta="center"),
                        dmc.AreaChart(
                            h=350,
                            withDots=False,
                            curveType="Bump",
                            dataKey="date",
                            withGradient=False,
                            withLegend=True,
                            unit="%",
                            strokeWidth=1,
                            legendProps={
                                "verticalAlign": "middle",
                                "height": 100,
                                # "aligb": "right",
                                # "layout": "vertical"
                            },
                            yAxisProps={
                                "tickMargin": 50,
                                "orientation": "right"},
                            data=df1.to_dict('records'),
                            series=[
                                {"name": "بازدهی فعالیت", "label": "فعالیت", "color": "lime.9"},
                                {"name": "بازدهی شاخص کل", "label": "شاخص", "color": "orange.9"},
                                {"name": "بازدهی شاخص هم وزن", "label": "هم وزن", "color": "blue.9"},
                                {"name": "بازدهی شاخص سرمایه گذاری ها", "label": "سرمایه گذاری ها", "color": "teal.9"},
                                {"name": "بازدهی شاخص 30 شرکت بزرگ", "label": "30 شرکت بزرگ", "color": "teal.9"},
                                {"name": "بازدهی عدم فعالیت", "label": "عدم فعالیت", "color": "red.9"}
                            ],
                            areaChartProps={"syncId": "trading_inteligence"},
                        ),
                        dmc.Text("نفع فعالیت (میلیارد ریال)", ta="center"),
                        dmc.AreaChart(
                            h=150,
                            withDots=True,
                            curveType="Bump",
                            dataKey="date",
                            type="split",
                            data=df2.to_dict('records'),
                            series=[{"name": "نفع فعالیت", "color": "teal.6"}],
                            yAxisProps={
                                "tickMargin": 15,
                                "orientation": "right"},
                            areaChartProps={"syncId": "trading_inteligence"},
                        ),
                    ]
                ),
                theme={
                    "components": {
                        "Text": {
                            "styles": {
                                "root": {
                                    'fontFamily': 'B Nazanin',
                                    "fontSize": "25px"
                                },

                            }
                        },
                        "AreaChart": {
                            "styles": {
                                "root": {
                                    'fontFamily': 'B Nazanin',
                                    "fontSize": "15px",
                                },
                                # "legend": {
                                #     "margin-left": "200px",
                                #     "flex": 1
                                # }
                            }
                        }
                    }
                }
            )
        ],
        justify="center",
        align="center",
        style={"margin-top": 0, "margin-left": 25, "margin-buttom": 20, "height": 50}
    )
])

if __name__ == "__main__":
    app.run_server(debug=False, host="127.0.0.1", port="8888")




# y_range = []
# figures = []
# fig = make_subplots(rows=2, cols=2,
#                     print_grid=False,
#                     horizontal_spacing=0.025,
#                     vertical_spacing=0.025,
#                     column_widths=[0.99, 0.01],
#                     specs=[
#                         [{"type": "xy", "secondary_y": True, "r": -0.01, "l": 0, "colspan": 1},
#                          {}],
#                         [{"type": "table", "t": 0.1, "b": -0.009, "r": -0.02, "colspan": 1},
#                          {"type": "indicator", "r": -5}]
#                     ])
#
# for d in range(len(start_dates)):
#
#     # d = 0
#
#     df = dataframe[dataframe["start_date"] == start_dates[d]]
#     df.reset_index(drop=True, inplace=True)
#
#     # round(df["نفع فعالیت"].abs().max() / 4)
#
#     y2_range = [
#         min(
#             round(float(min(df["نفع فعالیت"])) * 1.1),
#             -round(df["نفع فعالیت"].abs().max() / 4)
#         ),
#         max(
#             round(float(max(df["نفع فعالیت"])) * 1.1),
#             round(df["نفع فعالیت"].abs().max() / 4)
#         )]
#
#     y1_min = min(
#         round(min(df.drop(labels=["نفع فعالیت", "تاریخ", "start_date"], axis=1, inplace=False).min()) * 1.1,
#               ndigits=2), -0.1)
#     y1_max = max(
#         round(max(df.drop(labels=["نفع فعالیت", "تاریخ", "start_date"], axis=1, inplace=False).max()) * 1.1,
#               ndigits=2), 0.1)
#
#     if (y1_min * (-1) * y2_range[1]) / (y2_range[0] * (-1)) > y1_max:
#         y1_range = [y1_min * 100, ((y1_min * (-1) * y2_range[1]) / (y2_range[0] * (-1))) * 100]
#     else:
#         y1_range = [(((y1_max * (-1) * y2_range[0]) / y2_range[1]) * (-1)) * 100, y1_max * 100]
#
#     y_range.append({"y1_range": y1_range, "y2_range": y2_range})
#
#     # color = ["p" if df["نفع فعالیت"].iloc[i] > 0 else "n" for i in range(len(df))]
#     # fig_chart_1 = px.line(df, x="تاریخ", y=["بازدهی فعالیت", "بازدهی عدم فعالیت", "بازدهی شاخص کل", "بازدهی شاخص هم وزن",
#     #                                        "بازدهی شاخص سرمایه گذاری ها", "بازدهی شاخص 30 شرکت بزرگ"],
#     #                      color_discrete_sequence=["#00ff00", "#ff0000", "#ffff00", "#0000ff", "#000000", "#ff00ff"]
#     #                      )
#
#     fig_line_1 = go.Scatter(x=df["تاریخ"], y=df["بازدهی فعالیت"]*100, name="فعالیت", line={"color": "#00ff00", "width": 5, "dash": "solid"})
#     fig_line_2 = go.Scatter(x=df["تاریخ"], y=df["بازدهی عدم فعالیت"]*100, name="عدم فعالیت", line={"color": "#ff0000", "width": 5, "dash": "solid"})
#     fig_line_3 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص کل"]*100, name="شاخص کل", line={"color": "#ff8000", "width": 2, "dash": "longdashdot"})
#     fig_line_4 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص هم وزن"]*100, name="هم وزن", line={"color": "#0000ff", "width": 2, "dash": "longdashdot"})
#     fig_line_5 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص سرمایه گذاری ها"]*100, name="سرمایه گذاری ها", line={"color": "#000000", "width": 2, "dash": "dot"})
#     fig_line_6 = go.Scatter(x=df["تاریخ"], y=df["بازدهی شاخص 30 شرکت بزرگ"]*100, name="30 شرکت بزرگ", line={"color": "#ff00ff", "width": 2, "dash": "dot"})
#
#     df_area = df[["تاریخ", "نفع فعالیت"]]
#     # df_area["zeroline"] = [0] * len(df_area)
#
#     df_area_new = pd.DataFrame()
#     for i in range(1, len(df_area)-1):
#         if df_area["نفع فعالیت"].iloc[i] * df_area["نفع فعالیت"].iloc[i + 1] < 0:
#             total_days = (jdatetime.datetime.strptime(df_area["تاریخ"].iloc[i + 1], "%Y/%m/%d") - jdatetime.datetime.strptime(df_area["تاریخ"].iloc[i], "%Y/%m/%d")).days
#             diff = abs(df_area["نفع فعالیت"].iloc[i] - df_area["نفع فعالیت"].iloc[i + 1])
#             zero_day = round(total_days * (abs(df_area["نفع فعالیت"].iloc[i]) / diff))
#
#             new_day = (jdatetime.datetime.strptime(df_area["تاریخ"].iloc[i], "%Y/%m/%d") + jdatetime.timedelta(days=zero_day)).strftime("%Y/%m/%d")
#             df_area_new = pd.concat([df_area_new, pd.DataFrame(data={"تاریخ": [new_day], "نفع فعالیت": [0]})], axis=0, ignore_index=True)
#     df_area = pd.concat([df_area, df_area_new], axis=0)
#     df_area.sort_values(by="تاریخ", inplace=True, ignore_index=True)
#
#     # fig_chart_2 = go.Scatter(x=df["تاریخ"], y=df["نفع فعالیت"], name="نفع فعالیت",
#     #                          # text=df['نفع فعالیت'].astype("str").values.tolist(),
#     #                          # texttemplate="<b>{text:,.0f}<b>", textfont={"size": 12},
#     #                          fill="tozeroy", fillcolor="rgba(150, 114, 44, 0.35)",
#     #                          mode="none", legendgroup="نفع فعالیت"
#     #                          )
#
#     df["بازدهی فعالیت"] = (round(100 * df["بازدهی فعالیت"], ndigits=1)).astype("str")
#     df["بازدهی عدم فعالیت"] = round(100 * df["بازدهی عدم فعالیت"], ndigits=1).astype("str")
#     df["بازدهی شاخص کل"] = round(100 * df["بازدهی شاخص کل"], ndigits=1).astype("str")
#     df["بازدهی شاخص هم وزن"] = round(100 * df["بازدهی شاخص هم وزن"], ndigits=1).astype("str")
#     df["بازدهی شاخص سرمایه گذاری ها"] = round(100 * df["بازدهی شاخص سرمایه گذاری ها"], ndigits=1).astype("str")
#     df["بازدهی شاخص 30 شرکت بزرگ"] = round(100 * df["بازدهی شاخص 30 شرکت بزرگ"], ndigits=1).astype("str")
#     df["نفع فعالیت"] = round(df["نفع فعالیت"]).astype("int")
#     df["نفع فعالیت"] = [f"{df['نفع فعالیت'].iloc[i]:,}" for i in range(len(df["نفع فعالیت"]))]
#     df.rename(mapper={
#         "نفع فعالیت": "نفع فعالیت (میلیارد ریال)",
#         "بازدهی فعالیت": "بازدهی فعالیت (درصد)",
#         "بازدهی عدم فعالیت": "بازدهی عدم فعالیت (درصد)",
#         "بازدهی شاخص کل": "بازدهی شاخص کل (درصد)",
#         "بازدهی شاخص هم وزن": "بازدهی هم وزن (درصد)",
#         "بازدهی شاخص سرمایه گذاری ها": "بازدهی سرمایه گذاری ها (درصد)",
#         "بازدهی شاخص 30 شرکت بزرگ": "بازدهی شاخص 30 شرکت بزرگ (درصد)",
#     }, axis=1, inplace=True)
#
#     df.drop(columns="start_date", inplace=True)
#
#     df_ = df.T
#     df_.reset_index(drop=False, inplace=True)
#     df_.columns = df_.iloc[0, :]
#     df_ = df_.iloc[1:, :]
#     df_["تاریخ"] = "<b>" + df_["تاریخ"] + "<b>"
#     df_.drop(labels=start_dates[d], axis=1, inplace=True)
#
#     table_columns = df_.columns.tolist()
#     table_colors = []
#     for c in range(len(table_columns)):
#         if df_[table_columns[c]].iloc[0].replace(",", "").replace(".", "").replace("-", "").isnumeric():
#             tmp_clr = []
#             for i in range(len(df_)):
#                 if float(df_[table_columns[c]].iloc[i].replace(",", "")) > 0:
#                     tmp_clr.append("#009600")
#                 elif float(df_[table_columns[c]].iloc[i].replace(",", "")) < 0:
#                     df_[table_columns[c]].iloc[i] = "(" + df_[table_columns[c]].iloc[i].replace("-", "") + ")"
#                     tmp_clr.append("#c80000")
#                 else:
#                     tmp_clr.append("#000000")
#             table_colors.append(tmp_clr)
#         else:
#             table_colors.append(["#000000"] * len(df_))
#
#     # df_["نفع فعالیت (میلیارد ریال)"] = ["<b>" + df_["نفع فعالیت (میلیارد ریال)"].iloc[i] + "<b>" for i in range(len(df_))]
#     df_.iloc[-1, -1] = "<b>" + df_.iloc[-1, -1] + "<b>"
#
#     fig_indicator = go.Indicator(
#         mode="number",
#         value=int(len(df) - 1),
#         title={"text": "تعداد ماه ها"}
#     )
#
#     fig_table = go.Table(header=dict(values=("<b>" + df_.columns + "<b>").tolist(),
#                                      font=dict(size=16, family="B Nazanin"),
#                                      align="center",
#                                      fill=dict(color=["#b7b7bf"] + ["#e7e7f0"] * 6)),
#                          cells=dict(values=[df_[df_.columns[i]].values.tolist() for i in range(len(df_.columns))],
#                                     font=dict(size=20, family="B Nazanin", color=table_colors),
#                                     format=[[""]
#                                         # , [".1%"], [".1%"], [",.0f"], [".1%"], [".1%"], [".1%"]
#                                             ],
#                                     align="center",
#                                     fill=dict(color=["#b7b7bf"] + ["#e7e7f0"] * 6),
#                                     height=40),
#                          columnwidth=[4, 1, 1, 1, 1, 1, 1])
#
#     # fig.add_trace(fig_chart_2, row=1, col=1, secondary_y=True)
#     idx = 0
#     fig_number = 0
#     for i in range(1, len(df_area)):
#         if df_area["نفع فعالیت"].iloc[i] == 0:
#
#             fig_number += 1
#
#             temp_df = df_area.iloc[idx: i+1]
#             temp_df.reset_index(drop=True, inplace=True)
#
#             color_ = "rgba(255, 0, 0, 0.25)" if temp_df["نفع فعالیت"].iloc[1] < 0 else "rgba(0, 255, 0, 0.25)"
#             legnd = True if idx == 0 else False
#             fig_temp = go.Scatter(
#                 x=temp_df["تاریخ"],
#                 y=temp_df["نفع فعالیت"],
#                 name="نفع فعالیت",
#                 # text=df['نفع فعالیت'].astype("str").values.tolist(),
#                 # texttemplate="<b>{text:,.0f}<b>", textfont={"size": 12},
#                 fill="tozeroy",
#                 fillcolor=color_,
#                 mode="none",
#                 legendgroup="group1",
#                 showlegend=legnd
#             )
#             idx = i
#             fig.add_trace(fig_temp, row=1, col=1, secondary_y=True)
#
#
#         elif i == (len(df_area) - 1):
#
#             fig_number += 1
#
#             temp_df = df_area.iloc[idx:]
#             temp_df.reset_index(drop=True, inplace=True)
#
#             color_ = "rgba(255, 0, 0, 0.25)" if temp_df["نفع فعالیت"].iloc[1] < 0 else "rgba(0, 255, 0, 0.25)"
#             legnd = True if idx == 0 else False
#             fig_temp = go.Scatter(
#                 x=temp_df["تاریخ"],
#                 y=temp_df["نفع فعالیت"],
#                 name="نفع فعالیت",
#                 # text=df['نفع فعالیت'].astype("str").values.tolist(),
#                 # texttemplate="<b>{text:,.0f}<b>", textfont={"size": 12},
#                 fill="tozeroy",
#                 fillcolor=color_,
#                 mode="none",
#                 legendgroup="group1",
#                 showlegend=legnd
#             )
#             fig.add_trace(fig_temp, row=1, col=1, secondary_y=True)
#
#     fig.add_trace(fig_line_1, row=1, col=1, secondary_y=False)
#     fig.add_trace(fig_line_2, row=1, col=1, secondary_y=False)
#     fig.add_trace(fig_line_3, row=1, col=1, secondary_y=False)
#     fig.add_trace(fig_line_4, row=1, col=1, secondary_y=False)
#     fig.add_trace(fig_line_5, row=1, col=1, secondary_y=False)
#     fig.add_trace(fig_line_6, row=1, col=1, secondary_y=False)
#
#     fig.add_trace(fig_table, row=2, col=1)
#
#     fig.add_trace(fig_indicator, row=2, col=2)
#
#     figures.append(8+fig_number)
#
# fig.update_xaxes(tickfont=dict(family="B Nazanin", size=18), calendar="jalali", tickformat="%Y/%m/%d", showgrid=False, tickangle=45)
#
# fig.update_yaxes(tickfont=dict(family="B Nazanin", size=18), exponentformat="none", separatethousands=True,
#                  tickprefix=" ", ticksuffix="% ", showgrid=False, zeroline=True, zerolinewidth=4, secondary_y=False,
#                  range=y_range[0]["y1_range"])
# fig.update_yaxes(tickfont=dict(family="B Nazanin", size=18), exponentformat="none", separatethousands=True,
#                  tickprefix=" ", secondary_y=True, range=y_range[0]["y2_range"]
#                  )
# fig.update_layout(legend={"x": 0.95, "y": 1})
#
# labels = start_dates
# titles = ["", ""]
#
# buttons = [dict(label="<b>" + labels[0] + "</b>",
#                 method="update",
#                 args=[{
#                     # "visible": int(len(fig.data)/len(start_dates))*[True] + int(len(fig.data)/len(start_dates))*[False]},
#                     "visible": figures[0]*[True] + figures[1]*[False]},
#                       {"title": f"<b>بازدهی تجمعی ماهانه از تاریخ  {labels[0]}</b>",
#                        "yaxis.range": y_range[0]["y1_range"], "yaxis2.range": y_range[0]["y2_range"]
#                        },
#                       # {},
#                       #  #
#                       #  }
#                       ]),
#            dict(label="<b>" + labels[1] + "</b>",
#                 method="update",
#                 args=[{
#                     # "visible": int(len(fig.data)/len(start_dates))*[False] + int(len(fig.data)/len(start_dates))*[True]},
#                     "visible": figures[0]*[False] + figures[1]*[True]},
#                       {"title": f"<b>بازدهی تجمعی ماهانه از تاریخ  {labels[1]}</b>",
#                        "yaxis.range": y_range[1]["y1_range"], "yaxis2.range": y_range[1]["y2_range"]
#                        },
#                       # {}
#                       #     # ,
#                       #  }
#                       ])]
# # [
# #     {
# #         'args': [{'xaxis.range': [w - 0.5, 10.5 + w]}],
# #         'method': 'relayout'
# #     } for w in range(n - 10)
# # ]
#
#
# updatemenus = [{"active": 1, "x": 0.99, "y": 1.05, "buttons": buttons, "type": "buttons", "direction": "left"}]
# fig["layout"]["updatemenus"] = updatemenus
#
# fig.update_layout(
#     template="gridon",
#     font_family="B Nazanin",
#     margin=dict(l=50, r=25, t=50, b=50),
#     height=1300,
#     # showlegend=False,
#     title=dict(text=f"<b>بازدهی تجمعی ماهانه از تاریخ  {labels[0]}</b>", xanchor="center", font=dict(size=24)))
#
# for k in range(len(fig.data)):
#     # if k > len(fig.data)/2 - 1:
#     if k > figures[0] - 1:
#         fig.update_traces(visible=True, selector=k)
#     else:
#         fig.update_traces(visible=False, selector=k)
#
# fig.write_html("D:/database/trading_inteligence/trading_inteligence_chart_.html", config={"displayModeBar": False})
