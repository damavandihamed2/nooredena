import warnings
import jdatetime
import pandas as pd

import dash_tvlwc
import plotly.express as px
import plotly.graph_objects as go
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots
from dash import Dash, html, dcc, callback, Output, Input, _dash_renderer

from utils.database import make_connection


warnings.filterwarnings("ignore")
colors_list = px.colors.qualitative.Dark24
_dash_renderer._set_react_version("18.2.0")
six_month_before = (jdatetime.datetime.now() - jdatetime.timedelta(days=180)).strftime(format="%Y-%m-%d")
powerbi_database = make_connection()
cmdty_options = pd.read_sql(f"SELECT DISTINCT(name) FROM [nooredenadb].[commodity].[commodities_data] WHERE date_jalali >= '{six_month_before}' ORDER BY name", powerbi_database)["name"].tolist()
chartoptions = {
    'layout': {'background': {'type': 'solid', 'color': '#ffffff'}, 'textColor': 'black', "attributionLogo": True},
    'grid': {'vertLines': {'visible': True, 'color': 'rgba(0,0,0,0.075)'}, 'horzLines': {'visible': True, 'color': 'rgba(0,0,0,0.075)'}},
    'localization': {'locale': 'fa-IR', 'dateFormat': "yyyy-MM-dd"},
}
########################################################################################################################

app = Dash(__name__, external_stylesheets=['./assets/stylesheet.css', dbc.themes.BOOTSTRAP])
app.__favicon = ("assets/favicon.ico")
app.title = "نمودار کامودیتی ها"
app.layout = html.Div([
    dbc.Row([html.H1(children='نمودار کامودیتی ها', style={'textAlign': 'center', 'font-family': 'B Nazanin'})]),
    dbc.Row([
        dbc.Col(
            dbc.Button("دانلود", outline=True, color="secondary", id="cmdty_xlsx", style={'textAlign': 'center', 'font-family': 'B Nazanin', "margin-left": "50px"}),
            width=1, align="center"),
        dbc.Col(
            dmc.MantineProvider(
                children=[dmc.MultiSelect(
                    # label="chose a commodity",
                    placeholder="select",
                    id='dropdown-selection',
                    data=cmdty_options,
                    value=["دلار - tgju.org (ریال بر دلار)"],
                    searchable=True,
                    hidePickedOptions=False,
                    checkIconPosition="left",
                    persistence=True,
                )],
                theme={'fontFamily': 'B Nazanin', "width": "100%", "components": {"MultiSelect": {"styles": {"dropdown": {'fontFamily': 'B Nazanin', "fontWeight": "20px"}}}}}
            ), width=11)
    ], justify="center"),

    dbc.Row([
        dbc.Col(
            dcc.Graph(id='table',
                      style={
                          'height': '85vh', # 'width': '25vw'
                      },
                      config={'displaylogo': False, "displayModeBar": False}
                      ),
            width=3),
        dbc.Col(
            [html.Div(
                dash_tvlwc.Tvlwc(
                    id='line-chart',
                    width='100%',
                    height='75vh',
                )
            ),
                dmc.MantineProvider(
                    children=[dmc.SegmentedControl(
                        id="price-scale",
                        data=[{"value": 0, "label": "نرمال"}, {"value": 1, "label": "لگاریتم"}, {"value": 2, "label": "درصد"}],
                        style={
                        #     "backgroundColor": "rgb(222, 222, 222, 1)",
                        #     "indicator": {
                        #         "backgroundColor": "rgb(222, 222, 222, 0.5)",
                        #         "color": "rgb(155, 0, 0, 1)"
                        #     },
                            "control": {"backgroundColor": "#FF00FF"},
                        },
                        # color="#FF0000",
                        radius="lg",
                        value=0,
                        persistence=True,
                        # orientation="horizontal",
                        # fullWidth=False,
                        size="md",
                        # transitionDuration=500,
                        # transitionTimingFunction="linear"
                    )],
                    # theme={
                    #     # 'fontFamily': 'B Nazanin', "width": "100%",
                    #     "components": {
                    #         "SegmentedControl": {
                    #             "styles": {
                    #                 "root": {
                    #                     "backgroundColor": "rgb(220, 220, 220, 0.05)",
                    #
                    #                          },
                    #                 "innerLabel": {
                    #                     'fontFamily': 'B Nazanin', "fontSize": "15px", "fontWeight": "1000",
                    #                                "color": "#000000"
                    #                                },
                    #                 # "input": {"backgroundColor": "#FF00FF"},
                    #                 "indicator": {"backgroundColor": "#FFFFFF",
                    #                               "color": "rgb(255, 255, 255, 1)"
                    #                                },
                    #                 # "control": {"backgroundColor": "#FF00FF"},
                    #                 # "label": {"backgroundColor": "#000000"},
                    #             }
                    #         }
                    #     }
                    # }
                ),



                # dbc.RadioItems(
                #     id="price-scale",
                #     className="btn-group",
                #     inputClassName="btn-check",
                #     labelClassName="btn btn-outline-primary",
                #     labelCheckedClassName="active",
                #     options=[{"label": "نرمال", "value": 0}, {"label": "لگاریتمی", "value": 1},
                #              {"label": "درصدی", "value": 2}], value=0, persistence=True, style={"font-family": "B Nazanin"})





            ],
            style={"margin-top": "50px"},
            width=9)
    ]),
    dcc.Download(id="download_dataframe")
])


@callback(
    Output('table', 'figure'),
    Output('line-chart', 'seriesData'),
    Output('line-chart', 'seriesTypes'),
    Output('line-chart', 'seriesOptions'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):

    fg = make_subplots(
        rows=1,
        cols=1,
        print_grid=False,
        column_widths=[1],
        specs=[[{"type": "table", "r": -0.05}]],
        subplot_titles=["جدول قیمت کامودیتی ها"])

    tv_data = []
    tv_types = ["line"] * len(value)
    tv_options = []

    names = []
    global df_table
    df_table = pd.DataFrame(columns=["date", "date_jalali"])
    for v in range(len(value)):
        df = pd.read_sql(f"SELECT [date], [date_jalali], [price] ,[name] ,[commodity], [reference], [unit] FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{value[v]}'", powerbi_database)
        n_ = df["commodity"].unique()[0]
        r_ = df["reference"].unique()[0]
        n_ = n_ + " - " + r_
        names.append(n_)
        df_temp = df[["date", "price"]].rename({"date": "time", "price": "value"}, axis=1, inplace=False)
        df_temp.sort_values(by="time", inplace=True, ignore_index=True, ascending=True)
        tv_data.append(df_temp.to_dict("records"))

        tv_options.append({
            "priceScaleMode": 1,
            'lastValueVisible': True,
            'lineWidth': 2,
            'lineStyle': 0,
            'title': f'{n_}',
            'color': f'{colors_list[v]}'
                           })

        df = df[["date", "date_jalali", "price"]]
        df.rename(mapper={"price": f"{n_}"}, axis=1, inplace=True)
        df_table = df_table.merge(df, on=["date", "date_jalali"], how="outer")

    df_table.sort_values(by="date", ascending=False, inplace=True, ignore_index=True)
    fig_ = go.Table(header=dict(values=["<b>تاریخ میلادی<b>", "<b>تاریخ شمسی<b>"] + [f"<b>{df_table.columns.tolist()[i]}<b>" for i in range(len(df_table.columns))][2:],
                                font=dict(size=15, family="B Nazanin"),
                                align="center"),
                    cells=dict(values=[list(df_table['date'].replace("-", "/", regex=True).values),
                                       list(df_table['date_jalali'].replace("-", "/", regex=True).values)] +
                                      [list(df_table[names[i]].values) for i in range(len(names))],
                               font=dict(size=15, family="B Nazanin"),
                               format=[[""], [""]] + [[",.1f"]] * len(value),
                               align="center",
                               height=35))
    fg.add_trace(fig_, row=1, col=1)
    fg.update_layout(template="seaborn",
                     font_family="B Nazanin",
                     margin=dict(l=50, r=50, t=50, b=50))

    return fg, tv_data, tv_types, tv_options

@callback(
    Output("line-chart", "chartOptions"),
    Input("price-scale", "value")
)
def price_scale(value):
    chartoptions["rightPriceScale"] = {"mode": value}
    return chartoptions


@callback(Output('download_dataframe', 'data'), Input('cmdty_xlsx', 'n_clicks'), prevent_initial_call=True)
def download_excel(n_clicks):
    now_ = jdatetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    writer = pd.ExcelWriter(f"commodities data ({now_}).xlsx", engine="xlsxwriter")

    df_table.to_excel(writer, sheet_name="data", index=False)

    df_table_ = df_table.fillna(method="bfill")
    df_table_.to_excel(writer, sheet_name="data_filled", index=False)

    df_table_monthly = pd.DataFrame()
    df_table_monthly[["year", "month", "day"]] = df_table_["date"].str.split("-", regex=False, expand=True)
    df_table_monthly = df_table_monthly.groupby(by=["year", "month"], as_index=False, sort=False).max()
    df_table_monthly["date"] = df_table_monthly["year"] + "-" + df_table_monthly["month"] + "-" + df_table_monthly["day"]
    df_table_monthly = df_table_monthly[["date"]]
    df_table_monthly = df_table_monthly.merge(df_table_, on="date", how="left")
    df_table_monthly.to_excel(writer, sheet_name="data_monthly", index=False)

    df_table_monthly = pd.DataFrame()
    df_table_monthly[["year_jalali", "month_jalali", "day_jalali"]] = df_table_["date_jalali"].str.split("-", regex=False, expand=True)
    df_table_monthly = df_table_monthly.groupby(by=["year_jalali", "month_jalali"], as_index=False, sort=False).max()
    df_table_monthly["date_jalali"] = df_table_monthly["year_jalali"] + "-" + df_table_monthly["month_jalali"] + "-" + df_table_monthly["day_jalali"]
    df_table_monthly = df_table_monthly[["date_jalali"]]
    df_table_monthly = df_table_monthly.merge(df_table_, on="date_jalali", how="left")
    df_table_monthly.to_excel(writer, sheet_name="data_jalali_monthly", index=False)

    writer.close()

    return dcc.send_file(f"commodities data ({now_}).xlsx")


if __name__ == '__main__':
    app.run_server(debug=True, host="127.0.0.1", port="9999")

#######################################################################################################################
