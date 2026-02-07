import warnings
import pandas as pd

import plotly.express as px
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, callback, Input, Output, _dash_renderer

from utils.database import make_connection
from board_meeting_report import weekly_report_data
from board_meeting_report.dashboard_layout import tab_portfolio, portfolio_columns
from board_meeting_report.dashboard_layout import tab_trades, buy_columns, sell_columns, value_formatter_unified
from board_meeting_report.dashboard_layout import tab_bazdehi, bazdehi_columns_items, bazdehi_columns_total, bazdehi_columns_data

market_return = 0.3
warnings.filterwarnings("ignore")
_dash_renderer._set_react_version("18.2.0")
powerbi_database = make_connection()


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.GRID, './assets/styles-ag-grid.css'],
    # external_scripts = ['./assets/d3.min.js']
)
app.__favicon = ("assets/favicon.ico")
app.title = "پرتفوی"
app.layout = html.Div([

    dbc.Row(
        [
            dbc.Col(
                html.H2(
                    id="title-component",
                    style={'textAlign': 'center', 'font-family': 'B Nazanin', "alignItems": "center"}
                ),
                style={"align": "center"},
                width=4
            )
        ],
        justify="center",
        align="center",
        style={"marginTop": 10, "marginBottom": 0, "height": 35}
    ),

    dmc.MantineProvider(
        dmc.Tabs(
            [
                dmc.TabsList(
                    children=[
                        dmc.TabsTab("بازدهی", value="return"),
                        dmc.TabsTab("معاملات", value="trades"),
                        dmc.TabsTab("وضعیت کلی", value="portfolio"),
                    ],
                    justify="flex-end"
                ),

                # Tab 3 ----------------------------------------
                tab_bazdehi,

                # Tab 2 ----------------------------------------
                tab_trades,

                # Tab 1 ----------------------------------------
                tab_portfolio,

            ],
            color="blue",
            orientation="horizontal",
            value="portfolio",
        ),
        theme={"components": {"Tabs": {"styles": {"root": {"margin-left": "10px"},
                                                  "tabLabel": {'fontFamily': 'B Nazanin', "fontSize": "20px"}}}}}
    ),
    dcc.Interval(id="interval-component", interval=60 * (60 * 1000)) # refreshing every 60 minutes
],
    style={"width": "99%"}
)


@callback(
    [
        Output(component_id="title-component", component_property="children"),

        Output(component_id="table-1", component_property="rowData"),
        Output(component_id="table-1", component_property="dashGridOptions"),
        Output(component_id="table-1", component_property="columnDefs"),

        Output(component_id="tab-buy-title", component_property="children"),
        Output(component_id="table-buy", component_property="rowData"),
        Output(component_id="table-buy", component_property="dashGridOptions"),
        Output(component_id="table-buy", component_property="columnDefs"),
        Output(component_id="fig-buy", component_property="figure"),

        Output(component_id="tab-sell-title", component_property="children"),
        Output(component_id="table-sell", component_property="rowData"),
        Output(component_id="table-sell", component_property="dashGridOptions"),
        Output(component_id="table-sell", component_property="columnDefs"),
        Output(component_id="fig-sell", component_property="figure"),

        Output(component_id="table-3", component_property="rowData"),
        Output(component_id="table-3", component_property="columnDefs"),

     ],
    [Input(component_id="interval-component", component_property="n_intervals")]
)
def report_updater(n_intervals):

    portfolio = weekly_report_data.get_portfolio(include_avalhami=True, main_portfolio=True, prx_portfolio=True)
    portfolio["profitloss"] /= 1e9
    portfolio["value"] /= 1e9
    portfolio["total_cost"] /= 1e9
    date_, time_ = portfolio["date"].iloc[0], portfolio["time"].iloc[0]
    title_ = "وضعیت پرتفوی - " + f"{time_} {date_}"
    portfolio.drop(columns=["date", "time"], inplace=True)
    portfolio["index"] = range(1, len(portfolio) + 1)
    portfolio_total = pd.DataFrame([{"symbol": "مجموع", "total_cost": portfolio["total_cost"].sum(),
                                     "value": portfolio["value"].sum(), "profitloss": portfolio["profitloss"].sum(),
                                     "profitloss_percent": portfolio["profitloss"].sum() / portfolio["total_cost"].sum()}])
    table1_data = portfolio.to_dict('records')

    trades_start_date = weekly_report_data.get_last_board_meeting_date()
    trades_end_date = weekly_report_data.get_last_trade_date()

    trades_title_buy = "خرید ها از تاریخ  " + f" {trades_start_date} " + "  تا  " + f" {trades_end_date} "
    trades_buy = weekly_report_data.get_trades_buy(start_date=trades_start_date, end_date=trades_end_date,
                                                   fix_exclude=True, main_portfolio=True, prx_portfolio=True)
    table_buy = trades_buy["buy"]
    table_buy["value"] /= 1e6
    table_buy["index"] = range(1, len(table_buy) + 1)
    table_buy_total = trades_buy["buy_total"]
    table_buy_total["value"] /= 1e6
    fig_buy = px.sunburst(
        pd.DataFrame(table_buy), path=[px.Constant('خرید'), 'sector_name', 'symbol', 'basket'], values='value')
    fig_buy.update_traces(textinfo='label+value', texttemplate='%{label}<br>%{value:,.0f}')
    fig_buy.update_layout(font=dict(family="B Nazanin", size=20), margin=dict(l=0, r=0, t=0, b=0))

    trades_title_sell = "فروش ها از تاریخ  " + f" {trades_start_date} " + "  تا  " + f" {trades_end_date} "
    trades_sell = weekly_report_data.get_trades_sell(start_date=trades_start_date, end_date=trades_end_date,
                                                     fix_exclude=True, main_portfolio=True, prx_portfolio=True)
    table_sell = trades_sell["sell"]
    table_sell["value"] /= 1e6
    table_sell["total_cost"] /= 1e6
    table_sell["profitloss"] /= 1e6
    table_sell["index"] = range(1, len(table_sell) + 1)
    table_sell_total = trades_sell["sell_total"]
    table_sell_total["value"] /= 1e6
    table_sell_total["total_cost"] /= 1e6
    table_sell_total["profitloss"] /= 1e6
    fig_sell = px.sunburst(
        pd.DataFrame(table_sell), path=[px.Constant('فروش'), 'sector_name', 'symbol', 'basket'], values='value')
    fig_sell.update_traces(textinfo='label+value', texttemplate='%{label}<br>%{value:,.0f}')
    fig_sell.update_layout(font=dict(family="B Nazanin", size=20), margin=dict(l=0, r=0, t=0, b=0))

    budget = pd.DataFrame([{"date": "budget", "dividend": 8_146.97, "profitloss": 11_230.565}])
    bazdehi_table = weekly_report_data.get_bazdehi_table(start_date="1403/10/30", end_date=date_, include_avalhami=True)
    dates = bazdehi_table["date"].values.tolist()
    dates = sorted(dates, reverse=True)
    bazdehi_table["dividend"] /= 1e9
    bazdehi_table["profitloss"] /= 1e9
    bazdehi_table["value"] /= 1e9
    bazdehi_table["total_cost"] /= 1e9

    bazdehi_table_total = pd.DataFrame([{
        "date": "cumulative",
        "dividend": bazdehi_table["dividend"].sum(),
        "profitloss": bazdehi_table["profitloss"].sum(),
        "return_portfolio": (bazdehi_table["return_portfolio"].iloc[1:] + 1).prod() - 1,
        "return_total_index": (bazdehi_table["return_total_index"].iloc[1:] + 1).prod() - 1,
        "return_price_index_eq": (bazdehi_table["return_price_index_eq"].iloc[1:] + 1).prod() - 1,
        "return_investment_index": (bazdehi_table["return_investment_index"].iloc[1:] + 1).prod() - 1}])
    bazdehi_table_achievement_percent = pd.DataFrame([{
        "date": "achievement_percent",
        "dividend": bazdehi_table_total["dividend"].iloc[0] / budget["dividend"].iloc[0],
        "profitloss": bazdehi_table_total["profitloss"].iloc[0] / budget["profitloss"].iloc[0]}])
    bazdehi_table = pd.concat([budget, bazdehi_table, bazdehi_table_total, bazdehi_table_achievement_percent], axis=0, ignore_index=True)


    bazdehi_table = bazdehi_table[['date', 'value', 'total_cost', 'dividend', 'profitloss', 'return_portfolio',
                                   'return_portfolio_3', 'total_index', 'return_total_index', 'return_total_index_3',
                                   'price_index_eq', 'return_price_index_eq', 'return_price_index_eq_3',
                                   'investment_index', 'return_investment_index', 'return_investment_index_3']]

    bazdehi_table.set_index(keys=["date"], drop=True, inplace=True)
    bazdehi_table = bazdehi_table.T.reset_index(drop=False, names=["item"])
    bazdehi_items_mapper = {
        'value': 'ارزش روز', 'total_cost': 'بهای تمام شده', 'dividend': 'سود دریافت شده', 'profitloss': 'سود فروش',
        'return_portfolio': 'بازدهی ماهانه', 'return_portfolio_3': 'بازدهی سه ماه', 'total_index': 'شاخص کل',
        'return_total_index': 'بازدهی ماهانه', 'return_total_index_3': 'بازدهی سه ماه',
        'price_index_eq': 'شاخص قیمت هم وزن', 'return_price_index_eq': 'بازدهی ماهانه',
        'return_price_index_eq_3': 'بازدهی سه ماه', 'investment_index': 'شاخص صنعت',
        'return_investment_index': 'بازدهی ماهانه', 'return_investment_index_3': 'بازدهی سه ماه'}
    bazdehi_table["item"].replace(bazdehi_items_mapper, inplace=True, regex=False)

    bazdehi_table_data = bazdehi_table.to_dict('records')
    bazdehi_columns_dates = []
    for d in dates:
        date_col = {"headerName": d, "field": d, "cellDataType": "number", **bazdehi_columns_data}
        bazdehi_columns_dates.append(date_col)
    bazdehi_columns = bazdehi_columns_total + bazdehi_columns_dates + bazdehi_columns_items


    return [

        title_,

        table1_data,
        {"animateRows": True, "pinnedBottomRowData": portfolio_total.to_dict('records')},
        portfolio_columns,

        trades_title_buy,
        table_buy.to_dict('records'),
        {"animateRows": True, "pinnedBottomRowData": trades_buy["buy_total"].to_dict('records')},
        buy_columns,
        fig_buy,

        trades_title_sell,
        table_sell.to_dict('records'),
        {"animateRows": True, "pinnedBottomRowData": trades_sell["sell_total"].to_dict('records')},
        sell_columns,
        fig_sell,

        bazdehi_table_data,
        bazdehi_columns,
    ]

if __name__ == "__main__":
    # app.run_server(debug=False, host="172.20.30.34", port="6882")
    # app.run_server(debug=False, host="127.0.0.1", port="8888")
    app.run_server(debug=True, host="127.0.0.1", port="7777")
