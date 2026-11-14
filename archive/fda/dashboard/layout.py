import pandas as pd

import dash_ag_grid
from dash import html
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc

from utils.database import make_connection



select_style = {'fontFamily': 'B Nazanin', "fontSize": "16px", "fontWeight": "bold"}
card_style_eng = {"text-align": "left", "direction": "ltr", "font-family": "calibri",
                  "font-size": "18px", "font-weight": "bold"}
card_style_fa = {"text-align": "right", "direction": "rtl", "font-family": "B Nazanin",
                 "font-size": "18px", "font-weight": "bold"}


#################### Title Total ####################
fda_dashboard_layout_total_title = dbc.Row(
    children=[dbc.Col(children=[html.H2(children="آمارنامه دارویی شرکت ها", id="title-component",
                                        style={"margin": "auto", "height": "100%", "width": "100%",
                                               "align": "center", "direction": "rtl",
                                               'font-family': 'B Nazanin', "font-size": "25px"})],
                      style={"align": "center", "height": "100%", "marginTop": 0}, width=2)],
    justify="center", align="center", style={"marginTop": 10, "height": 40}
)

#################### Medicine Price Table (AG-Grid) ####################
fda_dashboard_layout_total = dbc.Row(
    children=[
        dbc.Col(
            children=[
                dash_ag_grid.AgGrid(
                    id="symbol_table",
                    columnDefs=[{}],
                    columnSize="responsiveSizeToFit",
                    columnSizeOptions={"skipHeader": False},
                    defaultColDef={"wrapHeaderText": True, "autoHeaderHeight": True,
                                   "editable": False,
                                   'cellRendererSelector': {
                                       "function": "rowPinningBottom(params)"}},
                    getRowStyle={"styleConditions": [
                        {
                            "condition": "(params.rowIndex % 2 == 0) && (params.node.rowPinned != 'bottom')",
                            "style": {"backgroundColor": "rgb(20, 160, 180, 0.1)",
                                      "color": "black"}}]},
                    style={"height": "100%", "width": "100%", "font-family": "B Nazanin"}
                )
            ],
            style={"align": "center", "height": "100%"},
            width=12
        )
    ],
    justify="center",
    align="center",
    style={"marginTop": 10, "height": 750}
)


#################### Title By Symbol ####################
fda_dashboard_layout_by_symbol_title = dbc.Row(
    children=[dbc.Col(children=[html.H2(children="", id="title-component-symbol",
                                        style={"margin": "auto", "height": "100%", "width": "100%",
                                               "align": "center", "direction": "rtl",
                                               'font-family': 'B Nazanin', "font-size": "20px"})],
                      style={"align": "center", "height": "100%", "marginTop": 0}, width=2)],
    justify="center", align="center", style={"marginTop": 10, "height": 40}
)

#################### Company Selection Dropdown ####################
fda_dashboard_layout_by_symbol_dropdown = dbc.Row(
    children=[
        dbc.Col(
            children=[
                dmc.MantineProvider(
                    dmc.Select(

                        placeholder="نماد را انتخاب کنید",
                        id="symbol_dropdown",
                        searchable=True,
                        value="دالبر",
                        w=100,
                        data=[{'label': symbol.replace("ك", "ک").replace("ي", "ی"), 'value': symbol} for
                              symbol in pd.read_sql(
                                "SELECT DISTINCT(symbol) FROM [nooredenadb].[fda].[medicines_weight]",
                                make_connection())["symbol"].values.tolist()]

                    ),
                    theme={"components": {"Select": {
                        "styles": {"root": {"margin-bottom": "0px", "direction": "rtl"},
                                   "label": select_style, "option": select_style, "input": select_style}
                    }}}
                )
            ],
            style={"height": "100%"},
            width=1,
        ),
        dbc.Col(
            children=["نماد:"], width=1,
            style={"height": "100%", "width": "50px", "font-family": "B Nazanin", "font-size": "20px",
                   "font-weight": "bold", "direction": "rtl"})
    ],
    justify="end",
    style={"height": 40}
)

#################### Medicine Price Table (AG-Grid) ####################
fda_dashboard_layout_by_symbol_ag_grid = dbc.Row(
    children=[
        dbc.Col(
            children=[
                dash_ag_grid.AgGrid(
                    id="symbol_med_table",
                    columnDefs=[{}],
                    columnSize="responsiveSizeToFit",
                    columnSizeOptions={"skipHeader": False},
                    defaultColDef={"wrapHeaderText": True, "autoHeaderHeight": True, "editable": False,
                                   'cellRendererSelector': {"function": "rowPinningBottom(params)"}},
                    getRowStyle={"styleConditions": [
                        {"condition": "(params.rowIndex % 2 == 0) && (params.node.rowPinned != 'bottom')",
                         "style": {"backgroundColor": "rgb(20, 160, 180, 0.1)", "color": "black"}}]},
                    style={"height": "100%", "width": "100%", "font-family": "B Nazanin"}
                )
            ],
            style={"align": "center", "height": "100%"},
            width=12
        )
    ],
    justify="center",
    align="center",
    style={"marginTop": 10, "height": 450}
)

#################### Price Line Chart ####################
fda_dashboard_layout_by_symbol_line_chart = dbc.Col(
    children=[
        dmc.MantineProvider(
            dmc.Card(
                children=[
                    html.H1("نمودار میانگین موزون قیمتی",
                            style={"fontFamily": "B Nazanin", "fontSize": "15px",
                                   "text-align": "center", "margin": "0px"}),
                    dmc.LineChart(id="selected_symbol_chart",
                                  h="95%", w="99%", data=[{}],
                                  dataKey="date", curveType="natural", withTooltip=False,
                                  series=[{"name": "price", "color": "indigo.6"}], tickLine="xy",
                                  gridAxis="x", withXAxis=True, withYAxis=True, withDots=False,
                                  xAxisProps={"tickMargin": -5, "padding": {"left": 10, "right": 10}}),
                ],
                withBorder=True
            ),
            theme={
                "components": {
                    "Card": {"styles": {
                        "root": {"margin": "0px", "height": "100%", "width": "100%"}}},
                    "LineChart": {"styles": {
                        "root": {"margin-bottom": "0px"},
                        "axis": {'fontFamily': 'B Nazanin', "fontSize": "14px", "direction": "rtl"}}}
                }
            }
        ),
    ],
    style={"align": "center", "height": "100%"},
    width=6
)

#################### Medicine Details ####################
fda_dashboard_layout_by_symbol_medicine_detail = dbc.Col(
    children=[
        dmc.MantineProvider(
            dmc.Card(
                children=[
                    dmc.Text(dmc.Anchor(id="selected_med_name_persian", href="", target="_blank",
                                        style={"font-family": "B Nazanin", "font-size": "20px",
                                               "font-weight": "bold"}),
                             style={"text-align": "center", "direction": "rtl"}),
                    dmc.Divider(variant="solid", size=3),
                    dbc.Row(
                        [
                            dbc.Col(html.Div(id="selected_med_general_name"), width=10,
                                    style=card_style_eng),
                            dbc.Col(html.Div("نام عمومی"), width=2, style=card_style_fa),
                        ],
                    ),
                    dmc.Divider(variant="dashed"),
                    dbc.Row(
                        [
                            dbc.Col(html.Div(id="selected_med_weight"), width=10,
                                    style={**card_style_fa, "text-align": "left", "direction": "ltr"}),
                            dbc.Col(html.Div("سهم از فروش"), width=2, style=card_style_fa),
                        ],
                        justify="between"
                    ),
                    dmc.Divider(variant="dashed"),
                    dbc.Row(
                        [
                            dbc.Col(html.Div(id="selected_med_dosage_form"), width=10,
                                    style=card_style_eng),
                            dbc.Col(html.Div("شکل دارویی"), width=2, style=card_style_fa),
                        ],
                        justify="between",
                    ),
                    dmc.Divider(variant="dashed"),
                    dbc.Row(
                        [
                            dbc.Col(html.Div(id="selected_med_roa"), width=10, style=card_style_eng),
                            dbc.Col(html.Div("نحوه مصرف"), width=2, style=card_style_fa),
                        ],
                        justify="between",
                    ),
                    dmc.Divider(variant="dashed"),
                    dbc.Row(
                        [
                            dbc.Col(html.Div(id="selected_med_atc1"), width=10, style=card_style_eng),

                            dbc.Col(html.Div("ATC1"), width=2,
                                    style={**card_style_eng, "text-align": "right"})
                        ],
                        justify="between"
                    ),
                    dmc.Divider(variant="dashed"),
                    dbc.Row(
                        [
                            dbc.Col(html.Div(id="selected_med_irc"), width=10, style=card_style_eng),

                            dbc.Col(html.Div("IRC"), width=2,
                                    style={**card_style_eng, "text-align": "right"})
                        ],
                        justify="between"
                    )
                ],
                withBorder=True
            ),
            theme={"components": {"Card": {
                "styles": {"root": {"margin": "0px", "height": "100%", "width": "100%"}}}}}
        )
    ],
    style={"align": "center", "height": "100%"},
    width=6
)



#################### Medicine Details ####################
fda_dashboard_layout_by_symbol_all = html.Div(
    children=[
        ########## Title
        fda_dashboard_layout_by_symbol_title,
        ########## Symbol Dropdown
        fda_dashboard_layout_by_symbol_dropdown,
        ########## Medicine Details and Price Line Chart
        dbc.Row(
            children=[
                ########## Medicine Details
                fda_dashboard_layout_by_symbol_medicine_detail,
                ########## Price Line Chart
                fda_dashboard_layout_by_symbol_line_chart
            ],
            justify="center",
            align="center",
            style={"marginTop": 0, "height": 200}
        ),
        ########## Medicine Price Table (AG-Grid)
        fda_dashboard_layout_by_symbol_ag_grid
    ],
    style={
        "width": "98%",
        "margin-left": 15,
        "margin-right": 10
    }
)

