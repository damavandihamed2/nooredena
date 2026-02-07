import dash_ag_grid
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from dash import html, dcc



rows_style = {"condition": "(params.rowIndex % 2 == 0) && (params.node.rowPinned != 'bottom')",
                         "style": {"backgroundColor": "rgb(22, 127, 146, 0.125)", "color": "black"}}
bazdehi_table_rows_style = {"condition": "(params.data && params.data.item && params.data.item.includes('بازدهی'))",
                            "style": {"backgroundColor": "rgb(125, 196, 205, 0.65)", "color": "black"}}
pinned_row_style = {"condition": "(params.node.rowPinned == 'bottom')",
                    "style": {'color': 'rgb(0, 100, 255, 1)', 'fontWeight': 'bold', 'font-size': '16px'}}

value_formatter_unified = """
params.value == null ? '' : 
    (
        (params.column.colId === 'index' || params.column.colId === 'symbol' || params.column.colId === 'basket' || params.column.colId === 'item') ? params.value : 
        params.column.colId === 'ownership' ? d3.format('.2%')(+params.value) : 
        (params.column.colId === 'share_of_portfo' || params.column.colId === 'achievement_percent' || params.column.colId === 'profitloss_percent') ? d3.format('(.1%')(+params.value): 
        (params.column.colId === 'profitloss' || params.column.colId === 'budget') ? d3.format('(,.0f')(+params.value) : 
        ((/^\\d{4}\\/\\d{2}(\\/\\d{2})?$/.test(params.column.colId)) ||  params.column.colId === 'cumulative')
        ? (
            (params.data && params.data.item && params.data.item.includes('بازدهی')) 
            ? d3.format('(.1%')(+params.value) :
                d3.format(',.0f')(+params.value)
            ) : 
        d3.format(',.0f')(+params.value)
    )
"""

tab_portfolio = dmc.TabsPanel(
    id="tab-1",
    value="portfolio",
    children=html.Div(
        children=[
            dash_ag_grid.AgGrid(
                id="table-1",
                defaultColDef={'editable': False,
                               'cellRendererSelector': {"function": "rowPinningBottom(params)"}},
                getRowStyle={"styleConditions": [rows_style, pinned_row_style]},
                style={"height": 685, "width": "100%", 'font-family': 'B Nazanin'},
                persistence=True
            )
        ],
        style={
            "height": 685,
            "margin-right": 0,
            "margin-left": 0,
            "margin-top": 0,

        }
    ),
)
portfolio_columns = [
        {"headerName": 'درصد سود و زیان', "field": "profitloss_percent", "cellDataType": "number", "flex": 2.5,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {
             "defaultStyle": {'textAlign': 'center', "font-size": "18px"},
             "styleConditions": [{"condition": "params.value < 0",
                                  "style": {"color": "rgb(255, 0, 0)", 'textAlign': 'center', "font-size": "20px"}}]}},
        {"headerName": 'مازاد ارزش (میلیارد ریال)', "field": "profitloss", "cellDataType": "number", "flex": 3,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {
             "defaultStyle": {'textAlign': 'center', "font-size": "18px"},
             "styleConditions": [{"condition": "params.value < 0",
                                  "style": {"color": "rgb(255, 0, 0)", 'textAlign': 'center', "font-size": "20px"}}]}},
        {"headerName": 'سهم از پرتفوی', "field": "share_of_portfo", "cellDataType": "number", "flex": 2,
         "cellStyle": {'textAlign': 'center', "font-size": "18px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'ارزش (میلیارد ریال)', "field": "value", "cellDataType": "number", "flex": 3,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'قیمت پایانی', "field": "final_price", "cellDataType": "number", "flex": 2,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'بهای تمام شده (میلیارد ریال)', "field": "total_cost", "cellDataType": "number", "flex": 3,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'بهای هر سهم', "field": "cost_per_share", "cellDataType": "number", "flex": 2,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'تعداد', "field": "amount", "cellDataType": "number", "flex": 2.5,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {'textAlign': 'center', "font-size": "18px"}},
        {"headerName": 'سبد', "field": "basket", "flex": 1.5,
         "cellStyle": {'textAlign': 'center', "font-size": "18px", "font-weight": "bold"}},
        {"headerName": 'درصد مالکیت', "field": "ownership", "cellDataType": "number", "flex": 2,
         "cellStyle": {
             "defaultStyle": {'textAlign': 'center', "font-size": "18px"},
             "styleConditions": [
                 {"condition": "params.value > 0.0095",
                  "style": {"background-color": "rgba(255, 0, 0, 0.15)", 'textAlign': 'center', "font-size": "18px"}}]},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'نماد', "field": "symbol", "flex": 2,
         "cellStyle": {'textAlign': 'center', "font-size": "18px", "font-weight": "bold"}},
        {"headerName": 'ردیف', "field": "index", "flex": 1,
         "cellStyle": {'textAlign': 'center', "font-size": "16px"}}
    ]


tab_buy = dmc.TabsPanel(
    id="tab-buy",
    value="buy",
    children=html.Div(
        html.Div(
            children=[
                dbc.Row(children=[
                    dbc.Col(
                        html.H2(id="tab-buy-title", style={"height": 50, "textAlign": "center"}),
                        align="center",
                        width=12,
                        style={"font-size": "12px", "font-weight": "bold", 'font-family': 'B Nazanin', "height": 50}
                    ),
                ],
                    align="center", justify="around"),
                dbc.Row(children=[
                    dbc.Col(
                        children=[
                            dcc.Graph(
                                id="fig-buy",
                                config={'displaylogo': False, "displayModeBar": False},
                                style={"height": 550}
                            )
                        ],
                        width=4,
                        align="center"
                    ),
                    dbc.Col(
                        children=[
                            dash_ag_grid.AgGrid(
                                columnSize="responsiveSizeToFit",
                                columnSizeOptions={"skipHeader": False},
                                id="table-buy",
                                defaultColDef={'editable': False,
                                               'cellRendererSelector': {"function": "rowPinningBottom(params)"}},
                                getRowStyle={"styleConditions": [rows_style, pinned_row_style]},
                                style={"height": 550, 'font-family': 'B Nazanin'},
                                persistence=True
                            ),

                        ],
                        width=8,
                        align="center"
                    )
                ], align="center", justify="center"),
            ]
        ),
        style={
            "height": 600,
            "margin-right": 0,
            "margin-left": 0,
            "margin-top": 0,
        }
    ),
)

tab_sell = dmc.TabsPanel(
    id="tab-sell",
    value="sell",
    children=html.Div(
        html.Div(
            children=[
                dbc.Row(children=[
                    dbc.Col(
                        html.H2(id="tab-sell-title", style={"height": 50, "textAlign": "center"}),
                        align="center",
                        width=12,
                        style={"font-size": "12px", "font-weight": "bold", 'font-family': 'B Nazanin', "height": 50}
                    ),
                ],
                        align="center", justify="center"),
                dbc.Row(children=[
                    dbc.Col(
                        children=[
                            dcc.Graph(
                                id="fig-sell",
                                config={'displaylogo': False, "displayModeBar": False},
                                style={"height": 550, "padding": "0px"}
                            )
                        ],
                        width=4,
                        align="center"
                    ),
                    dbc.Col(
                        children=[
                            dash_ag_grid.AgGrid(
                                columnSize="responsiveSizeToFit",
                                columnSizeOptions={"skipHeader": False},
                                id="table-sell",
                                defaultColDef={'editable': False,
                                               'cellRendererSelector': {"function": "rowPinningBottom(params)"}},
                                getRowStyle={"styleConditions": [rows_style, pinned_row_style]},
                                style={
                                    "height": 550, 'font-family': 'B Nazanin'},
                                persistence=True
                            ),

                        ],
                        width=8,
                        align="center"
                    )
                ], align="center", justify="center"),
            ]
        ),
        style={
            "height": 600,
            "margin-right": 0,
            "margin-left": 0,
            "margin-top": 0,
        }
    ),
)

tab_trades = dmc.TabsPanel(
    id="tab-2",
    value="trades",
    children=html.Div(
        children=[
            dbc.Row(children=[]),
            dmc.Tabs(
                [dmc.TabsList(children=[dmc.TabsTab("فروش", value="sell"),
                                        dmc.TabsTab("خرید", value="buy")], justify="flex-end"),
                 tab_sell, tab_buy],
                color="blue",
                orientation="horizontal",
                value="buy",
            )
        ],
        style={
            "height": 600,
            "margin-right": 0,
            "margin-left": 0,
            "margin-top": 0,

        }
    ),
)

buy_columns = [
        {"headerName": 'ارزش (میلیون ریال)', "field": "value", "cellDataType": "number", "flex": 2,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'قیمت میانگین', "field": "mean_price", "cellDataType": "number", "flex": 2,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'تعداد', "field": "volume", "cellDataType": "number", "flex": 2,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {'textAlign': 'center', "font-size": "18px"}},
        {"headerName": 'سبد', "field": "basket", "flex": 1.5,
         "cellStyle": {'textAlign': 'center', "font-size": "18px", "font-weight": "bold"}},
        {"headerName": 'نماد', "field": "symbol", "flex": 1.5,
         "cellStyle": {'textAlign': 'center', "font-size": "18px", "font-weight": "bold"}},
        {"headerName": 'ردیف', "field": "index", "flex": 1,
         "cellStyle": {'textAlign': 'center', "font-size": "16px"}}
]

sell_columns = [
        {"headerName": 'درصد سود/زیان', "field": "profitloss_percent", "cellDataType": "number", "flex": 1.5,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {"defaultStyle": {'textAlign': 'center', "font-size": "18px"},
                       "styleConditions": [{
                           "condition": "params.value < 0",
                           "style": {"color": "rgb(255, 0, 0)", 'textAlign': 'center', "font-size": "20px"}}]}},
        {"headerName": 'سود/زیان (میلیون ریال)', "field": "profitloss", "cellDataType": "number", "flex": 2,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {"defaultStyle": {'textAlign': 'center', "font-size": "18px"},
                       "styleConditions": [{
                           "condition": "params.value < 0",
                           "style": {"color": "rgb(255, 0, 0)", 'textAlign': 'center', "font-size": "20px"}}]}},
        {"headerName": 'بهای تمام شده (میلیون ریال)', "field": "total_cost", "cellDataType": "number", "flex": 2.4,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'ارزش (میلیون ریال)', "field": "value", "cellDataType": "number", "flex": 1.75,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'قیمت میانگین', "field": "mean_price", "cellDataType": "number", "flex": 1.4,
         "cellStyle": {'textAlign': 'center', "font-size": "20px"},
         "valueFormatter": {"function": value_formatter_unified}},
        {"headerName": 'تعداد', "field": "volume", "cellDataType": "number", "flex": 1.5,
         "valueFormatter": {"function": value_formatter_unified},
         "cellStyle": {'textAlign': 'center', "font-size": "18px"}},
        {"headerName": 'سبد', "field": "basket", "flex": 1,
         "cellStyle": {'textAlign': 'center', "font-size": "18px", "font-weight": "bold"}},
        {"headerName": 'نماد', "field": "symbol", "flex": 1.5,
         "cellStyle": {'textAlign': 'center', "font-size": "18px", "font-weight": "bold"}},
        {"headerName": 'ردیف', "field": "index", "flex": 0.9,
         "cellStyle": {'textAlign': 'center', "font-size": "16px"}}
]

tab_bazdehi = dmc.TabsPanel(
    id="tab-3",
    value="return",
    children=html.Div(
        children=[
            dash_ag_grid.AgGrid(
                id="table-3",
                columnSize="responsiveSizeToFit",
                columnSizeOptions={"skipHeader": False},
                defaultColDef={'editable': False},
                getRowStyle={"styleConditions": [bazdehi_table_rows_style]},
                style={"height": 685, "width": "100%", 'font-family': 'B Nazanin'},
                persistence=True
            ),
        ],
        style={
            "height": 685,
            "margin-right": 0,
            "margin-left": 0,
            "margin-top": 0,

        }
    ),
)

bazdehi_columns_items = [
    {"headerName": 'بودجه', "field": "budget", "flex": 4,
     "cellStyle": {'textAlign': 'center', "font-size": "18px", "border": "0px solid black"},
     "valueFormatter": {"function": value_formatter_unified}},
    {"headerName": 'ارقام به میلیارد ریال', "field": "item", "flex": 7,
     "cellStyle": {'textAlign': 'center', "font-size": "18px", "border": "0px solid black"},
     "valueFormatter": {"function": value_formatter_unified}}
]

bazdehi_columns_total = [
    {"headerName": 'درصد تحقق', "field": "achievement_percent", "cellDataType": "number", "flex": 4,
     "cellStyle": {
         "defaultStyle": {'textAlign': 'center', "font-size": "18px", "border": "0px solid black"},
         "styleConditions": [{"condition": "params.value > 0.95",
                              "style": {"background-color": "rgba(0, 255, 0, 0.35)", 'textAlign': 'center',
                                        "font-size": "18px", "border": "0px solid black"}}]},
     "valueFormatter": {"function": value_formatter_unified}},
    {"headerName": 'تجمیعی', "field": "cumulative", "cellDataType": "number", "flex": 4,
     "cellStyle": {"defaultStyle": {'textAlign': 'center', "font-size": "18px", "border": "0px solid black"}},
     "valueFormatter": {"function": value_formatter_unified}},
    ]

bazdehi_columns_data = {
    "flex": 4,
    "valueFormatter": {"function": value_formatter_unified},
    "cellStyle": {
        "defaultStyle": {'textAlign': 'center', "font-size": "18px", "padding": "0px", "border": "0px solid black"},
        "styleConditions": [{"condition": "params.value < 0",
                             "style": {"color": "rgb(255, 0, 0)", 'textAlign': 'center',
                                       "font-size": "18px", "padding": "0px", "border": "0px solid black"}}]
    }
}
