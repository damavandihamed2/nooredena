import warnings
import pandas as pd

import dash_mantine_components as dmc
from dash import html, dcc, Input, Output, Dash, _dash_renderer

from fda.crawler.fda import fda_url
from fda.dashboard.funcs import get_cumulative_date_prices, get_link_date_prices
from fda.dashboard.layout import (fda_dashboard_layout_total, fda_dashboard_layout_total_title,
                                  fda_dashboard_layout_by_symbol_all)



warnings.filterwarnings("ignore")
_dash_renderer._set_react_version("18.2.0")


##################################### Medicines Dashboard #####################################

df = pd.DataFrame()
app = Dash(
    __name__,
    assets_folder="../assets"
    )
app.title = "آمارنامه دارویی شرکت ها"
app.layout = html.Div(
    children=[

        dcc.Store(id='startup-trigger', data=True),
        #################### Title ####################
        fda_dashboard_layout_total_title,

        #################### Tabs ####################
        dmc.MantineProvider(
            dmc.Tabs(
                [
                    dmc.TabsList(children=[dmc.TabsTab("گزارش تفکیکی", value="by_symbol"),
                                           dmc.TabsTab("تجمیعی شرکت ها", value="total")], justify="flex-end"),


                    #################### By Symbol ####################
                    dmc.TabsPanel(
                        id="tab-2",
                        value="by_symbol",

                        children=[
                            fda_dashboard_layout_by_symbol_all
                        ]

                    ),

                    #################### Total ####################
                    dmc.TabsPanel(
                        id="tab-1",
                        value="total",
                        children=[
                            fda_dashboard_layout_total
                        ]
                    ),
                ],
                color="blue",
                orientation="horizontal",
                value="total",
            ),
            theme={"components": {"Tabs": {"styles": {"root": {"margin-left": "10px"},
                                                      "tabLabel": {'fontFamily': 'B Nazanin', "fontSize": "20px"}}}}}
        ),

    ],
    style={"width": "98%", "margin-left": 15, "margin-right": 10}
)


@app.callback(
    [
        Output(component_id="symbol_table", component_property="rowData"),
        Output(component_id="symbol_table", component_property="columnDefs")
    ],
    Input('startup-trigger', 'data')
)
def selected_symbol(symbol_):

    data_raw = get_cumulative_date_prices(get_last_month_full=False)

    table_data = data_raw["weighted_return"]
    table_data.sort_values(by=table_data.columns[-1], inplace=True, ascending=False)

    columns_ = table_data.columns

    value_formatter_unified = {"function": "params.value == null ? '' : "
                                           "(params.column.colId === 'symbol' ? params.value : "
                                           "params.column.colId === 'weight' ? d3.format('.2%')(+params.value) : "
                                           "(params.node.rowPinned === 'bottom' ? d3.format('0.2%')(+params.value) : "
                                           "d3.format('0.1%')(+params.value) ))"}
    cell_style_default_prices = {"textAlign": 'center', "font-size": "18px"}
    cell_style_default_main = {**cell_style_default_prices, "font-weight": "bold"}
    green_cell_style = {**cell_style_default_main, "background-color": "rgba(0, 225, 0, 0.2)"}
    red_cell_style = {**cell_style_default_main, "background-color": "rgba(225, 0, 0, 0.2)"}
    fix_conditions = [{"condition": "params.column.colId === 'symbol'", "style": cell_style_default_main}]

    symbol_table_cols = []
    for c in range(len(columns_)):
        field = columns_[c]
        name = field.replace("symbol", "نماد")
        dynamic_conditions = [
            {"condition": f"params.column.colId !== '{columns_[0]}' && "
                          f"params.column.colId !== '{columns_[1]}' && "
                          f"params.data['{columns_[c]}'] > 0 && "
                          f"params.node.rowPinned != 'bottom'",
             "style": green_cell_style},
            {"condition": f"params.column.colId !== '{columns_[0]}' && "
                          f"params.column.colId !== '{columns_[1]}' && "
                          f"params.data['{columns_[c]}'] < 0 && "
                          f"params.node.rowPinned != 'bottom'",
             "style": red_cell_style}
        ]
        cell_style_unified = {
            "defaultStyle": cell_style_default_prices, "styleConditions": fix_conditions + dynamic_conditions}
        dict_tmp = {"headerName": name, "field": field,
                    "valueFormatter": value_formatter_unified, "cellStyle": cell_style_unified}
        if field == "symbol": dict_tmp["width"] = "250%"
        symbol_table_cols.append(dict_tmp)

    ####################

    return [

        table_data.to_dict("records"),
        symbol_table_cols,
    ]


@app.callback(
    [Output(component_id="title-component-symbol", component_property="children"),
     Output(component_id="selected_symbol_chart", component_property="data"),
     Output(component_id="selected_symbol_chart", component_property="yAxisProps"),

     Output(component_id="selected_med_name_persian", component_property="children"),
     Output(component_id="selected_med_name_persian", component_property="href"),
     Output(component_id="selected_med_general_name", component_property="children"),
     Output(component_id="selected_med_weight", component_property="children"),
     Output(component_id="selected_med_dosage_form", component_property="children"),
     Output(component_id="selected_med_roa", component_property="children"),
     Output(component_id="selected_med_atc1", component_property="children"),

     Output(component_id="symbol_med_table", component_property="rowData"),
     Output(component_id="symbol_med_table", component_property="columnDefs"),
     Output(component_id="symbol_med_table", component_property="dashGridOptions")],

    [Input(component_id="symbol_dropdown", component_property="value")]
)
def selected_symbol(symbol_):

    title = f"آمارنامه دارویی شرکت {symbol_}"

    data_raw = get_link_date_prices(symbol_, get_last_month_full=False)
    global df
    df = data_raw["data"].copy()

    weighted_avg = data_raw["weighted_avg"]
    table_data = data_raw["table_data"]
    sum_data = data_raw["sum_data"][0]
    sum_data_cum = data_raw["sum_data_cum"][0]
    columns_ = table_data.columns
    y_axis_props = {"domain": [round(weighted_avg["price"].min()), round(weighted_avg["price"].max())],
                    "tickMargin": 55, "padding": {"top": 10, "bottom": 10}, "orientation": "right"}
    value_formatter_unified = {"function": "params.value == null ? '' : (params.column.colId === 'name_persian' "
                                           "? params.value :  params.column.colId === 'weight' "
                                           "? d3.format('.2%')(+params.value) : (params.node.rowPinned === 'bottom' "
                                           "? d3.format('0.2%')(+params.value) : d3.format(',')(+params.value)) )"}
    cell_style_default_prices = {"textAlign": 'center', "font-size": "18px"}
    cell_style_default_main = {**cell_style_default_prices, "font-weight": "bold"}
    green_cell_style = {**cell_style_default_main, "background-color": "rgba(0, 225, 0, 0.2)"}
    red_cell_style = {**cell_style_default_main, "background-color": "rgba(225, 0, 0, 0.2)"}
    pinned_row_style = {**cell_style_default_main, "color": "rgba(0, 35, 155, 1)", "font-size": "20px"}
    fix_conditions = [{"condition": "params.node.rowPinned === 'bottom'", "style": pinned_row_style},
                      {"condition": "params.column.colId === 'name_persian'", "style": cell_style_default_main},
                      {"condition": "params.column.colId === 'weight'", "style": cell_style_default_main}]
    symbol_table_cols = []
    for c in range(len(columns_)):
        field = columns_[c]
        name = field.replace("weight", "سهم از فروش").replace("name_persian", "نام")
        dynamic_conditions = [
            {"condition": f"params.column.colId !== '{columns_[0]}' && params.column.colId !== "
                          f"'{columns_[1]}' && params.column.colId !== '{columns_[2]}' && "
                          f"params.data['{columns_[c]}'] > params.data['{columns_[c - 1]}'] && "
                          f"params.node.rowPinned != 'bottom'",
             "style": green_cell_style},
            {"condition": f"params.column.colId !== '{columns_[0]}' && params.column.colId !== "
                          f"'{columns_[1]}' && params.column.colId !== '{columns_[2]}' && "
                          f"params.data['{columns_[c]}'] < params.data['{columns_[c - 1]}'] && "
                          f"params.node.rowPinned != 'bottom'",
             "style": red_cell_style}
        ]
        cell_style_unified = {
            "defaultStyle": cell_style_default_prices, "styleConditions": fix_conditions + dynamic_conditions}
        dict_tmp = {"headerName": name, "field": field,
                    "valueFormatter": value_formatter_unified, "cellStyle": cell_style_unified}
        if field == "weight": dict_tmp["width"] = "250%"
        if field == "name_persian": dict_tmp["width"] = "350%"
        symbol_table_cols.append(dict_tmp)

    ####################

    return [
        title,
        weighted_avg.to_dict("records"),
        y_axis_props,
        df["name_persian"].iloc[0],
        f"{fda_url}/NFI/Detail/{df["medicine_id"].iloc[0]}",
        df["general_name"].iloc[0],
        str(round(df["weight"].iloc[0] * 100, ndigits=2)) + " %",
        df["dosage_form"].iloc[0],
        df["roa"].iloc[0],
        df["ATC_1"].iloc[0],
        table_data.to_dict("records"),
        symbol_table_cols,
        {"animateRows": True, "pinnedBottomRowData": [sum_data, sum_data_cum]},
    ]

@app.callback(
    [Output(component_id="selected_med_name_persian", component_property="children", allow_duplicate=True),
     Output(component_id="selected_med_name_persian", component_property="href", allow_duplicate=True),
     Output(component_id="selected_med_general_name", component_property="children", allow_duplicate=True),
     Output(component_id="selected_med_weight", component_property="children", allow_duplicate=True),
     Output(component_id="selected_med_dosage_form", component_property="children", allow_duplicate=True),
     Output(component_id="selected_med_roa", component_property="children", allow_duplicate=True),
     Output(component_id="selected_med_atc1", component_property="children", allow_duplicate=True)],
    [Input(component_id="symbol_med_table", component_property="cellClicked")],
    prevent_initial_call=True
)
def selected_cell(cell):
    global df
    if cell: idx_ = cell["rowIndex"]
    else: idx_ = 0
    return [
        df["name_persian"].iloc[idx_],
        f"{fda_url}/NFI/Detail/{df['medicine_id'].iloc[idx_]}",
        df["general_name"].iloc[idx_],
        str(round(df["weight"].iloc[idx_] * 100, ndigits=2)) + " %",
        df["dosage_form"].iloc[idx_],
        df["roa"].iloc[idx_],
        df["ATC_1"].iloc[idx_],
    ]


if __name__ == '__main__':
    app.run_server(host="172.20.30.34", port="6880")
