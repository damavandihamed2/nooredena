import pandas as pd
import warnings, jdatetime

import dash_bootstrap_components as dbc
from dash.dash_table.Format import Format
from dash import Dash, dash_table, Input, Output, callback, html, dcc

from utils.database import make_connection
from fda.dashboard.funcs import get_company_names_list, get_company_medicines, get_company_price_history


warnings.filterwarnings("ignore")
db_conn = make_connection()

###########################################
###########################################

app = Dash(
    __name__,
    assets_folder="../assets"
    )
app.title = "جدول  قیمت دارو ها"
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.H1(children="جدول  قیمت دارو ها", style={'textAlign': 'center', 'font-family': 'B Nazanin'}),
    dbc.Alert(id='my-alert', style={'font-family': 'B Nazanin', 'textAlign': 'left'}),
    dcc.Dropdown(
        style={'textAlign': 'center', 'font-family': 'B Nazanin', "width": "100%"},
        id="my-dropdown",
    ),
    dash_table.DataTable(
        page_size=50,
        fixed_rows={'headers': True},
        fixed_columns={'headers': True, 'data': 2},
        merge_duplicate_headers=False,
        style_cell={'textAlign': 'center', 'font-family': 'B Nazanin'},
        style_cell_conditional=[{'if': {'column_id': 'name'}, 'minWidth': '180px', 'width': '180px',
                                 'maxWidth': '180px', 'overflow': 'hidden', 'textOverflow': 'ellipsis'}],
        style_table={'height': '650px', "width": "1850px", 'maxHeight': '650px', "maxWidth": "1850px",
                     'overflowY': 'auto', 'overflowX': 'auto'},
        id='my-table')
])


@callback(
    [
        Output(component_id='my-dropdown', component_property='options'),
        Output(component_id='my-dropdown', component_property='value'),
    ],
    Input('url', 'pathname'),
)
def fig_data(pathname):
    companies = get_company_names_list()
    return [
        companies,
        companies[0]
    ]



@callback(
    [
        Output(component_id='my-table', component_property='data'),
        Output(component_id='my-table', component_property='columns'),
        Output(component_id='my-table', component_property='style_data_conditional')
    ],
    Input(component_id='my-dropdown', component_property='value'),
    prevent_initial_call=True
)
def fig_data(value):
    medicines = get_company_medicines(value)
    history = get_company_price_history(value)

    dates = pd.DataFrame({"date": [c for c in history.columns if c != "medicine_id"]})
    dates[["year-month", "day"]] = dates["date"].str.extract(r"(\d{4}/\d{2})/(\d{2})")
    dates_ = dates.groupby(by=["year-month"], as_index=False).max()
    current_month = jdatetime.datetime.today().strftime("%Y/%m")
    dates = pd.concat(
        [dates_[dates_["year-month"] != current_month], dates[dates["year-month"] == current_month]],
        axis=0, ignore_index=True)[["date"]]

    dates = sorted(dates["date"].values.tolist())
    conditional_format = []
    for d in range(1, len(dates)):
        conditional_format.append({"if": {"filter_query": "{" + f"{dates[d]}" + "} > {" + f"{dates[d - 1]}" + "}",
                                          "column_id": f"{dates[d]}"}, "backgroundColor": "#5fc296"})

    names = ["name", "link"] + dates
    cols = [
        {'id': c, 'name': [c, c, c], "hideable": True, 'presentation': 'markdown'} if c == "link" else
        {'id': c, 'name': [c, c, c], "hide able": True} if c in ["name", "generic name"] else
        {'id': c, 'name': [c.split("/")[0], c.split("/")[1], c.split("/")[2]], "hideable": True, "type": "numeric",
         "format": Format(group=True, groups=[3])} for c in names
    ]

    global df
    df = pd.DataFrame()
    df = medicines.merge(history[["medicine_id"] + dates], on="medicine_id", how="left")
    df.sort_values("name", inplace=True, ignore_index=True)

    df["link"] = [f'[{df["link"].iloc[i].split("/")[-1]}]({df["link"].iloc[i]})' for i in range(len(df))]
    changes = pd.DataFrame(columns=df.columns, data=[[""] * len(df.columns)])
    for d in range(len(dates)):
        if dates[d] == min(dates):
            changes[dates[d]].iloc[0] = 0
        else:
            changes[dates[d]].iloc[0] = sum((df[dates[d]] - df[dates[d - 1]]) > 0)
    changes["name"].iloc[0] = "تغییرات"
    df = pd.concat([changes, df], axis=0, ignore_index=True)
    df["id"] = df.index

    return [
        df.drop(labels=["generic name", "medicine_id"], axis=1, inplace=False).to_dict("records"),
        cols,
        conditional_format
    ]


@callback(
    Output(component_id="my-alert", component_property="children"),
    Input(component_id="my-table", component_property="active_cell")
)
def update_graphs(active_cell):
    if active_cell:
        if "-" in active_cell["column_id"]:
            text1 = "name: " + str(df["name"].iloc[active_cell["row_id"]])
            text2 = "generic name: " + str(df["generic name"].iloc[active_cell["row_id"]])
            text3 = "Date: " + str(active_cell["column_id"])
        else:
            text1 = "name: " + str(df["name"].iloc[active_cell["row_id"]])
            text2 = "generic name: " + str(df["generic name"].iloc[active_cell["row_id"]])
            text3 = "-"
    else:
        text1 = "Click the table"
        text2 = "-"
        text3 = "-"

    return html.P([text1, html.Br(), text2, html.Br(), text3], style={"font-weight": "bold"})


if __name__ == '__main__':
    app.run_server(host="172.20.30.34", port="6884")
