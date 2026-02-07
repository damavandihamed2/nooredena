import warnings
import jdatetime
import pandas as pd
import requests as rq
from tqdm import tqdm

import plotly.express as px
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots
from dash import Dash, Input, Output, callback, html, dcc


warnings.filterwarnings("ignore")
sector_ = "27"
header_ = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}
res = rq.get(f"https://cdn.tsetmc.com/api/ClosingPrice/GetRelatedCompany/{sector_}", headers=header_)
res = pd.DataFrame(res.json()["relatedCompany"])
res = pd.DataFrame(res["instrument"].tolist())
res = res[["insCode", "lVal18AFC", "lVal30"]]

df = pd.DataFrame()
for i in tqdm(range(len(res))):
    res_ = rq.get(f"http://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{res['insCode'].iloc[i]}", headers=header_)
    res_ = pd.DataFrame([res_.json()["instrumentInfo"]])
    df = pd.concat([df, res_], axis=0, ignore_index=True)

sec = pd.DataFrame(df["sector"].tolist())
sec = sec[["cSecVal", "lSecVal"]]
df = df[["insCode", "lVal18AFC", "lVal30", "zTitad", "yVal", "flow", "flowTitle"]]
df = pd.concat([df, sec], axis=1)
mper = {"insCode": "symbol_id", "lVal30": "symbol_name", "lVal18AFC": "symbol", "zTitad": "total_share", "yVal": "yval",
        "flowTitle": "flow_name", "cSecVal": "sector", "lSecVal": "sector_name"}
df.rename(mapper=mper, axis=1, inplace=True)
df = df[df["yval"].isin(["300", "303", "305", "309", "313"])]
df.reset_index(drop=True, inplace=True)

historical_data = pd.DataFrame()
for s in tqdm(range(len(df))):
    idx = df['symbol_id'].iloc[s]
    r = rq.get(f"https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{idx}/0",
               headers=header_)
    r = pd.DataFrame(r.json()["closingPriceDaily"])
    r.sort_values(by="dEven", ascending=True, ignore_index=True, inplace=True)

    # r = r.iloc[r.index[r["zTotTran"] > 0][0]:]
    # r.reset_index(drop=True, inplace=True)

    r_ = rq.get(f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentShareChange/{idx}", headers=header_)
    r_ = pd.DataFrame(r_.json()["instrumentShareChange"])
    if len(r_) > 0:
        total_share = pd.DataFrame(data={"total_share": [r_["numberOfShareNew"].iloc[0]] * len(r),
                                         "dEven": r["dEven"]})
        for i in range(len(r_)):
            temp1 = total_share[total_share["dEven"] >= r_["dEven"].iloc[i]]
            temp2 = total_share[total_share["dEven"] < r_["dEven"].iloc[i]]
            temp2["total_share"] = [r_["numberOfShareOld"].iloc[i]] * len(temp2)
            total_share = pd.concat([temp1, temp2], axis=0, ignore_index=True)
        r = r.merge(total_share, on="dEven", how="left")
    else:
        r["total_share"] = [df["total_share"].iloc[s]] * len(r)
    r = r[["insCode", "dEven", "pClosing", "zTotTran", "qTotTran5J", "qTotCap", "total_share"]]
    historical_data = pd.concat([historical_data, r], axis=0, ignore_index=True)

historical_data.rename(mapper={"insCode": "symbol_id", "pClosing": "final_price", "zTotTran": "trade_amount",
                               "qTotTran5J": "trade_volume", "qTotCap": "trade_value", "dEven": "date"},
                       axis=1, inplace=True)
historical_data = historical_data.merge(df.drop(labels=["total_share"], axis=1), on="symbol_id", how="left")
historical_data["market_value"] = historical_data["total_share"] * historical_data["final_price"]

symbols = historical_data.groupby(by=["date", "symbol"], as_index=False).sum()[["date", "symbol", "trade_value", "market_value"]]
symbols.sort_values(by=["symbol", "date"], ascending=False, inplace=True, ignore_index=True)
symbols.rename(mapper={"trade_value": "trade_value_symbol", "market_value": "market_value_symbol"}, axis=1, inplace=True)

sector = historical_data.groupby(by=["sector_name", "date"], as_index=False).sum()[["sector_name", "date",
                                                                                    "trade_value", "market_value"]]
sector.rename(mapper={"trade_value": "trade_value_sector", "market_value": "market_value_sector"}, axis=1, inplace=True)

sector_symbol = symbols.merge(sector, on="date", how="left")

sector_symbol["trade_value_ratio"] = 100 * (sector_symbol["trade_value_symbol"] / sector_symbol["trade_value_sector"])
sector_symbol["market_value_ratio"] = 100 * (sector_symbol["market_value_symbol"] / sector_symbol["market_value_sector"])

###########################################################################
###########################################################################

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "صندوق ها"

title = html.H1(children='گزارش صندوق ها', style={'textAlign': 'center', 'font-family': 'B Nazanin'})
drp1 = dcc.Dropdown(sector_symbol["sector_name"].unique().tolist(), "فلزات اساسي", style={'textAlign': 'center', 'font-family': 'B Nazanin', "width": "100%"}, id="drpdwn1")
drp2 = dcc.Dropdown(sector_symbol["symbol"].unique().tolist(), "فولاد", style={'textAlign': 'center', 'font-family': 'B Nazanin', "width": "100%"}, id="drpdwn2")
graph = dcc.Graph(id='graph', style={'width': '99vw', 'height': '85vh'}, config={'displaylogo': False, "displayModeBar": False})

app.layout = html.Div([
    # dbc.Row([dbc.Col()]),
    title,
    dbc.Row([dbc.Col(drp1),
             dbc.Col(drp2)]),
    # dbc.Alert(id='my-alert', style={'font-family': 'B Nazanin', 'textAlign': 'left'}),
    dbc.Row([dbc.Col(graph)])
])

# @callback(Output(component_id='drpdwn2', component_property='options'),
#           Input(component_id='drpdwn1', component_property='value'))
# def fig_data(value):
#     temp = dataframe[dataframe["fund"] == value]
#     return temp["date"].unique().tolist()

@callback(Output(component_id='graph', component_property='figure'),
          [Input(component_id='drpdwn1', component_property='value'),
           Input(component_id='drpdwn2', component_property='value')])
def fig_data(sect, sym):

    # sect = "فلزات اساسي"
    # sym = "فولاد"

    dataframe_ = sector_symbol[(sector_symbol["sector_name"] == sect) & (sector_symbol["symbol"] == sym)]
    dataframe_.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
    dataframe_["date"] = [jdatetime.datetime.fromgregorian(year=int(dataframe_["date"].iloc[i] // 10000),
                                                           month=int((dataframe_["date"].iloc[i] % 10000) // 100),
                                                           day=int(dataframe_["date"].iloc[i] % 100)).strftime(
        format="%Y/%m/%d") for i in range(len(dataframe_))]
    dataframe_["date_30days"] = [(jdatetime.datetime.strptime(dataframe_["date"].iloc[i], format="%Y/%m/%d") -
                                  jdatetime.timedelta(days=30)).strftime(format="%Y/%m/%d")
                                 for i in range(len(dataframe_))]
    dataframe_["date_90days"] = [(jdatetime.datetime.strptime(dataframe_["date"].iloc[i], format="%Y/%m/%d") -
                                  jdatetime.timedelta(days=90)).strftime(format="%Y/%m/%d")
                                 for i in range(len(dataframe_))]

    dataframe_["trade_value_symbol_ma30"] = [(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_30days"].iloc[i])]["trade_value_symbol"].sum())/(max(len(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_30days"].iloc[i]) & (dataframe_["trade_value_symbol"] > 0)]), 1)) for i in range(len(dataframe_))]
    dataframe_["trade_value_sector_ma30"] = [(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_30days"].iloc[i])]["trade_value_sector"].sum()) / (max(len(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_30days"].iloc[i]) & (dataframe_["trade_value_sector"] > 0)]), 1)) for i in range(len(dataframe_))]
    dataframe_["trade_value_ratio_ma30"] = 100 * (dataframe_["trade_value_symbol_ma30"] / dataframe_["trade_value_sector_ma30"])

    dataframe_["trade_value_symbol_ma90"] = [(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_90days"].iloc[i])]["trade_value_symbol"].sum())/(max(len(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_90days"].iloc[i]) & (dataframe_["trade_value_symbol"] > 0)]), 1)) for i in range(len(dataframe_))]
    dataframe_["trade_value_sector_ma90"] = [(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_90days"].iloc[i])]["trade_value_sector"].sum()) / (max(len(dataframe_[(dataframe_["date"] <= dataframe_["date"].iloc[i]) & (dataframe_["date"] > dataframe_["date_90days"].iloc[i]) & (dataframe_["trade_value_sector"] > 0)]), 1)) for i in range(len(dataframe_))]
    dataframe_["trade_value_ratio_ma90"] = 100 * (dataframe_["trade_value_symbol_ma90"] / dataframe_["trade_value_sector_ma90"])

    dataframe_ = dataframe_[dataframe_["date"] >= "1400/01/01"]

    # fg = make_subplots(
    #     rows=1,
    #     cols=2,
    #     print_grid=False,
    #     column_widths=[1, 1],
    #     specs=[[{"type": "domain"}, {"type": "domain"}]],
    #     subplot_titles=["<b>فروش ها<b>", "<b>خرید ها<b>"],
    # )

    fg = px.line(data_frame=dataframe_, x="date", y=["market_value_ratio", "trade_value_ratio", "trade_value_ratio_ma30", "trade_value_ratio_ma90"], line_group="symbol")
    fg.update_layout(template="seaborn",
                          font_family="B Nazanin",
                          font_size=18,
                          # showlegend=False,
                          margin=dict(l=80, r=80, t=50, b=50))


    # fig_buy = px.pie(data_frame=df_buy, names="sector", values="value", )
    # fig_sell = px.pie(data_frame=df_sell, names="sector", values="value")
    #
    # fg.add_trace(fig_sell.data[0], row=1, col=1)
    # fg.add_trace(fig_buy.data[0], row=1, col=2)
    # fg.update_layout(template="seaborn",
    #                  font_family="B Nazanin",
    #                  font_size=20,
    #                  showlegend=False,
    #                  margin=dict(l=50, r=50, t=50, b=50))

    return fg

if __name__ == '__main__':
    app.run_server(host="192.168.3.33", port="1111")
