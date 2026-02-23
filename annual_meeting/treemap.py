import warnings
import pandas as pd

import plotly.express as px
from plotly.subplots import make_subplots

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()


fnt_fa = "B Nazanin"
color1, color2 = "#56c4cd", "#f8a81d"
col_pos, col_neg, col_black, col_neu = "#018501", "#cc0202", "#000000", "#858585"

clr = {"7": "#00ff00", "6": "#00ff00", "5": "#00ff00", "4": "#00c800", "3": "#009600",
       "2": "#007d00", "1": "#006400", "0": "#e8e8e8", "-1": "#640000", "-2": "#7d0000",
       "-3": "#960000", "-4": "#c80000", "-5": "#ff0000", "-6": "#ff0000", "-7": "#ff0000", None: "#e8e8e8"}
clr_ = clr.copy()
clr_["0"] = "#636363"
clr_font = {"7": "#000000", "6": "#000000", "5": "#000000", "4": "#000000", "3": "#ffffff",
            "2": "#ffffff", "1": "#ffffff", "0": "#000000", "-1": "#ffffff", "-2": "#ffffff",
            "-3": "#ffffff", "-4": "#ffffff", "-5": "#ffffff", "-6": "#ffffff", "-7": "#ffffff", None: "#e8e8e8"}

####################################################################################################

portfolio_start = pd.read_sql("SELECT * FROM [nooredenadb].[company].[portfolio_history] WHERE date = '1403/10/30'", db_conn)
portfolio_start["value"] = portfolio_start["volume"] * portfolio_start["final_price"]
portfolio_start = portfolio_start[["symbol", "value"]].groupby(["symbol"], as_index=False).sum()
portfolio_start["share_percent"] = portfolio_start["value"] / portfolio_start["value"].sum()
portfolio_start.loc[portfolio_start["share_percent"] < 0.01, 'symbol'] = "سایر"
portfolio_start = portfolio_start[["symbol", "value", "share_percent"]].groupby(["symbol"], as_index=False).sum()

portfolio_end = pd.read_sql("SELECT * FROM [nooredenadb].[company].[portfolio_history] WHERE date = '1404/10/30'", db_conn)
portfolio_end["value"] = portfolio_end["volume"] * portfolio_end["final_price"]
portfolio_end = portfolio_end[["symbol", "value"]].groupby(["symbol"], as_index=False).sum()
portfolio_end["share_percent"] = portfolio_end["value"] / portfolio_end["value"].sum()
portfolio_end.loc[portfolio_end["share_percent"] < 0.01, 'symbol'] = "سایر"
portfolio_end = portfolio_end[["symbol", "value", "share_percent"]].groupby(["symbol"], as_index=False).sum()

####################################################################################################

treemap_start = px.treemap(portfolio_start, path=[px.Constant(""), "symbol"],
                           values=portfolio_start["value"].values, custom_data=[portfolio_start["share_percent"].values])
treemap_start.data[0]["texttemplate"] = " %{label} <br> %{customdata[0]:.1%}"
treemap_start.data[0]["textposition"] = "middle center"
treemap_start.data[0]["textfont"] = dict(family=fnt_fa, size=20, weight="bold")
treemap_start.update_traces(marker=dict(cornerradius=10))

####################################################################################################

treemap_end = px.treemap(portfolio_end, path=[px.Constant(""), "symbol"],
                         values=portfolio_end["value"].values, custom_data=[portfolio_end["share_percent"].values])
treemap_end.data[0]["texttemplate"] = " %{label} <br> %{customdata[0]:.1%}"
treemap_end.data[0]["textposition"] = "middle center"
treemap_end.data[0]["textfont"] = dict(family=fnt_fa, size=20, weight="bold")
treemap_end.update_traces(marker=dict(cornerradius=10))

####################################################################################################

fig = make_subplots(rows=1, cols=2, print_grid=False, row_heights=[900],
                    specs=[[{"type": "treemap", "r": -0.025}, {"type": "treemap", "l": -0.025}]],
                    subplot_titles=["<b>انتهای دوره<b>", "<b>ابتدای دوره<b>"])


fig.add_trace(treemap_end.data[0], row=1, col=1)
fig.add_trace(treemap_start.data[0], row=1, col=2)


fig.update_annotations(font=dict(size=25, family=fnt_fa))
fig.update_layout(
    template="gridon",
    font_family=fnt_fa,
    font_size=25,
    margin=dict(l=0, r=0, t=80, b=50),
    height=900,
    showlegend=False
)

fig.write_html(f"./annual_meeting/TreeMap.html", config={"displayModeBar": False})
fig.write_image(f"./annual_meeting/TreeMap.jpeg", width=2000, height=850, scale=10)