import warnings
import pandas as pd
import requests as rq
from tqdm import tqdm

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tsetmc import tsetmc_api
from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
color_green = "#00ff00"
color_red = "#ff0000"
fnt = "B Nazanin"
query_dim_date = ("SELECT TRY_CONVERT(INT, REPLACE([Miladi], '-', '')) as dEven, [Jalali_1] as date "
                  "FROM [nooredenadb].[extra].[dim_date] where Jalali_1>='1402/01/01'")
dim_date = pd.read_sql(query_dim_date, db_conn)

sectors = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[sectors]", db_conn)
query_sectors_trades = ("SELECT date,sector,action,SUM(net_value) AS net_value,SUM(total_cost) AS total_cost FROM "
                        "(SELECT date,symbol,type,action,net_value,total_cost, CASE WHEN symbol='دارایکم' THEN"
                        " 'بانکها و موسسات اعتباری' WHEN symbol='گنگین' THEN 'چندرشته ای صنعتی' ELSE sector END "
                        "AS [sector] FROM [nooredenadb].[sigma].[sigma_buysell] WHERE date>='1402/11/01' AND "
                        "date<='1403/10/30') AS TEMP1 WHERE type IN ('صندوق', 'سهام', 'حق تقدم') and "
                        "sector!='صندوق سرمایه گذاری قابل معامله' GROUP BY date,sector,action,sector ORDER BY date")
sectors_trades = pd.read_sql(query_sectors_trades, db_conn)

sectors_trades.rename({"sector": "sector_name"}, axis=1, inplace=True)
sectors_trades.replace({"ک": "ك", "ی": "ي"}, regex=True, inplace=True)
sector_name_mapper = {"فرآورده هاي نفتي": "فراورده هاي نفتي، كك و سوخت هسته اي",
                      "سيمان آهك گچ": "سيمان، آهك و گچ",
                      "چندرشته اي صنعتي": "شرکتهاي چند رشته اي صنعتي",
                      "دارويي": "مواد و محصولات دارويي",
                      "شيميايي": "محصولات شيميايي",
                      "حمل و نقل انبارداري و ارتباطات " : "حمل ونقل، انبارداري و ارتباطات",
                      "دستگاههاي برقي": "ماشين آلات و دستگاه\\u200cهاي برقي",
                      "خودرو و قطعات": "خودرو و ساخت قطعات",
                      "عرضه برق،گاز،بخار و آب گرم": "عرضه برق، گاز، بخاروآب گرم",
                      "استخراج كانه هاي فلزي": "استخراج کانه هاي فلزي",
                      "بيمه و بازنشستگي": "بيمه وصندوق بازنشستگي به جزتامين اجتماعي",
                      "رايانه": "رايانه و فعاليت\\u200cهاي وابسته به آن",
                      "خرده فروشي": "خرده فروشي،باستثناي وسايل نقليه موتوري",
                      "غذايي بجز قند وشكر": "محصولات غذايي و آشاميدني به جز قند و شكر",
                      "فني و مهندسي": "خدمات فني و مهندسي"}
sectors_trades.replace(sector_name_mapper, regex=False, inplace=True)
sectors_trades_ = sectors_trades.merge(sectors, on="sector_name", how="left")

sectors_id = sectors_trades_.groupby(by=["sector_name", "sector", "sector_id"], as_index=False).first()[
    ["sector_name", "sector", "sector_id"]]

for i in tqdm(range(len(sectors_id))):
    i = 15
    sec = sectors_id["sector"].iloc[i]
    sec_id = sectors_id["sector_id"].iloc[i]
    sec_name = sectors_id["sector_name"].iloc[i]
    sec_his = rq.get(url=tsetmc_api.url_sector_history + sectors_id["sector_id"].iloc[i],
                     headers=tsetmc_api.headers_sector_history)
    sec_his = pd.DataFrame(sec_his.json()["indexB2"])
    sec_his = sec_his[sec_his["dEven"] >= 20240120].reset_index(drop=True)[["dEven", "xNivInuClMresIbs"]]
    if sec_id == "12331083953323969":
        pass
    sec_his = sec_his.merge(dim_date, on="dEven", how="left")[["date", "xNivInuClMresIbs"]]

    sec_trd = sectors_trades_[sectors_trades_["sector_id"] == sectors_id["sector_id"].iloc[i]].reset_index(
        drop=True)[["date", "action", "net_value"]]

    sec_df = sec_his.merge(sec_trd, on="date", how="left")
    sec_df["net_value"] = sec_df["net_value"].fillna(0, inplace=False).astype(int)
    sec_df.rename({"xNivInuClMresIbs": "index", "net_value": "value"}, axis=1, inplace=True)
    sec_df["action"].replace({"خريد": "buy", "فروش": "sell"}, regex=False, inplace=True)

    sec_df["color"] = [color_green if (sec_df["action"].iloc[i] == "buy") else
                       color_red if (sec_df["action"].iloc[i] == "sell") else
                       sec_df["action"].iloc[i] for i in range(len(sec_df))]
    sec_df["size"] = round((sec_df["value"] / sec_df["value"].max()) * 1000).astype(int)

    fig = make_subplots(rows=1, cols=1)
    fig_line = px.line(sec_df, x="date", y="index", line_shape="spline")
    fig.add_trace(fig_line.data[0], row=1, col=1)

    sec_df_ = sec_df[sec_df["value"] > 0].reset_index(drop=True, inplace=False)
    n = 0
    while True:
        sec_df__ = sec_df_.iloc[n * 15: (n + 1) * 15]
        fig_scatter = go.Scatter(x=sec_df__["date"], y=round(1.05 * sec_df__["index"]), line=dict(width=0),
                                 marker=dict(color=sec_df__["color"], size=sec_df__["size"] * 1, sizemode="area", opacity=1),

                                 # customdata=[[sec_df_["amount"].values[i], sec_df_["final_price"].values[i],
                                 #              sec_df_["mean_price"].values[i], sec_df_["cost"].values[i]] for i in
                                 #             range(len(sec_df_))],
                                 # hovertemplate="تعداد: %{customdata[0]:,} <br>"
                                 #               " پایانی قیمت (ریال): %{customdata[1]:,} <br>"
                                 #               " میانگین قیمت (ریال): %{customdata[2]:,} <br>"
                                 #               " مبلغ کل (میلیارد ریال): %{customdata[3]:,} <extra></extra>",
                                 # hoverlabel=dict(font=dict(family=fnt, size=18), align="right")
                                 )
        fig.add_trace(fig_scatter, row=1, col=1)
        n += 1
        if (n * 15) > len(sec_df_):
            break

        # end_day = (jdatetime.datetime.strptime(sec_df["date"].iloc[-1], format="%Y/%m/%d") + jdatetime.timedelta(
        #     days=5)).strftime(format="%Y/%m/%d")
        # fig.update_xaxes(
        #     rangeslider=dict(visible=True, range=[sec_df["date"].iloc[0], end_day], yaxis=dict(rangemode="match"),
        #                      autorange=False))
        fig.update_xaxes(tickfont=dict(family=fnt, size=18), calendar="jalali", tickformat="%Y/%m/%d", tickangle=30, ticklabelstep=3, showgrid=False,
                         # range=[sec_df["date"].iloc[max(-120, -len(sec_df))], end_day]
                         )
        fig.update_yaxes(tickfont=dict(family=fnt, size=18), exponentformat="none", separatethousands=True,
                         side="right", tickprefix=" ", fixedrange=False)

    fig.update_layout(
        title=dict(
            text="نمودار نقاط خرید و فروش در صنعت " + f'<b style="color: blue;">{sec_name}</b>',
            font=dict(size=25, family=fnt),
            xanchor="center"),
        template="seaborn",
        font_family=fnt,
        margin=dict(l=80, r=80, t=80, b=50),
        showlegend=False)

    fig.write_html(f"c:/users/h.damavandi/desktop/sectors_charts/{sec}.html", config={"displayModeBar": False})
