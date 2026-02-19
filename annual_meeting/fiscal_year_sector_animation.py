import warnings
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
(fnt, color1, color2) = ("B Nazanin", "#56c4cd", "#f8a81d")
colors = px.colors.qualitative.Alphabet
bar_numbers = 15

dim_date = pd.read_sql("SELECT [Jalali_1] as date, [JWeekDay] FROM [nooredenadb].[extra].[dim_date]", db_conn)
query_sector_value = ("SELECT date, SUM(value) AS value, sector FROM (SELECT [date], [gross_value_final_price] AS "
                      "value, CASE WHEN symbol = 'دارایکم' THEN 'بانکها و موسسات اعتباری' WHEN symbol = 'گنگین' THEN"
                      " 'چندرشته ای صنعتی' ELSE sector END AS [sector] FROM [nooredenadb].[sigma].[sigma_portfolio] WHERE "
                      "date>='1402/10/30' AND date<'1403/11/01') AS TEMP GROUP BY date, sector ORDER BY date")
sector_value = pd.read_sql(query_sector_value, db_conn)
sectors_mapper = {
    'ماشین آلات و تجهیزات': "ماشین آلات",
    'بانکها و موسسات اعتباری': "بانک ها",
    # 'چندرشته ای صنعتی': "",
    # 'شیمیایی': "",
    'خودرو و قطعات': "خودرو",
    'سرمایه گذاریها': "سرمایه گذاری ها",
    'استخراج زغال سنگ': "زغال سنگ",
    'سیمان آهک گچ': "سیمان",
    'فلزات اساسی': "فلزات",
    'فرآورده های نفتی': "نفتی",
    # 'فنی و مهندسی': "",
    # 'دارویی': "",
    'استخراج کانه های فلزی': "معادن",
    # 'ابزار پزشکی': "",
    'صندوق سرمایه گذاری قابل معامله': "صندوق",
    # 'مخابرات': "",
    'عرضه برق،گاز،بخار و آب گرم': "یوتیلیتی",
    'بیمه و بازنشستگی': "بیمه",
    'لاستیک و پلاستیک': "لاستیک",
    'کاشی و سرامیک': "کاشی",
    'حمل و نقل انبارداری و ارتباطات ': "حمل و نقل",
    # 'خرده فروشی': "",
    # 'دستگاههای برقی': "",
    'غذایی بجز قند وشکر': "غذایی",
    # 'رایانه': "",
    'زراعت و خدمات وابسته': "زراعی",
    # 'اطلاعات و ارتباطات': "",
}
sector_value["sector"].replace(sectors_mapper, inplace=True)
sector_color = pd.DataFrame({"sector": sector_value["sector"].unique().tolist(),
                             "color": colors + ["#000000"]})
sector_value = sector_value.merge(sector_color, on="sector", how="left")
sector_value.sort_values(["date", "value"], inplace=True, ignore_index=True, ascending=False)
sector_value["value"] /= 1e9

# sector_value.to_excel("c:/users/h.damavandi/desktop/sectors_value_raw.xlsx", index=False)

# fig = px.bar(sector_value, x="value", y="sector", animation_frame="date", animation_group="sector",
#              orientation="h", color="sector",
#              range_x=[0,25000],
#        )
# fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 5000
# fig.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 3000
# fig.write_html("c:/users/h.damavandi/desktop/new_fig.html")


sector_value["value"] = round(sector_value["value"])
sector_value = sector_value.merge(dim_date, on="date", how="left")
sector_value = sector_value[~sector_value["JWeekDay"].isin(["پنج شنبه", "جمعه"])].reset_index(drop=True)
sector_value.drop(columns=["JWeekDay"], inplace=True)

sector_value_flourish = pd.DataFrame(columns=["sector"])
for d in sorted(sector_value["date"].unique().tolist()):
    tmp = sector_value[sector_value["date"] == d][["sector", "value"]]
    tmp.rename({"value": d}, axis=1, inplace=True)
    sector_value_flourish = sector_value_flourish.merge(tmp, on="sector", how="outer")
sector_value_flourish.fillna(0, inplace=True)
sector_value_flourish.to_excel("c:/users/h.damavandi/desktop/sector_value_flourish.xlsx", index=False)

sector_value["year-month"] = sector_value["date"].str[:7]
sector_value = sector_value[["year-month", "date"]].groupby(by=["year-month"]).max().merge(
    sector_value, on="date", how="left")
sector_value = sector_value.merge(pd.DataFrame(
    {"date": sorted(sector_value["date"].unique()), "date_": list(range(13))}), on="date", how="left")
sector_value.to_excel("c:/users/h.damavandi/desktop/sectors_value.xlsx", index=False)


################################################

dates = sorted(sector_value["date"].unique().tolist())
n_frame={}
for day, d in zip(dates, dates):
    dataframe=sector_value[(sector_value['date']==day)]
    dataframe=dataframe.nlargest(n=bar_numbers, columns=["value"])
    dataframe.sort_values(by="value", inplace=True, ignore_index=True, ascending=True)
    n_frame[d] = dataframe


fig = go.Figure(
    data=[
        go.Bar(
            x=n_frame["1402/10/30"]["value"],
            y=n_frame["1402/10/30"]["sector"],
            orientation='h',
            texttemplate='%{x:,.0f}',
            textfont={'size':16, "family": fnt, "weight": "bold"},
            textposition='outside',
            insidetextanchor='end',
            width=0.75,
            marker={'color': n_frame["1402/10/30"]['color']}
        )
    ],
    layout=go.Layout(
        xaxis=dict(
            range=[0, 25000],
            autorange=False,
            title=dict(
                text="ارزش صنعت در پرتفوی (میلیارد ریال)",
                font=dict(
                    size=18,
                    family=fnt,
                    weight="bold"
                )
            ),
            tickfont=dict(
                size=16,
                family=fnt,
                weight="bold"
            ),
            tickformat=",",
        ),
        yaxis=dict(
            range=[len(n_frame["1402/10/30"]) - 0.5 - bar_numbers, len(n_frame["1402/10/30"]) - 0.5],
            autorange=False,
            tickfont=dict(
                size=16,
                family=fnt,
                weight="bold"
            )
        ),
        title=dict(
            text="ارزش صنعت در پرتفوی (میلیارد ریال)" + "<br>" + "1402/10/30",
            font=dict(
                size=28,
                family=fnt,
                weight="bold"
            ),
            x=0.5,
            xanchor='center'
        ),
        updatemenus=[dict(
            type="buttons",
            active=0,
            buttons=[
                dict(
                    label="شروع",
                    method="animate",
                    # https://github.com/plotly/plotly.js/blob/master/src/plots/animation_attributes.js
                    args=[
                        None,
                        {
                            "frame": {"duration": 350, "redraw": True},
                            "mode": "immediate",
                            "transition": {"duration": 350, "easing": "linear"}
                        }
                    ]
                )
            ]
        )]
    ),

    frames=[
            go.Frame(
                data=[
                        go.Bar(
                            x=value["value"],
                            y=value["sector"],
                            orientation='h',
                            texttemplate='%{x:,.0f}',
                            textfont={'size': 16, "family": fnt, "weight": "bold"},
                            marker={'color': value['color']}
                        )
                    ],
                layout=go.Layout(
                        xaxis=dict(
                            range=[0, 25000],
                            autorange=False,
                            tickformat=",",
                            tickfont=dict(size=16, family=fnt, weight="bold")
                        ),
                        yaxis=dict(
                            range=[len(value) - 0.5 - bar_numbers , len(value) - 0.5],
                            autorange=False,
                            tickfont=dict(size=16, family=fnt, weight="bold")
                        ),
                        title=dict(text="ارزش صنعت در پرتفوی (میلیارد ریال)" + "<br>" + f"{key}", font=dict(size=28, family=fnt, weight="bold")),

                    )
            )
        for key, value in n_frame.items()
    ]
)

#-------------------------------------------

fig.write_html("c:/users/h.damavandi/desktop/animation.html", auto_play=False)



