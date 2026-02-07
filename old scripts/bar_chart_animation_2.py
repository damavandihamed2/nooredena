import warnings
import datetime
import jdatetime
import pandas as pd

import plotly.express as px
from plotly.subplots import make_subplots

from utils.database import make_connection


warnings.filterwarnings("ignore")

closed_days = ["1401-12-17", "1401-12-29", "1402-01-01", "1402-01-02", "1402-01-12", "1402-01-13", "1402-01-23",
               "1402-02-02", "1402-02-03", "1402-02-26", "1402-03-14", "1402-03-15", "1402-06-15", "1402-06-25",
               "1402-07-02", "1402-07-11", "1402-09-26", "1402-11-22", "1402-12-06", "1402-12-29", "1403-01-01"]
today = jdatetime.datetime.today()
today_ = today.strftime("%Y-%m-%d")
today__ = today.strftime("%Y/%m/%d")
weekday = today.weekday()
fiscal_year = "1401-11-01"
one_week = (today - datetime.timedelta(days=today.weekday() + 3)).strftime("%Y-%m-%d")
one_month = (today - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
two_month = (today - datetime.timedelta(days=61)).strftime("%Y-%m-%d")
three_month = (today - datetime.timedelta(days=91)).strftime("%Y-%m-%d")
six_month = (today - datetime.timedelta(days=181)).strftime("%Y-%m-%d")
nine_month = (today - datetime.timedelta(days=271)).strftime("%Y-%m-%d")
one_year = (today - datetime.timedelta(days=366)).strftime("%Y-%m-%d")


def adj_final_price_date(dataframe, date):
    dataframe_ = dataframe[dataframe["date"] <= date]
    if len(dataframe_) == 0:
        price = dataframe["final_price_adj"].iloc[dataframe.index[dataframe["date"] == min(dataframe["date"])].values[0]]
    else:
        price = dataframe["final_price_adj"].iloc[dataframe.index[dataframe["date"] == max(dataframe_["date"])].values[0]]
    return price


powerbi_database = make_connection()

#######################################################################################################################

names = pd.read_excel("//filesrv/Public/گزارش روزانه/Backup/name_amount.xlsx")
names = names[names["تعداد"] > 0]
names.drop(index=[names.index[names["نماد"] == "آكنتور"].values[0]], inplace=True)
names.reset_index(drop=True, inplace=True)

print("----------------------------------------------------------------------", "\n",
      "loading historical data of portfolio stocks", "\n",
      "----------------------------------------------------------------------")

portfo_history = pd.DataFrame()
for i in range(len(names)):
    symbol = names["نماد"].iloc[i]
    if symbol[0] not in ["ض", "ط"]:
        while True:
            try:
                s = pd.read_sql(f"SELECT [date],[symbol],[final_price],[yesterday_price] FROM [nooredenadb].[tsetmc].[stock_historical_data] where symbol=('{symbol}')", powerbi_database)
                if s is None:
                    break
                if len(s) == 0:
                    break
                else:
                    s.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
                    # if symbol in symbols_data["symbol"].values:
                    #     new_data = pd.DataFrame(data={
                    #         "date": [today_],
                    #         "symbol": symbol,
                    #         "final_price": [symbols_data["final_price"].iloc[
                    #                             symbols_data.index[symbols_data["symbol"] == symbol].values[0]]],
                    #         "yesterday_price": [symbols_data["yesterday_price"].iloc[
                    #                                 symbols_data.index[symbols_data["symbol"] == symbol].values[0]]]
                    #     })
                    #     s = pd.concat([s, new_data], axis=0, ignore_index=True)
                    s.drop_duplicates(subset="date", keep="first", inplace=True, ignore_index=True)
                    s["coef"] = (s["yesterday_price"].shift(-1) / s["final_price"]).fillna(1.0)
                    s["adj_coef"] = s.iloc[::-1]["coef"].cumprod().iloc[::-1]
                    s["final_price_adj"] = round(s["final_price"] * s["adj_coef"])

                    temp = pd.DataFrame()
                    t1 = "1401-02-01"
                    while t1 < today_:
                        t2 = (jdatetime.datetime.strptime(date_string=t1, format="%Y-%m-%d") + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
                        tmp = pd.DataFrame()
                        tmp["symbol"] = [symbol]
                        tmp["return"] = [(adj_final_price_date(s, t2) - adj_final_price_date(s, t1))/adj_final_price_date(s, t1)]
                        tmp["period"] = [t1]
                        temp = pd.concat([temp, tmp],axis=0, ignore_index=True)
                        t1 = t2
                    portfo_history = pd.concat([portfo_history, temp], axis=0, ignore_index=True)
                    break
            except Exception as e:
                print(e)
                print(e)

##################################################################################################

# for i, j in [["return_week", "price_week"], ["return_one_month", "price_one_month"],
#              ["return_two_month", "price_two_month"], ["return_three_month", "price_three_month"],
#              ["return_six_month", "price_six_month"], ["return_nine_month", "price_nine_month"],
#              ["return_year", "price_year"], ["return_fiscal_year", "price_fiscal_year"]]:
#     portfo_history[i] = (portfo_history["price_last"] - portfo_history[j])/portfo_history[j]
####################################################################################################################
print("making plots ready")
fnt = "B Nazanin"
color1 = "#56c4cd"
color2 = "#f8a81d"
col_pos = "#018501"
col_neg = "#cc0202"
col_black = "#000000"
col_neu = "#858585"
########################################################################################################################
########################################################################################################################

########################################################################################################################
########################################################################################################################

dates = list(portfo_history["period"].unique())

df_ = pd.DataFrame()
color_ = []
for i in range(len(dates)):
    portfo_history_ = portfo_history[portfo_history["period"] == dates[i]]
    portfo_history_.sort_values("return", inplace=True, ignore_index=True, ascending=False)
    df_ = pd.concat([df_, portfo_history_], axis=0, ignore_index=True)
    color_.append(["#f57842" if portfo_history_["symbol"].iloc[i] == "فولاد" else
                   color1 for i in range(len(portfo_history_))])

fg = px.bar(data_frame=df_, x="symbol", y="return", range_y=[-0.15, 0.3], animation_frame="period"
            # , color="clr", color_discrete_map={color1: color1, color2: color2}
            )
for i in range(len(fg.frames)):
    fg.frames[i].data[0].marker["color"] = color_[i]

fg.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 1000000
fg.layout.updatemenus[0].buttons[0].args[1]["frame"]["redraw"] = False
fg.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = 1

# fg.update_layout(transition={
#     "duration": 1
#     # , "ordering": "return"
# })
fg.write_html("C:/Users/damavandi/Desktop/barchart_animation.html", config={"displayModeBar": False})


########################################################################################################################
########################################################################################################################
fg = make_subplots(
    rows=1,
    cols=1,
    print_grid=False,
    specs=[[{"type": "bar"}]],
)
labels = ["هفتگی", "یک ماهه", "دو ماهه", "سه ماهه", "شش ماهه", "نه ماهه", "سالانه", "از ابتدای سال مالی"]
titles = [
    f"بازدهی هفتگی - از تاریخ {one_week.replace('-', '/')}",
    f"بازدهی ماهانه - از تاریخ {one_month.replace('-', '/')}",
    f"بازدهی 2 ماهه - از تاریخ {two_month.replace('-', '/')}",
    f"بازدهی 3 ماهه - از تاریخ {three_month.replace('-', '/')}",
    f"بازدهی 6 ماهه - از تاریخ {six_month.replace('-', '/')}",
    f"بازدهی 9 ماهه - از تاریخ {nine_month.replace('-', '/')}",
    f"بازدهی سالانه - از تاریخ {one_year.replace('-', '/')}",
    f"بازدهی از ابتدای سال مالی - از تاریخ {fiscal_year.replace('-', '/')}"
]
buttons = []
for i, label in enumerate(labels):
    visibility = [i == j for j in range(len(labels))]
    button = dict(label="<b>" + label + "</b>",
                  method="update",
                  args=[{"visible": visibility},
                        {"title": f"<b>بازدهی سهم های پرتفوی - {today__}</b>" + "<br>" + titles[i]}])
    buttons.append(button)
updatemenus = list([
    dict(active=0,
         x=-0.05,
         y=1.025,
         buttons=buttons)
])

fg["layout"]["updatemenus"] = updatemenus
fg.update_annotations(font=dict(size=25, family=fnt))
fg.update_xaxes(showticklabels=False, showgrid=False)
fg.update_yaxes(tickfont=dict(family=fnt, size=22), showgrid=False, ticksuffix=" ")
fg.update_layout(
    title=dict(
        text=f"<b>بازدهی سهم های پرتفوی - {today__}</b>" + "<br>" + titles[0],
        font=dict(size=25, family=fnt),
        xanchor="center"
    ),
    template="seaborn",
    font_family=fnt,
    margin=dict(l=75, r=75, t=150, b=100),
    height=50*len(names),
    showlegend=False
)
Ld = len(fg.data)
for k in range(1, Ld):
    fg.update_traces(visible=False, selector=k)
# fg.write_html("C:/Users/damavandi/Desktop/barchart_animation.html", config={"displayModeBar": False})
