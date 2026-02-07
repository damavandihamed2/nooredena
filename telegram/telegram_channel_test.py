import pandas as pd
import requests as rq
from time import sleep
from bs4 import BeautifulSoup
import json, random, datetime, warnings, xmltodict, jdatetime

from utils.database import make_connection


warnings.filterwarnings("ignore")
powerbi_database = make_connection()

msg_last = pd.read_sql("select * from [nooredenadb].[extra].[msg_last]", powerbi_database)
today = jdatetime.datetime.today()
today_ = today.strftime("%Y/%m/%d")
today_g = (datetime.datetime.today().strftime(format="%Y%m%d"))
proxy = "127.0.0.1:10808"
proxies = {'http': 'socks5://' + proxy,
           'https': 'socks5://' + proxy}
bot_suppervisor = "6167479106:AAH7WrSSQFfwyhlslsWaEntVeQaEA8LNfMY"
bot_news = "5635293221:AAHjZjkQW5FHHvkFEhkNyRBtoLFvko1ATAQ"
news_channel_id = "1930639066"
header = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
           "Accept-Encoding": "gzip, deflate",
           "Accept-Language": "en-US,en;q=0.9,fa;q=0.8",
           "Cache-Control": "max-age=0",
           "Connection": "keep-alive",
           "Cookie": "_ga=GA1.2.1202286904.1669558882; _gid=GA1.2.1684951560.1687093900; _ga_8GD5G4PETZ=GS1.1.1687104026.28.0.1687104026.0.0.0",
           "Host": "cdn.tsetmc.com",
           "Upgrade-Insecure-Requests": "1",
           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
monthes_to_number = {"فروردین": "01", "اردیبهشت": "02", "خرداد": "03",
                     "تیر": "04", "مرداد": "05", "شهریور": "06",
                     "مهر": "07", "آبان": "08", "آذر": "09",
                     "دی": "10", "بهمن": "11", "اسفند": "12"}
donya_e_eqtesad_urls = ["https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz02",
           "https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMw%2C%2C",
           "https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMDc%2C",
           "https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMTA%2C",
           "https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz01MQ%2C%2C",
           "https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMjY%2C",
           "https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0zNQ%2C%2C"]


def deven_to_date(deven):
    deven = datetime.datetime.strptime(deven, "%Y%m%d")
    deven = jdatetime.datetime.fromgregorian(year=deven.year, month=deven.month, day=deven.day)
    deven = deven.strftime("%Y/%m/%d")
    return deven


def heven_to_time(heven):
    hour = heven[:2]
    minute = heven[2:4]
    heven = hour+":"+minute
    return heven


def post_telegram(txt, bot_id, channel_id):
    while True:
        try:
            res = rq.post(f"https://api.telegram.org/bot{bot_id}"
                          f"/sendMessage?chat_id=-100{channel_id}&text={txt}&parse_mode=html",
                           proxies=proxies)
            if res.status_code == 200:
                break
            sleep(random.randint(1, 4))
        except Exception as e:
            sleep(random.randint(1, 4))
            print(e)
    return res.status_code, res.text


def get_msg(flow, n_last):
    r = rq.get(url=f"http://cdn.tsetmc.com/api/Msg/GetMsgByFlow/{flow}/{n_last}", headers=header)
    r = json.loads(r.text)["msg"]
    r = pd.DataFrame(r)
    return r


def g_to_jalali(g_date):
    g_date = datetime.datetime.strptime(g_date.split(",")[1].split("+")[0][1:-1], "%d %b %Y %H:%M:%S")
    g_date = g_date + datetime.timedelta(hours=3, minutes=30)
    year = g_date.year
    month = g_date.month
    day = g_date.day
    time = g_date.strftime("%H:%M")
    date = jdatetime.datetime.fromgregorian(year=year, month=month, day=day).strftime("%Y/%m/%d")
    return date, time


# text_1 = "test"
# post_telegram(text_1, bot_news)

# text_2 = "test_test"
# post_telegram(text_2, bot_news)

# text = text.replace('/', '%2F')
# text = text.replace("(", '%28')
# text = text.replace(")", '%29')

########################################################################################################################

flows = [["1", "بورس"],
         ["2", "فرابورس"],
         ["3", "آتی"],
         ["4", "پایه"],
         ["6", "انرژی"],
         ["7", "کالا"]]

msg_df = pd.DataFrame()
for i in range(len(flows)):
    flow = flows[i][0]
    flow_name = flows[i][1]
    df = get_msg(flow, 100)
    df["flow"] = [flow]*len(df)
    df["flow_name"] = [flow_name]*len(df)
    df["dEven"] = df["dEven"].astype(dtype=str)
    df["hEven"] = df["hEven"].astype(dtype=str)
    df["hEven"] = [("0"*(6-len(df["hEven"].iloc[j])))+df["hEven"].iloc[j] for j in range(len(df))]
    df = df[df["dEven"] == today_g]
    if len(df) > 0:
        if today_g > msg_last["last_date"].iloc[msg_last.index[msg_last["flow"] == flow].values[0]]:
            msg_df = pd.concat([msg_df, df], axis=0, ignore_index=True)
            cursor = powerbi_database.cursor()
            cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = {max(df['dEven'])} WHERE flow='{flow}';")
            cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(df['hEven'])} WHERE flow='{flow}';")
            cursor.commit()
            cursor.close()
            sleep(random.randint(1, 4))
        else:
            df = df[df["hEven"] > msg_last["last_time"].iloc[msg_last.index[msg_last["flow"] == flow].values[0]]]
            if len(df) > 0:
                msg_df = pd.concat([msg_df, df], axis=0, ignore_index=True)
                cursor = powerbi_database.cursor()
                cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(df['hEven'])} WHERE flow='{flow}';")
                cursor.commit()
                cursor.close()
            sleep(random.randint(1, 4))

msg_df["date"] = [deven_to_date(msg_df["dEven"].iloc[i]) for i in range(len(msg_df))]
msg_df["time"] = [heven_to_time(msg_df["hEven"].iloc[i]) for i in range(len(msg_df))]
msg_df.sort_values(by="hEven", inplace=True, ignore_index=True)
msg_df["status_code"] = ["-"]*len(msg_df)

for i in range(len(msg_df)):
    text = "<b>" + msg_df["tseTitle"].iloc[i] + "</b>" + "\n" + "\n" +\
           msg_df["tseDesc"].iloc[i] + "\n" +\
           msg_df["date"].iloc[i] + "  " + msg_df["time"].iloc[i] + "\n" +\
           "#" + msg_df["flow_name"].iloc[i]
    text = text.replace('#', '%23')
    response = post_telegram(txt=text, bot_id=bot_suppervisor, channel_id=news_channel_id)
    msg_df["status_code"].loc[i] = response[0]
    print(f"{i} - {response[0]}")
    sleep(2)

########################################################################################################################

r_1 = rq.get("https://www.boursenews.ir/fa/rss/allnews")
rss_channel = xmltodict.parse(r_1.text)["rss"]["channel"]
items = rss_channel["item"]
items = pd.DataFrame(items)

items["date"] = [datetime.datetime.strptime(items["pubDate"].iloc[i][:-6], "%d %b %Y %H:%M:%S") for i in range(len(items))]
items["dEven"] = [(datetime.datetime.strftime(items["date"].iloc[i], format="%Y%m%d")) for i in range(len(items))]
items["hEven"] = [(datetime.datetime.strftime(items["date"].iloc[i], format="%H%m%S")) for i in range(len(items))]
items = items[items["dEven"] == today_g]
if len(items) > 0:
    if today_g > msg_last["last_date"].iloc[msg_last.index[msg_last["flow"] == "boursenews"].values[0]]:
        cursor = powerbi_database.cursor()
        cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = {max(items['dEven'])} WHERE flow='boursenews';")
        cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(items['hEven'])} WHERE flow='boursenews';")
        cursor.commit()
        cursor.close()
        sleep(random.randint(1, 4))
    else:
        items = items[items["hEven"] > msg_last["last_time"].iloc[msg_last.index[msg_last["flow"] == "boursenews"].values[0]]]
        if len(items) > 0:
            cursor = powerbi_database.cursor()
            cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(items['hEven'])} WHERE flow='boursenews';")
            cursor.commit()
            cursor.close()
            sleep(random.randint(1, 4))

if len(items) > 0:
    items["date"] = [deven_to_date(items["dEven"].iloc[i]) for i in range(len(items))]
    items["time"] = [heven_to_time(items["hEven"].iloc[i]) for i in range(len(items))]
    items.sort_values(by="hEven", inplace=True, ignore_index=True)
    items["status_code"] = ["-"]*len(items)
    for i in range(len(items)):
        text = "<b>" + items["title"].iloc[i] + "</b>" + "\n" + "\n" +\
               items["description"].iloc[i] + "\n" + "\n" + \
               '<a href="' + items["link"].iloc[i] + '">لینک خبر</a>' + "\n" + "\n" + \
               items["date"].iloc[i] + "  " + items["time"].iloc[i] + "\n" +\
               "#" + "boursenews"
        text = text.replace('#', '%23')
        response = post_telegram(txt=text, bot_id=bot_news, channel_id=news_channel_id)
        items["status_code"].loc[i] = response[0]
        print(f"{i} - {response[0]}")
        sleep(2)

#######################################################################################################################

r_2 = rq.get("https://www.ifb.ir/RSSForNews.aspx")
rss__channel = xmltodict.parse(r_2.text)["rss"]["channel"]
items = rss__channel["item"]
items = pd.DataFrame(items)
for i in range(len(items)):
    items["pubDate"].iloc[i] = items["pubDate"].iloc[i].replace("ق.ظ", "AM")
    items["pubDate"].iloc[i] = items["pubDate"].iloc[i].replace("ب.ظ", "PM")
items["dEven"] = [jdatetime.datetime.strptime(items["pubDate"].iloc[i][:10], "%d/%m/%Y").togregorian().strftime(format="%Y%m%d") for i in range(len(items))]
items["hEven"] = [datetime.datetime.strptime(items["pubDate"].iloc[i][11:], "%I:%M:%S %p").strftime(format="%H%M%S") for i in range(len(items))]

items = items[items["dEven"] == today_g]
if len(items) > 0:
    if today_g > msg_last["last_date"].iloc[msg_last.index[msg_last["flow"] == "ifb"].values[0]]:
        cursor = powerbi_database.cursor()
        cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = {max(items['dEven'])} WHERE flow='ifb';")
        cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(items['hEven'])} WHERE flow='ifb';")
        cursor.commit()
        cursor.close()
        sleep(random.randint(1, 4))
    else:
        items = items[items["hEven"] > msg_last["last_time"].iloc[msg_last.index[msg_last["flow"] == "ifb"].values[0]]]
        if len(items) > 0:
            cursor = powerbi_database.cursor()
            cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(items['hEven'])} WHERE flow='ifb';")
            cursor.commit()
            cursor.close()
            sleep(random.randint(1, 4))

if len(items) > 0:
    items["date"] = [deven_to_date(items["dEven"].iloc[i]) for i in range(len(items))]
    items["time"] = [heven_to_time(items["hEven"].iloc[i]) for i in range(len(items))]
    items.sort_values(by="hEven", inplace=True, ignore_index=True)
    items["status_code"] = ["-"] * len(items)
    for i in range(len(items)):
        text = "<b>" + items["title"].iloc[i] + "</b>" + "\n" + "\n" + \
               '<a href="' + items["link"].iloc[i] + '">لینک خبر</a>' + "\n" + "\n" + \
               items["date"].iloc[i] + "  " + items["time"].iloc[i] + "\n" + \
               "#" + "ifb"
        text = text.replace('#', '%23')
        response = post_telegram(txt=text, bot_id=bot_news, channel_id=news_channel_id)
        items["status_code"].loc[i] = response[0]
        print(f"{i} - {response[0]}")
        sleep(2)


#######################################################################################################################

r_3 = rq.get("https://boursepress.ir/page/rss")
rss___channel = xmltodict.parse(r_3.text)["rss"]["channel"]
items = rss___channel["item"]
items = pd.DataFrame(items)

items["dEven"] = [datetime.datetime.strptime(items["pubDate"].iloc[i][:-4], "%a, %d %b %Y %H:%M:%S").strftime(format="%Y%m%d") for i in range(len(items))]
items["hEven"] = [datetime.datetime.strptime(items["pubDate"].iloc[i][:-4], "%a, %d %b %Y %H:%M:%S").strftime(format="%H%M%S") for i in range(len(items))]

items = items[items["category"] != "دیگر رسانه ها"]
items = items[items["dEven"] == today_g]
if len(items) > 0:
    if today_g > msg_last["last_date"].iloc[msg_last.index[msg_last["flow"] == "boursepress"].values[0]]:
        cursor = powerbi_database.cursor()
        cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = {max(items['dEven'])} WHERE flow='boursepress';")
        cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(items['hEven'])} WHERE flow='boursepress';")
        cursor.commit()
        cursor.close()
        sleep(random.randint(1, 4))
    else:
        items = items[items["hEven"] > msg_last["last_time"].iloc[msg_last.index[msg_last["flow"] == "boursepress"].values[0]]]
        if len(items) > 0:
            cursor = powerbi_database.cursor()
            cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = {max(items['hEven'])} WHERE flow='boursepress';")
            cursor.commit()
            cursor.close()
            sleep(random.randint(1, 4))

if len(items) > 0:
    items["date"] = [deven_to_date(items["dEven"].iloc[i]) for i in range(len(items))]
    items["time"] = [heven_to_time(items["hEven"].iloc[i]) for i in range(len(items))]
    items.sort_values(by="hEven", inplace=True, ignore_index=True)
    for i in range(len(items)):
        text = "<b>" + items["title"].iloc[i] + "</b>" + "\n" + "\n" + \
               items["description"].iloc[i] + "\n" + "\n" + \
               '<a href="' + items["link"].iloc[i] + '">لینک خبر</a>' + "\n" + "\n" + \
               items["date"].iloc[i] + "  " + items["time"].iloc[i] + "\n" + \
               "#" + "boursepress"
        text = text.replace('#', '%23')
        response = post_telegram(txt=text, bot_id=bot_news, channel_id=news_channel_id)
        print(f"{i} - {response[0]}")
        sleep(2)

#######################################################################################################################
r_4 = rq.get("https://www.bourse24.ir/news")

soup = BeautifulSoup(r_4.text, features="html.parser")
news = soup.find_all(name='div', class_='col-md-4 col-lg-3')

items = pd.DataFrame()
for i in range(len(news)):
    tmp = pd.DataFrame()
    new = news[i].find_all(name="div", class_="post-content")

    href = new[0].find_all(name="a")
    title = href[0].text
    link = href[0].attrs["href"]
    id_ = link.split("/")[-2]
    category = href[1].text
    type = href[2].text
    tmp["title"] = [title]
    tmp["link"] = ["https://www.bourse24.ir/news/" + id_]
    tmp["category"] = [category]
    tmp["type"] = [type]
    tmp["id"] = [id_]

    meta = new[0].find_all(name="span")
    day = meta[1].text[1:-9].split(" ")[0]
    month = meta[1].text[1:-9].split(" ")[1]
    year = meta[1].text[1:-9].split(" ")[2]
    time = meta[1].text[-6:-1]
    tmp["year"] = [year]
    tmp["month"] = [month]
    tmp["day"] = [day]
    tmp["time"] = [time]
    items = pd.concat([items, tmp], axis=0, ignore_index=True)

items["month"].replace(monthes_to_number, inplace=True)
items = items[[items["category"].iloc[i] in ["کدال نگر ", "ویژه های خبری ", "در لحظه با مجامع ", "قاصدک "] for i in range(len(items))]]
items = items[items["id"] > msg_last["last_date"].iloc[msg_last.index[msg_last["flow"] == "bourse24"].values[0]]]

if len(items) > 0:
    items["date"] = items["year"] + ["/"] + items["month"] + ["/"] + items["day"]
    cursor = powerbi_database.cursor()
    cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = {max(items['id'])} WHERE flow='bourse24';")
    cursor.commit()
    cursor.close()
    items.sort_values(by="id", inplace=True, ignore_index=True)
    for i in range(len(items)):
        text = "<b>" + items["title"].iloc[i] + "</b>" + "\n" + "\n" + \
               '<a href="' + items["link"].iloc[i] + '">لینک خبر</a>' + "\n" + "\n" + \
               items["date"].iloc[i] + "  " + items["time"].iloc[i] + "\n" + \
               "#" + "bourse24"
        text = text.replace('#', '%23')
        text = text.replace('+', '%2B')
        response = post_telegram(txt=text, bot_id=bot_news, channel_id=news_channel_id)
        print(f"{i} - {response[0]}")
        sleep(2)

#######################################################################################################################

items = pd.DataFrame()
for u in range(len(donya_e_eqtesad_urls)):
    r_5 = rq.get(donya_e_eqtesad_urls[u])
    r_5 = xmltodict.parse(r_5.text)
    rss = r_5["rss"]
    rss_channel = rss["channel"]
    if "item" in rss_channel.keys():
        item = rss_channel["item"]
        item = pd.DataFrame(item)
        items = pd.concat([items, item], axis=0, ignore_index=True)
    sleep(random.randint(1, 4))

items["id"] = [items["link"].iloc[i].split("-")[-1] for i in range(len(items))]
items["date"] = [g_to_jalali(items["pubDate"].iloc[i])[0] for i in range(len(items))]
items["time"] = [g_to_jalali(items["pubDate"].iloc[i])[1] for i in range(len(items))]
items = items[items["id"] > msg_last["last_date"].iloc[msg_last.index[msg_last["flow"] == "donya-e-eqtesad"].values[0]]]

if len(items) > 0:
    cursor = powerbi_database.cursor()
    cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = {max(items['id'])} WHERE flow='donya-e-eqtesad';")
    cursor.commit()
    cursor.close()
    items.sort_values(by="id", inplace=True, ignore_index=True)
    for i in range(len(items)):
        text = "<b>" + items["title"].iloc[i] + "</b>" + "\n" + "\n" + \
               '<a href="' + items["link"].iloc[i] + '">لینک خبر</a>' + "\n" + "\n" + \
               items["date"].iloc[i] + "  " + items["time"].iloc[i] + "\n" + \
               "#" + "donya_e_eqtesad"
        text = text.replace('#', '%23')
        text = text.replace('+', '%2B')
        response = post_telegram(txt=text, bot_id=bot_news, channel_id=news_channel_id)
        print(f"{i} - {response[0]}")
        sleep(2)

#######################################################################################################################
# ("https://bourseon.com/")
"""
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz02"                             بورس
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMw%2C%2C"                 بورس کالا
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMDc%2C"                       کدال
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMTA%2C"                       روند
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz01MQ%2C%2C"                آینده_نگر
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0xMjY%2C"                        نفت
"https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0zNQ%2C%2C"                 پتروشیمی
"""
"""
def insert_to_database(dataframe, database_table):
    cursor = powerbi_database.cursor()
    db_table = database_table
    if len(dataframe) // 1000 != 0:
        lup = len(dataframe) // 1000
        lup_extended = len(dataframe) % 1000
        for i in range(lup):
            values = ""
            for j in range(i * 1000, (i + 1) * 1000):
                values = values + (str(tuple(dataframe.iloc[j, :])).replace('nan', 'NULL')).replace("None", "NULL")
                values = values + ","
            values = values[:-1]
            cursor.execute(f"INSERT INTO {db_table} ({', '.join(dataframe.columns.tolist())}) VALUES {values}")
            cursor.commit()
        if lup_extended != 0:
            values = ""
            for k in range(lup * 1000, len(dataframe)):
                values = values + (str(tuple(dataframe.iloc[k, :])).replace('nan', 'NULL')).replace("None", "NULL")
                values = values + ","
            values = values[:-1]
            cursor.execute(f"INSERT INTO {db_table} ({', '.join(dataframe.columns.tolist())}) VALUES {values}")
            cursor.commit()
    else:
        values = ""
        for h in range(len(dataframe)):
            values = values + (str(tuple(dataframe.iloc[h, :])).replace('nan', 'NULL')).replace("None", "NULL")
            values = values + ","
        values = values[:-1]
        cursor.execute(f"INSERT INTO {db_table} ({', '.join(dataframe.columns.tolist())}) VALUES {values}")
        cursor.commit()
    cursor.close()


insert_to_database(pd.DataFrame(data={"flow": ["1", "2", "3", "4", "6", "7", "boursenews", "ifb", "boursepress"],
                                      "last_time": ["113528", "112659", "105046", "000000", "083718", "000000", "110657", "132000", "000100"],
                                      "last_date": ["20230625", "20230625", "20230625", "20230620", "20230625", "20230620", "20230625", "20230624", "20230625"]}), "[nooredenadb].[extra].[msg_last]")

insert_to_database(pd.DataFrame(data={"flow": ["donya-e-eqtesad"],
                                      "last_time": ["000000"],
                                      "last_date": ["3981493"]}), "[nooredenadb].[extra].[msg_last]")


fow = "boursepress"
cursor = powerbi_database.cursor()
cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_date = '20230625' WHERE flow='{fow}';")
cursor.execute(f"UPDATE [nooredenadb].[extra].[msg_last] SET last_time = '112659' WHERE flow='{fow}';")
cursor.commit()
cursor.close()


"""
