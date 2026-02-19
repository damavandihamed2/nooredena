import numpy as np
import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
import tgju_crawl as tg
from bs4 import BeautifulSoup
import json, random, warnings, jdatetime
from playwright.sync_api import sync_playwright

import enigma
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
authenticator = [{"username": "09210882478", "password": "MMR123456"}]
captcha_path = "D:/database/captcha_image/enigma/"
result_path = "D:/database/commodity/results.xlsx"
urea_path = "D:/database/commodity/urea.xlsx"
gas_path = "D:/database/commodity/gas_price.xlsx"
sugar_path = "D:/database/commodity/sugar_price.xlsx"
proxy = "127.0.0.1:10808"
proxies = {'http': 'socks5://' + proxy,
           'https': 'socks5://' + proxy}
db_conn = make_connection()
today = jdatetime.datetime.today()

dim_date = pd.read_sql("SELECT TRY_CONVERT(VARCHAR, Miladi) as Miladi, REPLACE(Jalali_1, '/', '-') as date "
                       "FROM [nooredenadb].[extra].[dim_date]", db_conn)

#######################################################################################################################

gas = pd.read_excel(gas_path)
gas["date_jalali"].replace(" ", "", regex=True, inplace=True)
gas["date_jalali"].replace("/", "-", regex=True, inplace=True)
gas = gas[["date_jalali", "خوراک صنایع (دلار بر مترمکعب)"]]
gas.rename({"خوراک صنایع (دلار بر مترمکعب)": "price"}, inplace=True, axis=1)
gas["date"] = [jdatetime.datetime.strptime(gas["date_jalali"].iloc[i], "%Y-%m-%d").togregorian().strftime("%Y-%m-%d")
               for i in range(len(gas))]
gas["owner"] = "razmehgir"
gas["commodity"] = "گاز خوراک"
gas["unit"] = "دلار بر مترمکعب"
gas["reference"] = "وزارت نفت"
gas["name"] = "گاز خوراک - (دلار بر مترمکعب)"

last_date_jalali = pd.read_sql("SELECT MAX(date_jalali) as date FROM [nooredenadb].[commodity].[commodities_data]"
                               " WHERE commodity='گاز خوراک'", db_conn)
last_date_jalali = last_date_jalali["date"].iloc[0]
gas = gas[gas["date_jalali"] > last_date_jalali]
if len(gas) > 0:
    gas.reset_index(drop=True, inplace=True)
    insert_to_database(dataframe=gas, database_table="[nooredenadb].[commodity].[commodities_data]")

#######################################################################################################################

while True:
    captcha = enigma.get_captcha()
    enigma_login = enigma.login(username=authenticator[0]["username"], password=authenticator[0]["password"],
                                captcha_key=captcha["key"], captcha_value=captcha["value"])
    if enigma_login.status_code == 200:

        crsr = db_conn.cursor()
        crsr.execute(f"UPDATE [nooredenadb].[extra].[captcha_images] SET "
                     f"captcha_value='{captcha["value"]}' WHERE captcha_id='{captcha['captcha_id']}'")
        crsr.close()

        access_token = json.loads(enigma_login.text)["data"]["token"]["access"]
        refresh_token = json.loads(enigma_login.text)["data"]["token"]["refresh"]
        break
    sleep(3)

#######################################################################################################################

cmdt_raw = pd.read_sql("SELECT * FROM [nooredenadb].[commodity].[commodities_detail_data]", db_conn)
cmdt = cmdt_raw[cmdt_raw["id"] != "-"]
temporary_df = pd.DataFrame()
for i in tqdm(range(len(cmdt))):
    comm_names = [cmdt["retrieve_slug"].iloc[i].replace(">", "").replace("<", "")]
    while True:
        commodities_response = enigma.get_commodities(commodities=comm_names, access_token=access_token)
        if commodities_response.status_code == 200:
            break
        elif commodities_response.status_code == 401:
            access_token = enigma.refresh_access_token(old_access_token=access_token, refresh_token=refresh_token)
        elif commodities_response.status_code == 402:
            print("Your Account Has Expired")
        elif commodities_response.status_code == 400:
            print("رفرنس فرستاده شده وجود ندارد." + f"{comm_names}")
            break
        else:
            print(commodities_response.status_code, " - ", commodities_response.text)
            break
        sleep(random.randint(2, 6))

    if len(commodities_response.json()["data"]) > 0:
        temp_df = pd.DataFrame(commodities_response.json()["data"]["chart_data"])
        temp_df["owner"] = [cmdt["owner"].iloc[i]]*len(temp_df)
        temp_df["commodity"] = [json.loads(commodities_response.text)["data"]["market_data"][0][
                                    "first_operand_commodity"]]*len(temp_df)
        temp_df["unit"] = [json.loads(commodities_response.text)["data"]["market_data"][0][
                               "first_operand_unit"]]*len(temp_df)
        temp_df["reference"] = [json.loads(commodities_response.text)["data"]["market_data"][0][
                                    "first_operand_reference"]]*len(temp_df)

        temporary_df = pd.concat([temporary_df, temp_df], axis=0, ignore_index=True)
    # sleep(random.randint(1, 3))

temporary_df["name"] = temporary_df["commodity"] + " - " + temporary_df["reference"] + " (" + temporary_df["unit"] + ")"
temporary_df.rename({"result_1": "price"}, axis=1, inplace=True)

complete_df = pd.DataFrame()
for i in tqdm(range(len(cmdt_raw))):

    cmdt_name = cmdt_raw["name"].iloc[i]

    if cmdt_raw["result_sheet"].iloc[i] == "-":
        temp = temporary_df[temporary_df["name"] == cmdt_raw["name"].iloc[i]]
        temp.sort_values(by="date", ascending=True, ignore_index=True, inplace=True)
        complete_df = pd.concat([complete_df, temp], axis=0, ignore_index=True)

    elif cmdt_raw["id"].iloc[i] == "-":

        if cmdt_raw["result_name"].iloc[i] in ["Granular urea(Iran fob)($/t)", "Granular urea(Brazil (cfr))($/t)",
                                               "Granular urea(Middle East non-US netbacks fob)($/t)"]:

            result = pd.read_excel(result_path, sheet_name=cmdt_raw["result_sheet"].iloc[i])
            result.replace("None", np.nan, inplace=True)
            result = result[result["Unnamed: 0"] == cmdt_raw["result_name"].iloc[i]]
            result.reset_index(drop=True, inplace=True)
            result = result.T
            result = result.iloc[1:, :]
            result.reset_index(drop=False, inplace=True, names=["date"])
            result.rename({0: "price"}, inplace=True, axis=1)

            urea = pd.read_excel(urea_path)
            urea = urea[["date", cmdt_raw["result_name"].iloc[i]]]
            urea.rename({cmdt_raw["result_name"].iloc[i]: "price"}, inplace=True, axis=1)

            result = pd.concat([result, urea], axis=0, ignore_index=True)
            result.drop_duplicates(subset="date", keep="first", ignore_index=True, inplace=True)

            result["owner"] = [cmdt_raw["owner"].iloc[i]] * len(result)
            result["commodity"] = [cmdt_raw["commodity"].iloc[i]] * len(result)
            result["unit"] = [cmdt_raw["unit"].iloc[i]] * len(result)
            result["reference"] = [cmdt_raw["reference"].iloc[i]] * len(result)
            result["name"] = [cmdt_raw["name"].iloc[i]] * len(result)

            result.sort_values(by="date", ascending=True, ignore_index=True, inplace=True)
            complete_df = pd.concat([complete_df, result], axis=0, ignore_index=True)

        else:
            result = pd.read_excel(result_path, sheet_name=cmdt_raw["result_sheet"].iloc[i])
            result.replace("None", np.nan, inplace=True)
            result = result[result["Unnamed: 0"] == cmdt_raw["result_name"].iloc[i]]
            result.reset_index(drop=True, inplace=True)
            result = result.T
            result = result.iloc[1:, :]
            result.reset_index(drop=False, inplace=True, names=["date"])
            result.rename({0: "price"}, inplace=True, axis=1)
            result.dropna(subset="price", ignore_index=True, inplace=True)

            result["owner"] = [cmdt_raw["owner"].iloc[i]] * len(result)
            result["commodity"] = [cmdt_raw["commodity"].iloc[i]] * len(result)
            result["unit"] = [cmdt_raw["unit"].iloc[i]] * len(result)
            result["reference"] = [cmdt_raw["reference"].iloc[i]] * len(result)
            result["name"] = [cmdt_raw["name"].iloc[i]] * len(result)

            result.sort_values(by="date", ascending=True, ignore_index=True, inplace=True)
            complete_df = pd.concat([complete_df, result], axis=0, ignore_index=True)

    else:
        temp = temporary_df[temporary_df["name"] == cmdt_raw["name"].iloc[i]]
        result = pd.read_excel(result_path,
                               sheet_name=cmdt_raw["result_sheet"].iloc[i])
        result.replace("None", np.nan, inplace=True)

        result = result[result["Unnamed: 0"] == cmdt_raw["result_name"].iloc[i]]
        result.reset_index(drop=True, inplace=True)
        result = result.T
        result = result.iloc[1:, :]
        result.reset_index(drop=False, inplace=True, names=["date"])

        if cmdt_raw["result_sheet"].iloc[i] == "APAG Data":
            result = result[result["date"] >= "2023-03-06"]
            result.reset_index(drop=True, inplace=True)

        result.rename({0: "price"}, inplace=True, axis=1)
        temp_ = pd.DataFrame(temp.drop(["date", "price"], axis=1).iloc[0]).T
        temp_ = pd.concat([temp_] * len(result), axis=0, ignore_index=True)
        result = pd.concat([result, temp_], axis=1)

        temp = temp[temp["date"] < min(result["date"])]
        temp = pd.concat([temp, result], axis=0, ignore_index=True)
        temp.sort_values(by="date", ascending=True, ignore_index=True, inplace=True)

        complete_df = pd.concat([complete_df, temp], axis=0, ignore_index=True)

####################################################################################################################

complete_df["date_jalali"] = [jdatetime.datetime.fromgregorian(year=int(complete_df["date"].iloc[i][:4]),
                                                               month=int(complete_df["date"].iloc[i][5:7]),
                                                               day=int(complete_df["date"].iloc[i][8:])).strftime(
    format="%Y-%m-%d") for i in range(len(complete_df))]
complete_df = complete_df[['date', 'date_jalali', 'price', 'owner', 'commodity', 'unit', 'reference', 'name']]
complete_df["price"] = complete_df["price"].astype(dtype="float")
complete_df.sort_values(by="date", inplace=True, ignore_index=True)

max_dates = pd.read_sql("SELECT MAX(date) AS maxdate, [name] FROM [nooredenadb].[commodity].[commodities_data] "
                        "GROUP BY [name]", db_conn)
complete_df_ = complete_df.merge(max_dates, on="name", how="left")
complete_df_["maxdate"].fillna("0", inplace=True)
complete_df_ = complete_df_[complete_df_["date"] > complete_df_["maxdate"]]
complete_df_.drop(columns="maxdate", inplace=True)
complete_df_.reset_index(drop=True, inplace=True)
if len(complete_df_) > 0:
    insert_to_database(dataframe=complete_df_, database_table="[nooredenadb].[commodity].[commodities_data]")

####################################################################################################################

print("gathering OilPrice data" + "\n")
oil_price_id = ["46", "48", "144"]
csrf = rq.get("https://oilprice.com/ajax/csrf",
              # proxies=proxies
              )
hash = json.loads(csrf.text)["hash"]

oil_price_dataframe = pd.DataFrame()
for i in range(len(oil_price_id)):
    oil_price_payload = f"blend_id={oil_price_id[i]}&period=7&op_csrf_token={hash}"
    oil_price_header = {'authority': 'oilprice.com', 'accept': 'application/json, text/javascript, */*; q=0.01',
                        'accept-language': 'en-US,en;q=0.9', 'origin': 'https://oilprice.com', 'pragma': 'no-cache',
                        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'cache-control': 'no-cache',
                        'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
                        'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-origin', 'x-requested-with': 'XMLHttpRequest',
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                      "(KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"}

    oil_price = rq.post(url="https://oilprice.com/freewidgets/json_get_oilprices",
                        data=oil_price_payload,
                        headers=oil_price_header,
                        # proxies=proxies
                        )
    oil_price = json.loads(oil_price.text)
    oil_price_df = pd.DataFrame(oil_price["prices"])

    oil_price_df["time"] = pd.to_datetime(oil_price_df["time"], unit="s").dt.strftime("%Y-%m-%d")
    oil_price_df.rename({"time": "date"}, axis=1, inplace=True)
    oil_price_df.drop_duplicates(subset="date", keep="first", inplace=True, ignore_index=True)

    oil_price_df["commodity"] = [oil_price["blend"]["blend_name"]] * len(oil_price_df)
    oil_price_df["commodity"].replace({"Brent Crude": "نفت برنت", "DME Oman":  "نفت عمان",
                                       "Dubai": "نفت دبی"}, inplace=True)
    oil_price_df["owner"] = "razmehgir"
    oil_price_df["unit"] = "دلار بر بشکه"
    oil_price_df["reference"] = "OIL PRICE"
    oil_price_df["name"] = oil_price_df["commodity"] + " - " + oil_price_df["reference"] + " (" + \
                                  oil_price_df["unit"] + ")"
    oil_price_df["date_jalali"] = [
        jdatetime.datetime.fromgregorian(
            year=int(oil_price_df["date"].iloc[i][:4]), month=int(oil_price_df["date"].iloc[i][5:7]),
            day=int(oil_price_df["date"].iloc[i][8:])).strftime(format="%Y-%m-%d") for i in range(len(oil_price_df))]
    oil_price_df = oil_price_df[['date', 'date_jalali', 'price', 'owner', 'commodity', 'unit', 'reference', 'name']]
    oil_price_df["price"] = oil_price_df["price"].astype(dtype="float")
    oil_price_df.sort_values(by="date", inplace=True, ignore_index=True)

    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE reference = 'OIL PRICE' and "
                 f"commodity='{oil_price_df['commodity'].iloc[0]}' and date >= '{oil_price_df['date'].min()}'")
    crsr.close()

    sleep(1.5)

    insert_to_database(dataframe=oil_price_df, database_table="[nooredenadb].[commodity].[commodities_data]")

    sleep(random.randint(100, 401) / 100)

print("Done!")

####################################################################################################################

print("gathering SugarPrice data" + "\n")
url_sugar = "http://www.isfs.ir/nerkhejahan1.asp"
headers_sugar = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,'
                           'image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', 'Pragma': 'no-cache',
                 'Accept-Language': 'en-US,en;q=0.9', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive',
                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/124.0.0.0 Safari/537.36', 'Upgrade-Insecure-Requests': '1'}

response = rq.get(url_sugar, headers=headers_sugar)
soup = BeautifulSoup(response.text, features="html.parser")
table = soup.find('table', {"id": "table857"})
tr = table.find_all("tr")
trs = []
for t in range(1, len(tr)):
    trs.append(tr[t].text.split())
data = pd.DataFrame(data=trs, columns=["date_jalali", "date", "شکر خام نيويورک", "شکر سفيد لندن"])
data = data[:20]
data.replace("/", "-", regex=True, inplace=True)

df = pd.DataFrame()
for c in ["شکر خام نيويورک", "شکر سفيد لندن"]:
    d_max = pd.read_sql(f"SELECT MAX(date) FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity='{c}'",
                        db_conn)
    d_max = d_max[""].iloc[0]
    tmp = data[data["date"] > d_max]
    if len(tmp) > 0:
        tmp = tmp[["date", "date_jalali", c]]
        tmp.rename(mapper={c: "price"}, inplace=True, axis=1)
        tmp.dropna(subset="price", inplace=True, ignore_index=True)
        tmp["commodity"] = [c] * len(tmp)
        df = pd.concat([df, tmp], axis=0, ignore_index=True)

if len(df) > 0:
    df["owner"] = ["maleklou"] * len(df)
    df["unit"] = ["دلار بر تن"] * len(df)
    df["reference"] = ["isfs.ir"] * len(df)
    df["name"] = df["commodity"] + " - " + df["reference"] + " (" + df["unit"] + ")"
    insert_to_database(dataframe=df, database_table="[nooredenadb].[commodity].[commodities_data]")
print("Done!")

####################################################################################################################

commodities = ["اوره خلیج فارس - investing.com (دلار بر تن)", "اسلب - متال بولتن (دلار بر تن)",
               "بیلت - متال بولتن (دلار بر تن)", "متانول پلتس - چین پلتس (دلار بر تن)"]
table = pd.DataFrame()
for i in range(len(commodities)):
    df = pd.read_sql(f"SELECT [date], [price], [commodity] FROM [nooredenadb].[commodity].[commodities_data] "
                     f"where name='{commodities[i]}' ORDER BY [date]",
                     db_conn)
    tmp = pd.DataFrame(data={
        "name": [df["commodity"].iloc[0]],
        "current_price": [df["price"].iloc[-1]],
        "current_change": [df["price"].iloc[-1] - df["price"].iloc[-2]],
        "current_change_percent": [
            round(100*(df["price"].iloc[-1] - df["price"].iloc[-2])/df["price"].iloc[-2], ndigits=5)]
    })
    table = pd.concat([table, tmp], axis=0, ignore_index=True)

for i in range(len(table)):
    name_ = table['name'].iloc[i]
    crsr = db_conn.cursor()
    crsr.execute(f"UPDATE [nooredenadb].[commodity].[commodity_data_today] SET price={table['current_price'].iloc[i]}, "
                 f"price_change={table['current_change'].iloc[i]}, "
                 f"change_percent={table['current_change_percent'].iloc[i]} WHERE name='{name_}'")
    crsr.close()

####################################################################################################################

response = rq.get("https://itpnews.com/persian/")
php_session_id = response.headers["set-cookie"].split(";")[0]
itp_commodities = pd.read_sql("SELECT * FROM [nooredenadb].[commodity].[commodities_itpnews_detail_data]", db_conn)

from_d = (jdatetime.datetime.today() - jdatetime.timedelta(days=10)).strftime("%Y/%m/%d")
to_d = jdatetime.datetime.today().strftime("%Y/%m/%d")
headers_ = {"cookie": f"{php_session_id};"}
cmdty_df = pd.DataFrame()
for c in range(len(itp_commodities)):
    cmdt = itp_commodities["commodity"].iloc[c]
    id_ = itp_commodities["id"].iloc[c]
    print(f"fetching {cmdt} data")
    response_ = rq.get(f"https://itpnews.com/fa/engine/ajax/get_prices.php?type=chart&p_id={id_}&"
                       f"format=d&from_d={from_d}&to_d={to_d}")
    response_data = response_.json()
    if len(response_data) > 0:
        response_data = pd.DataFrame(data={"date_jalali": response_data["cats"], "price": response_data["avg_price"]})
        response_data["commodity"] = [cmdt] * len(response_data)
        cmdty_df = pd.concat([cmdty_df, response_data], axis=0, ignore_index=True)
    else:
        print(f"No data for {cmdt}")
    sleep(random.randint(100, 301) / 100)

month_j_n = {'فروردین': "01", 'اردیبهشت': "02", 'خرداد': "03", 'تیر': "04", 'مرداد': "05", 'شهریور': "06",
             'مهر': "07", 'آبان': "08", 'آذر': "09", 'دی': "10", 'بهمن': "11", 'اسفند': "12"}
cmdty_df["date_jalali"].replace(month_j_n, inplace=True, regex=True)
cmdty_df["date"] = [""] * len(cmdty_df)
for i in tqdm(range(len(cmdty_df))):
    date_ = jdatetime.datetime.strptime(cmdty_df["date_jalali"].iloc[i], "%m-%d-%Y")
    cmdty_df["date_jalali"].iloc[i] = date_.strftime("%Y-%m-%d")
    cmdty_df["date"].iloc[i] = date_.togregorian().strftime("%Y-%m-%d")

cmdty_df["reference"] = "itpnews"
cmdty_df["unit"] = "ریال بر واحد"
cmdty_df["name"] = cmdty_df["commodity"] + " - " + cmdty_df["reference"] + " (" + cmdty_df["unit"] + ")"
cmdty_df["owner"] = "shayan"
cmdty_df.drop_duplicates(subset=["commodity", "date"], keep="first", ignore_index=True, inplace=True)

try:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE "
                 f"date_jalali >='{from_d.replace('/', '-')}' and reference='itpnews'")
    crsr.close()
    insert_to_database(dataframe=cmdty_df, database_table="[nooredenadb].[commodity].[commodities_data]")
except Exception as e:
    print(e)

#######################################################################################################################

data = tg.get_tgju_data(symbol="گندم / بازار آمریکا").reset_index(drop=False, names="date")[["date", "Symbol", "Close"]]
data.rename({"Close": "price", "Symbol": "commodity"}, axis=1, inplace=True)
data["price"] = (data["price"] / 27.2155) * 10
data = data.merge(dim_date, on="date", how="left").sort_values(by="date", inplace=False, ignore_index=True)
data.rename(mapper={"date": "date_jalali", "Miladi": "date"}, inplace=True, axis=1)
data[["reference", "owner", "unit"]] = "tgju.org", "pirnajafi", "دلار بر تن"
data["name"] = data["commodity"] + " - " + data["reference"] + " (" + data["unit"] + ")"
data = data[data["date_jalali"] >= from_d.replace('/', '-')].reset_index(drop=True, inplace=False)
if len(data) > 0:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE "
                 f"date_jalali >='{from_d.replace('/', '-')}' and commodity='گندم / بازار آمریکا'")
    crsr.close()
    insert_to_database(dataframe=data, database_table="[nooredenadb].[commodity].[commodities_data]")

#######################################################################################################################

p = sync_playwright().start()
browser = p.chromium.launch(headless=False)
page = browser.new_page()
wci_data = None
def handle_response(response):
    global wci_data
    if "https://en.macromicro.me/charts/data/44756" in response.url:
        target_response = response
        wci_data = target_response.json()["data"]["c:44756"]
        page.wait_for_timeout(5000)
        browser.close()
page.on("response", handle_response)
try:
    page.goto("https://en.macromicro.me/charts/44756/drewry-world-container-index", wait_until="networkidle")
except Exception as e:
    print(e)

df_ = pd.DataFrame()
for i in range(9):
    name_en = wci_data["info"]["chart_config"]["seriesConfigs"][i]["name_en"]
    tmp = pd.DataFrame(data=wci_data["series"][i], columns=["date", "price"])
    tmp["commodity"] = name_en
    df_ = pd.concat([df_, tmp], axis=0, ignore_index=True)

df_["price"] = df_["price"].astype(float)
wci_rename = {'Composite': "شاخص هزینه حمل کانتینری",
              'Shanghai to Rotterdam': "هزینه حمل کانتینر از شانگهای به روتردام",
              'Shanghai to Genoa': "هزینه حمل کانتینر از شانگهای به جنوآ",
              'Shanghai to Los Angeles': "هزینه حمل کانتینر از شانگهای به لس آنجلس",
              'Shanghai to New York': "هزینه حمل کانتینر از شانگهای به نیویورک",
              'Rotterdam to Shanghai': "هزینه حمل کانتینر از روتردام به شانگهای",
              'Los Angeles to Shanghai': "هزینه حمل کانتینر از لس آنجلس به شانگهای",
              'New York to Rotterdam': "هزینه حمل کانتینر از نیویورک به روتردام",
              'Rotterdam to New York': "هزینه حمل کانتینر از روتردام به نیویورک"}

df_["commodity"].replace(wci_rename, inplace=True, regex=False)
df_ = df_.merge(dim_date.rename({"date": "date_jalali", "Miladi": "date"}, axis=1, inplace=False),
                on="date", how="left")
df_[["owner", "unit", "reference"]] = ["shayan", "دلار بر کانتینر", "macromicro.me"]
df_["name"] = df_["commodity"] + " - " + df_["reference"] + "(" + df_["unit"] + ")"

crsr = db_conn.cursor()
crsr.execute("DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE "
             "owner='shayan' AND unit='دلار بر کانتینر' AND reference='macromicro.me'")
crsr.close()
insert_to_database(dataframe=df_, database_table="[nooredenadb].[commodity].[commodities_data]")
