import numpy as np
import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
from bs4 import BeautifulSoup
import json, random, warnings, jdatetime
# from playwright.sync_api import sync_playwright


from tgju import tgju
from enigma import enigma
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
authenticator = [{"username": "09210882478", "password": "MMR123456"}]
result_path = "D:/database/commodity/results.xlsx"
urea_path, gas_path, sugar_path = ("D:/database/commodity/urea.xlsx", "D:/database/commodity/gas_price.xlsx",
                                   "D:/database/commodity/sugar_price.xlsx")
proxy = "127.0.0.1:10808"
proxies = {'http': 'socks5://' + proxy,
           'https': 'socks5://' + proxy}
db_conn = make_connection()
today = jdatetime.datetime.today()
today_g = today.togregorian().strftime("%Y-%m-%d")

dim_date = pd.read_sql("SELECT TRY_CONVERT(VARCHAR, Miladi) as Miladi, REPLACE(Jalali_1, '/', '-') as date "
                       "FROM [nooredenadb].[extra].[dim_date]", db_conn)

#######################################################################################################################

gas = pd.read_excel(gas_path)
gas["date_jalali"].replace({" ": "", "/": "-"}, regex=True, inplace=True)
gas = gas[["date_jalali", "خوراک صنایع (دلار بر مترمکعب)"]].rename(
    {"خوراک صنایع (دلار بر مترمکعب)": "price"}, inplace=False, axis=1)
gas = gas.merge(dim_date.rename({"date": "date_jalali", "Miladi": "date"}, axis=1, inplace=False),
                on="date_jalali", how="left")
gas[["owner", "commodity", "unit", "reference"]] = ["razmehgir", "گاز خوراک", "دلار بر مترمکعب", "وزارت نفت"]
gas["name"] = gas["commodity"] + " - " + gas["reference"] + " (" + gas["unit"] + ")"

crsr = db_conn.cursor()
crsr.execute("DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE owner='razmehgir' AND "
             "commodity='گاز خوراک' AND unit='دلار بر مترمکعب' AND reference='وزارت نفت'")
crsr.close()
insert_to_database(dataframe=gas, database_table="[nooredenadb].[commodity].[commodities_data]")

#######################################################################################################################

query_banks_data = ("SELECT temp3.reference, temp3.price, temp3.commodity, temp3.unit, temp3.owner, temp4.date_jalali,"
                    " temp4.date FROM (SELECT [bank] AS reference, ([fiscalYear] - 621) AS fiscalYear , [fiscalMonth],"
                    " [operational_spread] as price, 'اسپرد عملیات اصلی' as commodity, 'درصد' as unit, 'pirnajafi' as "
                    "owner FROM [nooredenadb].[bourseview].[banks_data] UNION SELECT [bank] AS reference, "
                    "([fiscalYear] - 621) AS fiscalYear , [fiscalMonth], [facilities_effective_rate] as price,"
                    " 'نرخ موثر درآمد تسهیلات' as commodity, 'درصد' as unit, 'pirnajafi' as "
                    "owner FROM [nooredenadb].[bourseview].[banks_data] UNION SELECT [bank] AS reference, "
                    "([fiscalYear] - 621) AS fiscalYear , [fiscalMonth], [deposits_effective_rate] as price,"
                    " 'هزینه موثر سپرده' as commodity, 'درصد' as unit, 'pirnajafi' as owner "
                    "FROM [nooredenadb].[bourseview].[banks_data]) as temp3 LEFT JOIN "
                    "(SELECT temp1.*, temp2.date FROM (SELECT max(REPLACE(Jalali_1, '/', '-')) "
                    "AS date_jalali, try_convert(INT, jyear) as fiscalYear, jmonthN as fiscalMonth "
                    "FROM [nooredenadb].[extra].[dim_date] group by jyear, jmonthN) as temp1 LEFT JOIN "
                    "(SELECT try_convert(varchar, Miladi) as date, REPLACE(Jalali_1, '/', '-') AS date_jalali "
                    "FROM [nooredenadb].[extra].[dim_date]) as temp2 on temp1.date_jalali=temp2.date_jalali) AS temp4 "
                    "on temp3.fiscalYear=temp4.fiscalYear AND temp3.fiscalMonth=temp4.fiscalMonth")
banks_data = pd.read_sql(query_banks_data, db_conn)
banks_data["reference"].replace({"ي": "ی", "ك": "ک"}, inplace=True, regex=True)
banks_data["price"] *= 100
banks_data["name"] = banks_data["commodity"] + " - " + banks_data["reference"] + " (" + banks_data["unit"] + ")"

crsr = db_conn.cursor()
crsr.execute("DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity IN ('اسپرد عملیات اصلی', 'نرخ موثر درآمد تسهیلات', 'هزینه موثر سپرده')")
crsr.close()
insert_to_database(dataframe=banks_data, database_table="[nooredenadb].[commodity].[commodities_data]")

#######################################################################################################################

query_amar_center_data = "SELECT * FROM [nooredenadb].[economic].[commodities_amar_centre]"
amar_center_data = pd.read_sql(query_amar_center_data, db_conn)
amar_center_data["name"] = (amar_center_data["commodity"] + " - " + amar_center_data["reference"] + " (" +
                            amar_center_data["unit"] + ")")

crsr = db_conn.cursor()
crsr.execute("DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE owner='jahanshahi'")
crsr.close()
insert_to_database(dataframe=amar_center_data, database_table="[nooredenadb].[commodity].[commodities_data]")

#######################################################################################################################

enigma_agent = enigma.EnigmaAgent(username=authenticator[0]["username"], password=authenticator[0]["password"])
enigma_agent.login()

cmdt_raw = pd.read_sql("SELECT * FROM [nooredenadb].[commodity].[commodities_detail_data]", db_conn)
cmdt = cmdt_raw[cmdt_raw["id"] != "-"]
temporary_df = pd.DataFrame()
for i in tqdm(range(len(cmdt))):
    comm_names = [cmdt["retrieve_slug"].iloc[i].replace(">", "").replace("<", "")]
    enigma_agent.get_commodities(commodities_list=comm_names)
    commodity_data = enigma_agent.commodity_data
    if len(commodity_data) > 0:
        commodity_chart_data = commodity_data["chart_data"]
        commodity_market_data = commodity_data["market_data"]
        commodity_df = pd.DataFrame(commodity_chart_data)
        commodity_df = commodity_df[commodity_df["date"] <= today_g].reset_index(drop=True)
        commodity_df["owner"] = cmdt["owner"].iloc[i]
        commodity_df["commodity"] = commodity_market_data[0]["first_operand_commodity"]
        commodity_df["unit"] = commodity_market_data[0]["first_operand_unit"]
        commodity_df["reference"] = commodity_market_data[0]["first_operand_reference"]
        if ((commodity_market_data[0]["first_operand_commodity"] == "ذرت") and
                (commodity_market_data[0]["first_operand_reference"] == "investing.com")):
            commodity_df["result_1"] = commodity_df["result_1"] * ((1/100) * (39.37))
        temporary_df = pd.concat([temporary_df, commodity_df], axis=0, ignore_index=True)
    # sleep(random.randint(100, 301) / 100)
temporary_df["name"] = temporary_df["commodity"] + " - " + temporary_df["reference"] + " (" + temporary_df["unit"] + ")"
temporary_df.rename({"result_1": "price"}, axis=1, inplace=True)

#######################################################################################################################

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
            result = pd.read_excel(result_path, sheet_name=cmdt_raw["result_sheet"].iloc[i]).replace(
                "None", np.nan, inplace=False)
            result = result.set_index("Unnamed: 0", inplace=False).T.reset_index(
                drop=False, inplace=False, names=["date"])
            result = result[["date", cmdt_raw["result_name"].iloc[i]]].rename(
                {cmdt_raw["result_name"].iloc[i]: "price"}, axis=1, inplace=False)
            urea = pd.read_excel(urea_path).rename(
                {cmdt_raw["result_name"].iloc[i]: "price"}, inplace=False, axis=1)[["date", "price"]]
            result = pd.concat([result, urea], axis=0, ignore_index=True).drop_duplicates(
                subset="date", keep="first", ignore_index=True, inplace=False)
            result[["owner", "commodity", "unit", "reference", "name"]] = [
                cmdt_raw["owner"].iloc[i], cmdt_raw["commodity"].iloc[i], cmdt_raw["unit"].iloc[i],
                cmdt_raw["reference"].iloc[i], cmdt_raw["name"].iloc[i]]
            result.sort_values(by="date", ascending=True, ignore_index=True, inplace=True)
            complete_df = pd.concat([complete_df, result], axis=0, ignore_index=True)
        else:
            result = pd.read_excel(result_path, sheet_name=cmdt_raw["result_sheet"].iloc[i]).replace(
                "None", np.nan, inplace=False)
            result = result.set_index("Unnamed: 0", inplace=False).T.reset_index(
                drop=False, inplace=False, names=["date"])
            result = result[["date", cmdt_raw["result_name"].iloc[i]]].rename(
                {cmdt_raw["result_name"].iloc[i]: "price"}, axis=1, inplace=False)
            result[["owner", "commodity", "unit", "reference", "name"]] = [
                cmdt_raw["owner"].iloc[i], cmdt_raw["commodity"].iloc[i], cmdt_raw["unit"].iloc[i],
                cmdt_raw["reference"].iloc[i], cmdt_raw["name"].iloc[i]]
            result.sort_values(by="date", ascending=True, ignore_index=True, inplace=True)
            complete_df = pd.concat([complete_df, result], axis=0, ignore_index=True)
    else:

        temp = temporary_df[temporary_df["name"] == cmdt_raw["name"].iloc[i]]
        result = pd.read_excel(result_path, sheet_name=cmdt_raw["result_sheet"].iloc[i]).replace(
            "None", np.nan, inplace=False)
        result = result.set_index("Unnamed: 0", inplace=False).T.reset_index(
            drop=False, inplace=False, names=["date"])
        result = result[["date", cmdt_raw["result_name"].iloc[i]]].rename(
            {cmdt_raw["result_name"].iloc[i]: "price"}, axis=1, inplace=False)
        if cmdt_raw["result_sheet"].iloc[i] == "APAG Data":
            result = result[result["date"] >= "2023-03-06"].reset_index(drop=True, inplace=False)
        result[["owner", "commodity", "unit", "reference", "name"]] = [
            cmdt_raw["owner"].iloc[i], cmdt_raw["commodity"].iloc[i], cmdt_raw["unit"].iloc[i],
            cmdt_raw["reference"].iloc[i], cmdt_raw["name"].iloc[i]]

        temp = pd.concat([temp, result], axis=0, ignore_index=True)
        temp = temp.drop_duplicates(subset="date", keep="last", inplace=False).sort_values(
            by="date", ascending=True, ignore_index=True, inplace=False)

        complete_df = pd.concat([complete_df, temp], axis=0, ignore_index=True)

####################################################################################################################

complete_df = complete_df.merge(
    dim_date.rename({"date": "date_jalali", "Miladi": "date"}, axis=1, inplace=False
                    ), on="date", how="left").sort_values(by="date", inplace=False, ignore_index=True)
complete_df["price"] = complete_df["price"].astype(dtype="float")

max_dates = pd.read_sql("SELECT MAX(date) AS maxdate, [name] FROM [nooredenadb].[commodity].[commodities_data] "
                        "GROUP BY [name]", db_conn)
complete_df = complete_df.merge(max_dates, on="name", how="left")
complete_df["maxdate"].fillna("0", inplace=True)

complete_df = complete_df[complete_df["date"] > complete_df["maxdate"]].drop(
    columns="maxdate", inplace=False).reset_index(drop=True, inplace=False)
if len(complete_df) > 0:
    insert_to_database(dataframe=complete_df, database_table="[nooredenadb].[commodity].[commodities_data]")

####################################################################################################################

print("gathering OilPrice data" + "\n")
oil_price_id = ["46", "48", "144"]
csrf = rq.get("https://oilprice.com/ajax/csrf", proxies=proxies)
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
                        data=oil_price_payload, headers=oil_price_header, proxies=proxies)
    oil_price_df = pd.DataFrame(oil_price.json()["prices"])
    oil_price_df["time"] = pd.to_datetime(oil_price_df["time"], unit="s").dt.strftime("%Y-%m-%d")
    oil_price_df = oil_price_df.drop_duplicates(
        subset="time", keep="first", inplace=False, ignore_index=True).rename({"time": "date"}, axis=1, inplace=False)
    if (oil_price.json()["blend"]["blend_name"] == "Dubai") and ("2025-03-24" in oil_price_df["date"].values):
        idx_ = oil_price_df.index[oil_price_df["date"] == "2025-03-24"].values[0]
        if oil_price_df["price"].iloc[idx_] == "13.554":
            oil_price_df["price"].iloc[idx_] = np.nan
    oil_price_df["commodity"] = oil_price.json()["blend"]["blend_name"]
    oil_price_dataframe = pd.concat([oil_price_dataframe, oil_price_df], axis=0, ignore_index=True)
    sleep(random.randint(100, 301) / 100)
oil_price_dataframe.dropna(subset=["price"], inplace=True, ignore_index=True)
oil_price_dataframe["price"] = oil_price_dataframe["price"].astype(dtype="float")
oil_price_dataframe["commodity"].replace(
    {"Brent Crude": "نفت برنت", "DME Oman":  "نفت عمان", "Dubai": "نفت دبی"}, inplace=True)
oil_price_dataframe[["owner", "unit", "reference"]] = ["razmehgir", "دلار بر بشکه", "OIL PRICE"]
oil_price_dataframe["name"] = (oil_price_dataframe["commodity"] + " - " + oil_price_dataframe["reference"] +
                               " (" + oil_price_dataframe["unit"] + ")")
oil_price_dataframe = oil_price_dataframe.merge(
    dim_date.rename({"date": "date_jalali", "Miladi": "date"}, axis=1, inplace=False), on="date", how="left"
).sort_values(by="date", inplace=False, ignore_index=True)

oil_price_dataframe_mindate = oil_price_dataframe[["date", "name"]].groupby(by="name", as_index=False).min()
query_oilprice = ""
for i in range(len(oil_price_dataframe_mindate)):
    query_oilprice += (f"(name='{oil_price_dataframe_mindate["name"].iloc[i]}' AND "
                       f"date>='{oil_price_dataframe_mindate["date"].iloc[i]}') OR ")
crsr = db_conn.cursor()
crsr.execute("DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE " + query_oilprice[: -4])
crsr.close()
insert_to_database(dataframe=oil_price_dataframe, database_table="[nooredenadb].[commodity].[commodities_data]")
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
data = data[:20].replace("/", "-", regex=True, inplace=False)

data = pd.melt(data, id_vars=["date_jalali", "date"], var_name="commodity", value_name="price")
data[["owner", "unit", "reference"]] = ["maleklou", "دلار بر تن", "isfs.ir"]
data["name"] = data["commodity"] + " - " + data["reference"] + " (" + data["unit"] + ")"

crsr = db_conn.cursor()
crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity "
             f"IN ('شکر خام نيويورک', 'شکر سفيد لندن') AND owner='maleklou' AND "
             f"reference='isfs.ir' AND date>='{data['date'].min()}'")
crsr.close()
insert_to_database(dataframe=data, database_table="[nooredenadb].[commodity].[commodities_data]")
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
# php_session_id = response.headers["set-cookie"].split(";")[0]
itp_commodities = pd.read_sql("SELECT * FROM [nooredenadb].[commodity].[commodities_itpnews_detail_data]", db_conn)

from_d = (jdatetime.datetime.today() - jdatetime.timedelta(days=10)).strftime("%Y/%m/%d")
to_d = jdatetime.datetime.today().strftime("%Y/%m/%d")
headers_ = {
    # "cookie": f"{php_session_id};"
}
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
cmdty_df["date"] = ""
for i in tqdm(range(len(cmdty_df))):
    date_ = jdatetime.datetime.strptime(cmdty_df["date_jalali"].iloc[i], "%m-%d-%Y")
    cmdty_df["date_jalali"].iloc[i] = date_.strftime("%Y-%m-%d")
    cmdty_df["date"].iloc[i] = date_.togregorian().strftime("%Y-%m-%d")

cmdty_df[["owner", "reference", "unit"]] = ["shayan", "itpnews", "ریال بر واحد"]
cmdty_df["name"] = cmdty_df["commodity"] + " - " + cmdty_df["reference"] + " (" + cmdty_df["unit"] + ")"
cmdty_df.drop_duplicates(subset=["commodity", "date"], keep="first", ignore_index=True, inplace=True)

try:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE "
                 f"date_jalali>='{from_d.replace('/', '-')}' AND reference='itpnews'")
    crsr.close()
    insert_to_database(dataframe=cmdty_df, database_table="[nooredenadb].[commodity].[commodities_data]")
except Exception as e:
    print(e)

#######################################################################################################################

from_d = (jdatetime.datetime.today() - jdatetime.timedelta(days=60)).strftime("%Y-%m-%d")

data = tgju.get_symbol_data_table(symbol="commodities-us-wheat")
data = data[["date", "close_price"]].rename({"date": "Miladi", "close_price": "price"}, axis=1, inplace=False)
data["commodity"] = "گندم / بازار آمریکا"
data["price"] = (data["price"] / 27.2155) * 10
data = data.merge(dim_date, on="Miladi", how="left").sort_values(by="date", inplace=False, ignore_index=True)
data.rename(mapper={"date": "date_jalali", "Miladi": "date"}, inplace=True, axis=1)
data[["reference", "owner", "unit"]] = ["tgju.org", "pirnajafi", "دلار بر تن"]
data["name"] = data["commodity"] + " - " + data["reference"] + " (" + data["unit"] + ")"
data = data[data["date_jalali"] >= from_d].reset_index(drop=True, inplace=False)

if len(data) > 0:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE "
                 f"date_jalali >='{from_d}' AND commodity='گندم / بازار آمریکا'")
    crsr.close()
    insert_to_database(dataframe=data, database_table="[nooredenadb].[commodity].[commodities_data]")

#######################################################################################################################

# main_headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
#                               "Chrome/135.0.0.0 Safari/537.36"}
# main_url = ("https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/"
#             "world-container-index-assessed-by-drewry")
# main_page = rq.get(main_url, headers=main_headers, proxies=proxies)
# soup = BeautifulSoup(main_page.text, features="html.parser")
# iframes = soup.find_all('iframe')
# chartblocks_iframes = [iframe for iframe in iframes if iframe.get('src') and "chartblocks" in iframe.get('src')]
# chartblocks_srcs = [iframe.get('src') for iframe in chartblocks_iframes]
#
# wci_df = pd.DataFrame()
# for src in range(len(chartblocks_srcs)):
#     url = "https://api.chartblocks.com/v1/chart/data/" + chartblocks_srcs[src].split("?c=")[1].replace("&", "?")
#     res = rq.get(url, headers=main_headers, proxies=proxies)
#     res = res.json()["data"]["series"]
#     for n_ in res.keys():
#         if "raw" in res[n_].keys():
#             temp = pd.DataFrame(res[n_]["values"])
#             temp["commodity"] = n_
#             wci_df = pd.concat([wci_df, temp], axis=0, ignore_index=True)
#
# wci_df["x"] = wci_df["x"].apply(lambda x: jdatetime.datetime.fromtimestamp(x/1e3).strftime('%Y-%m-%d'))
# wci_df.rename({"y": "price", "x": "date"}, axis=1, inplace=True)
# wci_df = wci_df.merge(dim_date, on="date", how="left").rename(
#     columns={"Miladi": "date", "date": "date_jalali"}, inplace=False)
#
# wci_rename = {"ds-0": "هزینه حمل کانتینر از شانگهای به روتردام", "ts-3156": "هزینه حمل کانتینر از شانگهای به لس آنجلس",
#               "ts-4385": "هزینه حمل کانتینر از شانگهای به جنوآ", "ts-7202": "هزینه حمل کانتینر از شانگهای به نیویورک",
#               "ts-12079": "شاخص هزینه حمل کانتینری"}
# wci_df["commodity"].replace(wci_rename, inplace=True)
# wci_df[["owner", "unit", "reference"]] = ["shayan", "دلار بر کانتینر", "macromicro.me"]
# wci_df["name"] = wci_df["commodity"] + " - " + wci_df["reference"] + " (" + wci_df["unit"] + ")"
#
# wci_last_date = pd.read_sql("SELECT MAX(date) as date FROM [nooredenadb].[commodity].[commodities_data] WHERE "
#                             "owner='shayan' AND unit='دلار بر کانتینر' AND reference='macromicro.me'",
#                             db_conn)["date"].iloc[0]
# wci_df = wci_df[wci_df["date"] > wci_last_date].reset_index(drop=True, inplace=False)
# if len(wci_df) > 0:
#     insert_to_database(dataframe=wci_df, database_table="[nooredenadb].[commodity].[commodities_data]")
#
# p = sync_playwright().start()
# browser = p.chromium.launch(headless=True)
# page = browser.new_page()
# wci_data = None
# def handle_response(response):
#     global wci_data
#     if "https://en.macromicro.me/charts/data/44756" in response.url:
#         target_response = response
#         wci_data = target_response.json()["data"]["c:44756"]
#         page.wait_for_timeout(5000)
#         browser.close()
# page.on("response", handle_response)
# try:
#     page.goto("https://en.macromicro.me/charts/44756/drewry-world-container-index", wait_until="networkidle")
# except Exception as e:
#     print(e)
#
# df_ = pd.DataFrame()
# for i in range(9):
#     name_en = wci_data["info"]["chart_config"]["seriesConfigs"][i]["name_en"]
#     tmp = pd.DataFrame(data=wci_data["series"][i], columns=["date", "price"])
#     tmp["commodity"] = name_en
#     df_ = pd.concat([df_, tmp], axis=0, ignore_index=True)
#
# df_["price"] = df_["price"].astype(float)
# wci_rename = {'Composite': "شاخص هزینه حمل کانتینری",
#               'Shanghai to Rotterdam': "هزینه حمل کانتینر از شانگهای به روتردام",
#               'Shanghai to Genoa': "هزینه حمل کانتینر از شانگهای به جنوآ",
#               'Shanghai to Los Angeles': "هزینه حمل کانتینر از شانگهای به لس آنجلس",
#               'Shanghai to New York': "هزینه حمل کانتینر از شانگهای به نیویورک",
#               'Rotterdam to Shanghai': "هزینه حمل کانتینر از روتردام به شانگهای",
#               'Los Angeles to Shanghai': "هزینه حمل کانتینر از لس آنجلس به شانگهای",
#               'New York to Rotterdam': "هزینه حمل کانتینر از نیویورک به روتردام",
#               'Rotterdam to New York': "هزینه حمل کانتینر از روتردام به نیویورک"}
#
# df_["commodity"].replace(wci_rename, inplace=True, regex=False)
# df_ = df_.merge(dim_date.rename(
#     {"date": "date_jalali", "Miladi": "date"}, axis=1, inplace=False), on="date", how="left")
# df_[["owner", "unit", "reference"]] = ["shayan", "دلار بر کانتینر", "macromicro.me"]
# df_["name"] = df_["commodity"] + " - " + df_["reference"] + " (" + df_["unit"] + ")"
#
# crsr = db_conn.cursor()
# crsr.execute("DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE "
#              "owner='shayan' AND unit='دلار بر کانتینر' AND reference='macromicro.me'")
# crsr.close()
# insert_to_database(dataframe=df_, database_table="[nooredenadb].[commodity].[commodities_data]")
