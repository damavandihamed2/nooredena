import pandas as pd
import requests as rq
from typing import Literal
import warnings, jdatetime

from tgju import tgju
from enigma import enigma
from utils.database import make_connection


warnings.filterwarnings("ignore")
powerbi_database = make_connection()
authenticator = [{"username": "09210882478", "password": "MMR123456"}]

query_dim_date = ("SELECT TRY_CONVERT(INT, REPLACE(Miladi, '-', '')) deven, TRY_CONVERT(VARCHAR, Miladi) date_m, "
                  "Jalali_1, REPLACE(Jalali_1, '/', '-') date FROM [nooredenadb].[extra].[dim_date]")
dim_date = pd.read_sql(query_dim_date, powerbi_database)

##################################################

query_market_option = ("SELECT TEMP1.date,TEMP1.market_trade_volume,TEMP1.market_trade_value,TEMP2.option_trade_volume,"
                       "TEMP2.option_trade_value FROM (SELECT date,SUM(trade_volume) AS market_trade_volume,"
                       "SUM(trade_value) AS market_trade_value FROM [nooredenadb].[tsetmc].[symbols_data_daily] WHERE "
                       "symbol_id NOT IN (SELECT symbol_id FROM [nooredenadb].[tsetmc].[symbols] where subsector=6812 OR"
                       " yval='380') AND yval IN (300,303,305,309,313,400,401,403,404) AND RIGHT(symbol_name_eng, 1) "
                       "IN (1, 2) GROUP BY date) AS TEMP1 JOIN (SELECT date,SUM(trade_volume) AS option_trade_volume, "
                       "SUM(trade_value) AS option_trade_value FROM [nooredenadb].[tsetmc].[symbols_data_daily] WHERE yval"
                       " IN (311,312,320,321) GROUP BY date) AS TEMP2 ON TEMP1.date=TEMP2.date")
market_option = pd.read_sql(query_market_option, powerbi_database)

query_market_option_today = ("SELECT TEMP1.date,TEMP1.market_trade_volume,TEMP1.market_trade_value,"
                             "TEMP2.option_trade_volume,TEMP2.option_trade_value FROM (SELECT date,SUM(trade_volume) AS"
                             " market_trade_volume,SUM(trade_value) AS market_trade_value FROM [nooredenadb].[tsetmc]."
                             "[symbols_data_today] WHERE symbol_id NOT IN (SELECT symbol_id FROM [nooredenadb].[tsetmc]."
                             "[symbols] where subsector=6812 OR yval='380') AND yval IN (300,303,305,309,313,400,401,"
                             "403,404) AND RIGHT(symbol_name_eng, 1) IN (1, 2) GROUP BY date) as TEMP1 JOIN (SELECT "
                             "date,SUM(trade_volume) AS option_trade_volume,SUM(trade_value) AS option_trade_value "
                             "FROM [nooredenadb].[tsetmc].[symbols_data_today] WHERE yval IN (311,312,320,321) "
                             "GROUP BY date) AS TEMP2 ON TEMP1.date=TEMP2.date")
market_option_today = pd.read_sql(query_market_option_today, powerbi_database)
market_option = pd.concat([market_option, market_option_today], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False, ignore_index=True)

##################################################

total_idx_his = pd.read_sql("SELECT date, final as total_index FROM [nooredenadb].[tsetmc].[sector_historical_data]"
                                  " where sector_id='32097828799138957' and date>='1395-01-01' order by date",
                                  powerbi_database)
total_idx_today = pd.read_sql("SELECT date, total_index FROM [nooredenadb].[tsetmc].[market_data_today]",
                                powerbi_database)
total_idx_his = pd.concat([total_idx_his, total_idx_today], axis=0, ignore_index=True
                                ).drop_duplicates(subset="date", keep="last", inplace=False, ignore_index=True)

total_eq_idx_his = pd.read_sql("SELECT date, final as total_index_equal FROM [nooredenadb].[tsetmc]."
                                        "[sector_historical_data] WHERE sector_id = '67130298613737946' and date >= "
                                        "'1395-01-01' ORDER BY date", powerbi_database)
total_eq_idx_today = pd.read_sql("SELECT date, total_index_equal FROM [nooredenadb].[tsetmc].[market_data_today]",
                                      powerbi_database)
total_eq_idx_his = pd.concat([total_eq_idx_his, total_eq_idx_today], axis=0, ignore_index=True
                                      ).drop_duplicates(subset="date", keep="last", inplace=False, ignore_index=True)

##################################################

header = {
    "accept": "application/json, text/plain, */*", "referer": "https://www.tsetmc.com/",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"', "sec-ch-ua-mobile": "?0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/143.0.0.0 Safari/537.36", "sec-ch-ua-platform": '"Windows"'}

bourse = rq.get("https://cdn.tsetmc.com/api/MarketData/GetMarketValueByFlow/1/9999", headers=header)
bourse = pd.DataFrame(bourse.json()["marketValue"]).rename({"marketCap": "bourse"}, axis=1, inplace=False)
bourse_today = rq.get("https://cdn.tsetmc.com/api/MarketData/GetMarketOverview/1", headers=header)
bourse_today = pd.DataFrame([bourse_today.json()["marketOverview"]]).rename(
    {"marketValue": "bourse", "marketActivityDEven":"deven"}, axis=1, inplace=False)[["deven", "bourse"]]
bourse = pd.concat([bourse, bourse_today], axis=0).drop_duplicates(
    subset="deven", keep="last", inplace=False).sort_values(by="deven", inplace=False, ignore_index=True)

farabourse = rq.get("https://cdn.tsetmc.com/api/MarketData/GetMarketValueByFlow/2/9999", headers=header)
farabourse = pd.DataFrame(farabourse.json()["marketValue"]).rename({"marketCap": "farabourse"}, axis=1, inplace=False)
farabourse_today = rq.get("https://cdn.tsetmc.com/api/MarketData/GetMarketOverview/2", headers=header)
farabourse_today = pd.DataFrame([farabourse_today.json()["marketOverview"]]).rename(
    {"marketValue": "farabourse", "marketActivityDEven":"deven"}, axis=1, inplace=False)[["deven", "farabourse"]]
farabourse = pd.concat([farabourse, farabourse_today], axis=0).drop_duplicates(
    subset="deven", keep="last", inplace=False).sort_values(by="deven", inplace=False, ignore_index=True)

merket_cap = bourse.merge(farabourse, on="deven", how="left")
merket_cap["market_cap"] = merket_cap["bourse"] + merket_cap["farabourse"]
merket_cap = merket_cap.merge(dim_date[["deven", "date"]], on="deven", how="left")[["date", "market_cap"]]

##################################################

repo_rate = pd.read_sql("SELECT REPLACE(date, '/', '-') AS date, repo_rate FROM "
                        "[nooredenadb].[extra].[cbi_repo_rate] ORDER BY date", powerbi_database)

##################################################

current_data = tgju.get_current_data()


print("fetching gold data")
gold = tgju.get_symbol_data_table(symbol="ons")
gold_ = gold[["date", "close_price"]].rename(
    {"close_price": "gold"}, axis=1, inplace=False)
gold_now = pd.DataFrame([current_data["current"]["ons"]])[["ts", "p"]].rename(
    {"p": "gold", "ts": "date"}, axis=1, inplace=False)
gold_now["date"] = gold_now["date"].str[:10]
gold_now["gold"] = gold_now["gold"].str.replace(",", "", regex=True).astype(float)
gold_df = pd.concat([gold_, gold_now], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False)


print("fetching bitcoin data")
bitcoin = tgju.get_symbol_data_table(symbol="crypto-bitcoin")
bitcoin_ = bitcoin[["date", "close_price"]].rename(
    {"close_price": "bitcoin"}, axis=1, inplace=False)
bitcoin_now = pd.DataFrame([current_data["current"]["crypto-bitcoin"]])[["ts", "p"]].rename(
    {"p": "bitcoin", "ts": "date"}, axis=1, inplace=False)
bitcoin_now["date"] = bitcoin_now["date"].str[:10]
bitcoin_now["bitcoin"] = bitcoin_now["bitcoin"].str.replace(",", "", regex=True).astype(float)
bitcoin_df = pd.concat([bitcoin_, bitcoin_now], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False)


print("fetching sekke bubble data")
sekke_bubble = tgju.get_symbol_data_table(symbol="sekeb_blubber")
sekke_bubble_ = sekke_bubble[["date", "close_price"]].rename(
    {"close_price": "sekke_bubble"}, axis=1, inplace=False)
sekke_bubble_now = pd.DataFrame([current_data["current"]["sekeb_blubber"]])[["ts", "p"]].rename(
    {"p": "sekke_bubble", "ts": "date"}, axis=1, inplace=False)
sekke_bubble_now["date"] = sekke_bubble_now["date"].str[:10]
sekke_bubble_now["sekke_bubble"] = sekke_bubble_now["sekke_bubble"].str.replace(",", "", regex=True).astype(float)
sekke_bubble_df = pd.concat([sekke_bubble_, sekke_bubble_now], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False)

print("fetching nim-sekke bubble data")
nim_sekke_bubble = tgju.get_symbol_data_table(symbol="nim_blubber")
nim_sekke_bubble_ = nim_sekke_bubble[["date", "close_price"]].rename(
    {"close_price": "nim_sekke_bubble"}, axis=1, inplace=False)
nim_sekke_bubble_now = pd.DataFrame([current_data["current"]["nim_blubber"]])[["ts", "p"]].rename(
    {"p": "nim_sekke_bubble", "ts": "date"}, axis=1, inplace=False)
nim_sekke_bubble_now["date"] = nim_sekke_bubble_now["date"].str[:10]
nim_sekke_bubble_now["nim_sekke_bubble"] = nim_sekke_bubble_now[
    "nim_sekke_bubble"].str.replace(",", "", regex=True).astype(float)
nim_sekke_bubble_df = pd.concat([nim_sekke_bubble_, nim_sekke_bubble_now], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False)

print("fetching rob-sekke bubble data")
rob_sekke_bubble = tgju.get_symbol_data_table(symbol="rob_blubber")
rob_sekke_bubble_ = rob_sekke_bubble[["date", "close_price"]].rename(
    {"close_price": "rob_sekke_bubble"}, axis=1, inplace=False)
rob_sekke_bubble_now = pd.DataFrame([current_data["current"]["rob_blubber"]])[["ts", "p"]].rename(
    {"p": "rob_sekke_bubble", "ts": "date"}, axis=1, inplace=False)
rob_sekke_bubble_now["date"] = rob_sekke_bubble_now["date"].str[:10]
rob_sekke_bubble_now["rob_sekke_bubble"] = rob_sekke_bubble_now[
    "rob_sekke_bubble"].str.replace(",", "", regex=True).astype(float)
rob_sekke_bubble_df = pd.concat([rob_sekke_bubble_, rob_sekke_bubble_now], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False)


print("fetching dollar data")
price_dollar_rl = tgju.get_symbol_data_table(symbol="price_dollar_rl")
price_dollar_rl_ = price_dollar_rl[["date", "close_price"]].rename(
    {"close_price": "dollar"}, axis=1, inplace=False)
price_dollar_rl_now = pd.DataFrame([current_data["current"]["price_dollar_rl"]])[["ts", "p"]].rename(
    {"p": "dollar", "ts": "date"}, axis=1, inplace=False)
price_dollar_rl_now["date"] = price_dollar_rl_now["date"].str[:10]
price_dollar_rl_now["dollar"] = price_dollar_rl_now[
    "dollar"].str.replace(",", "", regex=True).astype(float)
price_dollar_rl_df = pd.concat([price_dollar_rl_, price_dollar_rl_now], axis=0, ignore_index=True).drop_duplicates(
    subset="date", keep="last", inplace=False)

print("fetching dollar nima buy data")
nima_buy_usd = tgju.get_symbol_data_table(symbol="nima_buy_usd")
nima_buy_usd_ = nima_buy_usd[["date", "close_price"]].rename(
    {"close_price": "dollar_nima_buy"}, axis=1, inplace=False)
nima_buy_usd_df = nima_buy_usd_.drop_duplicates(subset="date", keep="last", inplace=False)

print("fetching dollar nima sell data")
nima_sell_usd = tgju.get_symbol_data_table(symbol="nima_sell_usd")
nima_sell_usd_ = nima_sell_usd[["date", "close_price"]].rename(
    {"close_price": "dollar_nima_sell"}, axis=1, inplace=False)
nima_sell_usd_df = nima_sell_usd_.drop_duplicates(subset="date", keep="last", inplace=False)

##################################################

enigma_agent = enigma.EnigmaAgent(username=authenticator[0]["username"], password=authenticator[0]["password"])
enigma_agent.login()

enigma_agent.get_dashboard()
enigma_agent.get_market_indices()

pe_df = pd.DataFrame(enigma_agent.market_indices_data["p_e_chart"])
pe_df_last = pd.DataFrame([{
    "report_date": enigma_agent.dashboard_data["dashboard_market_indices"]["report_date"].split(" ")[0],
    "total_p_e_forward": enigma_agent.dashboard_data["dashboard_market_indices"]["total_p_e_forward"],
    "total_p_e_ttm": enigma_agent.dashboard_data["dashboard_market_indices"]["total_p_e_ttm"]}])
pe_df = pd.concat([pe_df, pe_df_last], axis=0, ignore_index=True).drop_duplicates(
    subset="report_date", keep="last", ignore_index=True, inplace=False)
pe_df = pe_df.rename({"report_date": "date"}, axis=1, inplace=False)[["date", "total_p_e_ttm", "total_p_e_forward"]]

# ps_df = pd.DataFrame(enigma_agent.market_indices_data["p_s_chart"])
# ps_df_last = pd.DataFrame([{
#     "report_date": enigma_agent.dashboard_data["dashboard_market_indices"]["report_date"].split(" ")[0],
#     "total_p_s_forward": enigma_agent.dashboard_data["dashboard_market_indices"]["total_p_s_forward"],
#     "total_p_s_ttm": enigma_agent.dashboard_data["dashboard_market_indices"]["total_p_s_ttm"]}])
# ps_df = pd.concat([ps_df, ps_df_last], axis=0, ignore_index=True).drop_duplicates(
#     subset="report_date", keep="last", ignore_index=True, inplace=False)
# ps_df = ps_df.rename({"report_date": "date"}, axis=1, inplace=False)[["date", "total_p_s_ttm", "total_p_s_forward"]]

cmdty_list = [
    "سکه-بهار-آزادی-tgjuorg-ریال-بر-واحد", "شمش-آلومینیوم-investingcom-دلار-بر-تن", "شمش-روی-investingcom-دلار-بر-تن",
    "بیلت-متال-بولتن-دلار-بر-تن", "کاتد-مس-investingcom-دلار-بر-تن", "پلی-اتیلن-سبک-پلتس-خاورمیانه-دلار-بر-تن",
    "متانول-پلتس-چین-پلتس-دلار-بر-تن", "شمش-سرب-businessinsidercom-دلار-بر-تن", "زغال-سنگ-وارداتی-سی-اف-آر-چین-دلار-بر-تن",
    "اوره-خلیج-فارس-investingcom-دلار-بر-تن"]
cmdty_name_mapper = {"سکه بهار آزادی": "sekke_bahar", "شمش روی": "zinc", "شمش آلومینیوم": "aluminium", "بیلت": "billet",
                     "کاتد مس": "copper", "متانول پلتس": "methanol", "پلی اتیلن سبک پلتس": "polyethylene_light",
                     "شمش سرب": "lead", "زغال سنگ وارداتی": "coal", "اوره خلیج فارس": "urea"}
enigma_agent.get_commodities(commodities_list=cmdty_list)
cmdty_data = pd.DataFrame(enigma_agent.commodity_data["chart_data"])
cmdty_data.rename(columns={
    f"result_{enigma_agent.commodity_data["market_data"][i]["index"]}": enigma_agent.commodity_data["market_data"][i][
        "first_operand_commodity"] for i in range(len(enigma_agent.commodity_data["market_data"]))}, inplace=True)
cmdty_data.rename(columns=cmdty_name_mapper, inplace=True)

##################################################

print("feching brent oil data")
csrf = rq.get("https://oilprice.com/ajax/csrf")
oil_price_header = {
    'authority': 'oilprice.com', 'accept': 'application/json, text/javascript, */*; q=0.01', 'pragma': 'no-cache',
    'accept-language': 'en-US,en;q=0.9', 'sec-fetch-site': 'same-origin', 'x-requested-with': 'XMLHttpRequest',
    'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"', 'origin': 'https://oilprice.com',
    'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                  " Chrome/116.0.0.0 Safari/537.36", 'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'}
oil_price = rq.post(url="https://oilprice.com/freewidgets/json_get_oilprices",
                    data=f"blend_id=46&period=7&op_csrf_token={csrf.json()["hash"]}", headers=oil_price_header)
oil_brent = pd.DataFrame(oil_price.json()["prices"])
oil_brent["time"] = pd.to_datetime(oil_brent["time"], unit="s").dt.strftime("%Y-%m-%d")
oil_brent = oil_brent.drop_duplicates(subset="time", keep="first", inplace=False, ignore_index=True
                                      ).rename({"time": "date", "price": "brent_oil"}, axis=1, inplace=False)
oil_brent["brent_oil"] = oil_brent["brent_oil"].astype(float)

##################################################

query_bandar_crack = ("SELECT date_jalali as date, price crack_bandar FROM [nooredenadb].[commodity].[commodities_data]"
                      " WHERE commodity='کرک شبندر جدید'")
bandar_crack = pd.read_sql(query_bandar_crack, powerbi_database)

##################################################

query_dollar_index = ("SELECT TEMP1.date, TEMP2.dollar_index FROM (SELECT date, MAX(time) time FROM [nooredenadb]."
                      "[commodity].[commodity_data] WHERE id=700007 GROUP BY date) AS TEMP1 LEFT JOIN (SELECT date, "
                      "time, current_price dollar_index FROM [nooredenadb].[commodity].[commodity_data] WHERE "
                      "id=700007) AS TEMP2 ON TEMP1.date=TEMP2.date AND TEMP1.time=TEMP2.time")
dollar_index = pd.read_sql(query_dollar_index, powerbi_database)

##################################################

dataframe1 = pd.DataFrame(columns=["date"])
for d in [repo_rate, total_idx_his, total_eq_idx_his, merket_cap, market_option, dollar_index, bandar_crack]:
    dataframe1 = dataframe1.merge(d, on="date", how="outer")

dataframe2 = pd.DataFrame(columns=["date_m"])
for d in [pe_df, price_dollar_rl_df, nima_buy_usd_df, nima_sell_usd_df, sekke_bubble_df, nim_sekke_bubble_df,
          rob_sekke_bubble_df, gold_df, bitcoin_df, cmdty_data, oil_brent]:
    dataframe2 = dataframe2.merge(d.rename(columns={"date": "date_m"}), on="date_m", how="outer")
dataframe2 = dataframe2.merge(dim_date[["date_m", "date"]], on="date_m", how="left").drop(columns=["date_m"], inplace=False)

dataframe = pd.merge(left=dataframe1, right=dataframe2, left_index=False, right_index=False, how="outer", on="date")
dataframe.ffill(axis=0, inplace=True)

##################################################

today = jdatetime.datetime.now()

if today.weekday() in  [0, 1, 2, 3]:
    start_date = today - jdatetime.timedelta(days=(today.weekday() + 3 + 7))
    # start_date = start_date - jdatetime.timedelta(days=1)
    end_date = today - jdatetime.timedelta(days=(today.weekday() + 3))
else:
    start_date = today - jdatetime.timedelta(days=(today.weekday() + 3))
    # start_date = start_date - jdatetime.timedelta(days=1)
    end_date = today - jdatetime.timedelta(days=(today.weekday() - 4))

# end_date = today - jdatetime.timedelta(days=1)

funds_gold = pd.read_sql("SELECT symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols] "
                         "WHERE sector='68' AND yval='380' AND symbol_name LIKE '%طلا%'", powerbi_database)
funds_lev = pd.read_sql("SELECT symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols] "
                         "WHERE sector='68' AND symbol_name LIKE '%اهرم%'", powerbi_database)

def get_date_ids_price(ids: list[str], date:int, price_name:str, type: Literal["historical", "today"]):
    if type == "historical":
        df = pd.read_sql(f"SELECT symbol_id, final_price AS [{price_name}] FROM [nooredenadb].[tsetmc].[symbols_history] "
                        f"WHERE symbol_id IN {str(tuple(ids))} AND date={date}", powerbi_database)

    elif type == "today":
        df = pd.read_sql(f"SELECT symbol_id, close_price AS [{price_name}] FROM [nooredenadb].[tsetmc].[symbols_data_today]"
                         f" WHERE symbol_id IN {str(tuple(ids))}", powerbi_database)
    else:
        raise ValueError("type must be a string with value of 'historical' or 'today'")
    return df

funds_gold_start = get_date_ids_price(ids=funds_gold['symbol_id'].to_list(),
                                      date=int(start_date.togregorian().strftime("%Y%m%d")),
                                      price_name=start_date.strftime("%Y/%m/%d"),
                                      type="historical")
funds_lev_start = get_date_ids_price(ids=funds_lev['symbol_id'].to_list(),
                                      date=int(start_date.togregorian().strftime("%Y%m%d")),
                                      price_name=start_date.strftime("%Y/%m/%d"),
                                      type="historical")

if end_date == today:
    funds_gold_end = get_date_ids_price(ids=funds_gold['symbol_id'].to_list(),
                                        date=int(end_date.togregorian().strftime("%Y%m%d")),
                                        price_name=end_date.strftime("%Y/%m/%d"),
                                        type="today")
    funds_lev_end = get_date_ids_price(ids=funds_lev['symbol_id'].to_list(),
                                       date=int(end_date.togregorian().strftime("%Y%m%d")),
                                       price_name=end_date.strftime("%Y/%m/%d"),
                                       type="today")
else:
    funds_gold_end = get_date_ids_price(ids=funds_gold['symbol_id'].to_list(),
                                        date=int(end_date.togregorian().strftime("%Y%m%d")),
                                        price_name=end_date.strftime("%Y/%m/%d"),
                                        type="historical")
    funds_lev_end = get_date_ids_price(ids=funds_lev['symbol_id'].to_list(),
                                       date=int(end_date.togregorian().strftime("%Y%m%d")),
                                       price_name=end_date.strftime("%Y/%m/%d"),
                                       type="historical")


funds_gold_return = funds_gold_start.merge(funds_gold_end, on="symbol_id", how='left')
funds_gold_return["return"] = (funds_gold_return[f"{end_date.strftime("%Y/%m/%d")}"] /
                               funds_gold_return[f"{start_date.strftime("%Y/%m/%d")}"]) - 1

funds_lev_return = funds_lev_start.merge(funds_lev_end, on="symbol_id", how='left')
funds_lev_return["return"] = (funds_lev_return[f"{end_date.strftime("%Y/%m/%d")}"] /
                              funds_lev_return[f"{start_date.strftime("%Y/%m/%d")}"]) - 1


query_funds_bubble = ("SELECT symbol, symbol_id, close_price, nav FROM [nooredenadb].[tsetmc].[symbols_data_today]"
                          " WHERE yval IN ('305', '380')")
funds_bubble = pd.read_sql(query_funds_bubble, powerbi_database)
funds_bubble["bubble"] = round(((funds_bubble["close_price"] / funds_bubble["nav"]) - 1) , 4)
funds_bubble = funds_bubble[["symbol_id", "symbol", "bubble"]]

funds_lev = funds_lev_return.merge(funds_bubble, on="symbol_id", how="left")[
    ["symbol", f"{start_date.strftime("%Y/%m/%d")}", f"{end_date.strftime("%Y/%m/%d")}", "return", "bubble"]]
funds_gold = funds_gold_return.merge(funds_bubble, on="symbol_id", how="left")[
    ["symbol", f"{start_date.strftime("%Y/%m/%d")}", f"{end_date.strftime("%Y/%m/%d")}", "return", "bubble"]]
funds_gold.dropna(subset=["symbol"], inplace=True, ignore_index=True)

##################################################

indices = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[indices]", powerbi_database)

indices_start = pd.read_sql(f"SELECT indices_id, close_price as [{start_date.strftime('%Y/%m/%d')}] FROM [nooredenadb]."
                            f"[tsetmc].[indices_history] WHERE date={start_date.togregorian().strftime('%Y%m%d')}",
                            powerbi_database)

if end_date == today:
    indices_end = pd.read_sql(f"SELECT indices_id, close_price as [{end_date.strftime('%Y/%m/%d')}] FROM [nooredenadb]."
                              f"[tsetmc].[indices_data_today]", powerbi_database)
else:
    indices_end = pd.read_sql(f"SELECT indices_id, close_price as [{end_date.strftime('%Y/%m/%d')}] FROM [nooredenadb]."
                              f"[tsetmc].[indices_history] WHERE date={end_date.togregorian().strftime('%Y%m%d')}",
                              powerbi_database)

indices_return = indices_start.merge(indices_end, on="indices_id", how="left").merge(
    indices[["indices_id", "indices_name"]], on="indices_id", how="left")[[
    "indices_name", f"{start_date.strftime('%Y/%m/%d')}", f"{end_date.strftime('%Y/%m/%d')}"]]
indices_return["return"] = (indices_return[f"{end_date.strftime('%Y/%m/%d')}"] /
                            indices_return[f"{start_date.strftime('%Y/%m/%d')}"]) - 1

##################################################

week_start_date = (jdatetime.datetime.now() - jdatetime.timedelta(
    days=(3 +(jdatetime.datetime.now() - jdatetime.timedelta(days=3)).weekday())
)).strftime("%Y-%m-%d")

return_list = ['date', 'dollar_index', 'crack_bandar','gold', 'bitcoin', 'dollar', 'dollar_nima_buy',
               'dollar_nima_sell', 'sekke_bahar', 'aluminium', 'zinc', 'copper', 'lead', 'billet', 'coal',
               'polyethylene_light', 'methanol', 'urea', 'brent_oil']
dataframe_cmdty = dataframe[return_list]

today = jdatetime.datetime.now()
if today.weekday() in  [0, 1, 2, 3]:
    start_date = today - jdatetime.timedelta(days=(today.weekday() + 3 + 7))
    end_date = today - jdatetime.timedelta(days=(today.weekday() + 1))
else:
    start_date = today - jdatetime.timedelta(days=(today.weekday() + 3))
    end_date = today


last_week_cmdty = dataframe_cmdty[dataframe_cmdty["date"] <= start_date.strftime("%Y-%m-%d")]["date"].max()
last_date_cmdty = dataframe_cmdty[dataframe_cmdty["date"] <= end_date.strftime("%Y-%m-%d")]["date"].max()
# last_date_cmdty = dataframe_cmdty["date"].max()
dataframe_cmdty = dataframe_cmdty[dataframe_cmdty["date"].isin([last_week_cmdty, last_date_cmdty])]
dataframe_cmdty = dataframe_cmdty.set_index(["date"], inplace=False).T.reset_index(drop=False, names=["product"])
dataframe_cmdty["return"] = round(((dataframe_cmdty[last_week_cmdty] / dataframe_cmdty[last_date_cmdty]) - 1), 4)

##################################################

query_market_top10_return_30d = ("SELECT TOP(10) date, symbol, price_last, total_share * price_last "
                                 "AS market_cap, price_one_month, "
                                 "((try_convert(FLOAT, price_last) / try_convert(FLOAT, price_one_month)) - 1) "
                                 "[return] FROM [nooredenadb].[tsetmc].[market_return] "
                                 "WHERE symbol != 'وسرمايه' ORDER BY market_cap DESC")
market_top10_return_30d = pd.read_sql(query_market_top10_return_30d, powerbi_database)
market_top10_return_30d.sort_values(by="return", inplace=True, ignore_index=True, ascending=False)

query_indices_return_30d = ("SELECT TEMP1.*, indices.indices_name FROM (SELECT [date], [indices_id], "
                            "[close_price], [price_one_month], ((try_convert(float, [close_price]) / "
                            "try_convert(float, [price_one_month])) - 1)  AS [return] FROM [nooredenadb].[tsetmc]"
                            ".[indices_return]) TEMP1 LEFT JOIN [nooredenadb].[tsetmc].[indices] "
                            "ON indices.indices_id=TEMP1.indices_id")
indices_return_30d = pd.read_sql(query_indices_return_30d, powerbi_database)
indices_return_30d.sort_values(by="return", inplace=True, ignore_index=True, ascending=False)


query_portfolio_top10_return_30d = ("SELECT TEMP1.value,TEMP2.* FROM (SELECT TOP(10) [symbol],[amount]*[close_price] "
                                    "value FROM [nooredenadb].[portfolio].[portfolio_daily_report_diff] WHERE "
                                    "symbol!='حامی اول' ORDER BY value DESC) TEMP1 LEFT JOIN (SELECT [date],[symbol],"
                                    "[price_last],[price_one_month],((try_convert(float,[price_last])/try_convert(float"
                                    ",[price_one_month]))-1) [return] FROM [nooredenadb].[tsetmc].[market_return]) "
                                    "TEMP2 ON TEMP1.symbol=TEMP2.symbol")
portfolio_top10_return_30d = pd.read_sql(query_portfolio_top10_return_30d, powerbi_database)
portfolio_top10_return_30d.sort_values(by="return", inplace=True, ignore_index=True, ascending=False)

##################################################

dataframe = dataframe[[
    "date", "dollar", "dollar_nima_buy", "dollar_nima_sell",
    "sekke_bahar", "sekke_bubble", "nim_sekke_bubble", "rob_sekke_bubble", "gold", "bitcoin",
    "repo_rate", "total_index", "total_index_equal", "market_cap", "market_trade_volume", "market_trade_value",
    "option_trade_volume", "option_trade_value", "total_p_e_ttm", "total_p_e_forward", "dollar_index", "crack_bandar",
    "aluminium", "zinc", "copper", "lead", "billet", "coal", "polyethylene_light", "methanol", "urea", "brent_oil"
]]

now_ = jdatetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
writer = pd.ExcelWriter(f"c:/users/h.damavandi/desktop/bulletin_dataframe ({now_}).xlsx", engine="xlsxwriter")
dataframe.to_excel(writer, sheet_name="all", index=False)
funds_lev.to_excel(writer, sheet_name="funds_lev", index=False)
funds_gold.to_excel(writer, sheet_name="funds_gold", index=False)
dataframe_cmdty.to_excel(writer, sheet_name="cmdty_world", index=False)
indices_return.to_excel(writer, sheet_name="indices_return", index=False)
portfolio_top10_return_30d.to_excel(writer, sheet_name="portfolio_top10_return_30d", index=False)
market_top10_return_30d.to_excel(writer, sheet_name="market_top10_return_30d", index=False)
indices_return_30d.to_excel(writer, sheet_name="indices_return_30d", index=False)
writer.close()

##################################################
