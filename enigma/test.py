import pandas as pd
from tqdm import tqdm
from enigma.enigma import EnigmaAgent
from utils.database import insert_to_database, make_connection

db_conn = make_connection()
authenticator = [
    {"username": "09210882478", "password": "MMR123456"},
    {"username": "09021003706", "password": "1003706Ali"},
]
enigma_agent = EnigmaAgent(
    username=authenticator[1]["username"],
    password=authenticator[1]["password"])
enigma_agent.login(use_old_token=False)

# enigma_agent.get_watchlist()
# watchlist_data = enigma_agent.watchlist_data
# watchlist_data = watchlist_data["initial_data"]
#
# data = []
# for k in watchlist_data:
#     if not watchlist_data[k]["symbol"][-1] == "ح":
#         data.append(
#             watchlist_data[k]
#         )
# data_df = pd.DataFrame(data)
# data_df = data_df[["symbol", "name", "symbol_12chars", "industry_code", "industry", "market_name"]]
# data_df.to_pickle("c:/users/h.damavandi/desktop/data_df.pkl")

data_df = pd.read_pickle("c:/users/h.damavandi/desktop/data_df.pkl")
search_list = []
for i in tqdm(range(len(data_df))):
    s, s_id = data_df["symbol"].iloc[i], data_df["symbol_12chars"].iloc[i]
    c_ = 1
    while True:
        if c_ > 5:
            break
        try:
            enigma_agent.search(symbol=s)
            s_d = enigma_agent.search_data
            s_d_ = [d for d in s_d if d["symbol_12chars"] == s_id][0]
            s_d_ = {"symbol_12chars": s_d_["symbol_12chars"], "id": s_d_["id"],
                    "market_id": s_d_["market"]["id"], "market_name": s_d_["market"]["name"]}
            search_list.append(s_d_)
            break
        except IndexError as ie:
            print(ie, "\n", s, "\n", s_d)
            c_ += 1
        except Exception as e:
            print(e)
            enigma_agent._refresh_access_token()
            c_ += 1

# search_df = pd.DataFrame(search_list)
# search_df.to_pickle("c:/users/h.damavandi/desktop/search_df.pkl")

search_df = pd.read_pickle("c:/users/h.damavandi/desktop/search_df.pkl")
df = data_df.drop(columns=["market_name"], inplace=False).merge(search_df, on="symbol_12chars", how="left")


markets = df.groupby(by=["market_id", "market_name"], as_index=False).size().drop(
    columns="size", inplace=False).rename({"market_id": "id", "market_name": "marketName"}, axis=1, inplace=False)
industries = df.groupby(by=["industry_code", "industry"], as_index=False).size().drop(
    columns="size", inplace=False).rename({"industry_code": "id", "industry": "industryName"}, axis=1, inplace=False)
df.drop(columns=["industry", "market_name"], inplace=True)
df.rename(
    {"name": "symbolName", "symbol_12chars": "symbolCode", "industry_code": "industryId", "market_id": "marketId"},
    axis=1, inplace=True
)

insert_to_database(markets, "[nooredenadb].[enigma].[markets]")
insert_to_database(industries, "[nooredenadb].[enigma].[industries]")
insert_to_database(df, "[nooredenadb].[enigma].[symbols]")





data_df = pd.read_sql("SELECT * FROM [nooredenadb].[enigma].[symbols] WHERE id IS NULL", db_conn)
search_list = []
for i in tqdm(range(len(data_df))):
    s, s_id = data_df["symbol"].iloc[i], data_df["symbolCode"].iloc[i]
    c_ = 1
    while True:
        if c_ > 5:
            break
        try:
            enigma_agent.search(symbol=s)
            s_d = enigma_agent.search_data
            s_d_ = [d for d in s_d if d["symbol_12chars"] == s_id][0]
            s_d_ = {"symbolCode": s_d_["symbol_12chars"],
                    "id": s_d_["id"],
                    "marketId": s_d_["market"]["id"]}
            search_list.append(s_d_)
            break
        except IndexError as ie:
            print(ie, "\n", s, "\n", s_d)
            c_ += 1
        except Exception as e:
            print(e)
            enigma_agent._refresh_access_token()
            c_ += 1

search_df = pd.DataFrame(search_list)
search_df.to_pickle("c:/users/h.damavandi/desktop/search_df.pkl")

crsr = db_conn.cursor()
for i in tqdm(range(len(search_df))):
    code = search_df["symbolCode"].iloc[i]
    id_ = search_df["id"].iloc[i]
    market_id = search_df["marketId"].iloc[i]
    crsr.execute(f"UPDATE [nooredenadb].[enigma].[symbols] SET id = '{id_}' , marketId = {market_id} WHERE symbolCode='{code}'")
crsr.close()


##################################################
##################################################
##################################################
##################################################
##################################################


import pandas as pd
from tqdm import tqdm
from enigma.enigma import EnigmaAgent
from utils.database import insert_to_database, make_connection

db_conn = make_connection()
authenticator = [
    {"username": "09210882478", "password": "MMR123456"},
    {"username": "09021003706", "password": "1003706Ali"},
]
enigma_agent = EnigmaAgent(
    username=authenticator[1]["username"],
    password=authenticator[1]["password"])
enigma_agent.login(use_old_token=False)

# enigma_agent.get_watchlist()
# watchlist_data = enigma_agent.watchlist_data
# watchlist_data = watchlist_data["initial_data"]
#
# data = []
# for k in watchlist_data:
#     if not watchlist_data[k]["symbol"][-1] == "ح":
#         data.append(
#             watchlist_data[k]
#         )
# data_df = pd.DataFrame(data)
# data_df = data_df[["symbol", "name", "symbol_12chars", "industry_code", "industry", "market_name"]]
# data_df.to_pickle("c:/users/h.damavandi/desktop/data_df.pkl")

data_df = pd.read_pickle("c:/users/h.damavandi/desktop/data_df.pkl")
search_list = []
for i in tqdm(range(len(data_df))):
    s, s_id = data_df["symbol"].iloc[i], data_df["symbol_12chars"].iloc[i]
    c_ = 1
    while True:
        if c_ > 5:
            break
        try:
            enigma_agent.search(symbol=s)
            s_d = enigma_agent.search_data
            s_d_ = [d for d in s_d if d["symbol_12chars"] == s_id][0]
            s_d_ = {"symbol_12chars": s_d_["symbol_12chars"], "id": s_d_["id"],
                    "market_id": s_d_["market"]["id"], "market_name": s_d_["market"]["name"]}
            search_list.append(s_d_)
            break
        except IndexError as ie:
            print(ie, "\n", s, "\n", s_d)
            c_ += 1
        except Exception as e:
            print(e)
            enigma_agent._refresh_access_token()
            c_ += 1

# search_df = pd.DataFrame(search_list)
# search_df.to_pickle("c:/users/h.damavandi/desktop/search_df.pkl")

search_df = pd.read_pickle("c:/users/h.damavandi/desktop/search_df.pkl")
df = data_df.drop(columns=["market_name"], inplace=False).merge(search_df, on="symbol_12chars", how="left")


markets = df.groupby(by=["market_id", "market_name"], as_index=False).size().drop(
    columns="size", inplace=False).rename({"market_id": "id", "market_name": "marketName"}, axis=1, inplace=False)
industries = df.groupby(by=["industry_code", "industry"], as_index=False).size().drop(
    columns="size", inplace=False).rename({"industry_code": "id", "industry": "industryName"}, axis=1, inplace=False)
df.drop(columns=["industry", "market_name"], inplace=True)
df.rename(
    {"name": "symbolName", "symbol_12chars": "symbolCode", "industry_code": "industryId", "market_id": "marketId"},
    axis=1, inplace=True
)

insert_to_database(markets, "[nooredenadb].[enigma].[markets]")
insert_to_database(industries, "[nooredenadb].[enigma].[industries]")
insert_to_database(df, "[nooredenadb].[enigma].[symbols]")





data_df = pd.read_sql("SELECT * FROM [nooredenadb].[enigma].[symbols] WHERE id IS NULL", db_conn)
search_list = []
for i in tqdm(range(len(data_df))):
    s, s_id = data_df["symbol"].iloc[i], data_df["symbolCode"].iloc[i]
    c_ = 1
    while True:
        if c_ > 5:
            break
        try:
            enigma_agent.search(symbol=s)
            s_d = enigma_agent.search_data
            s_d_ = [d for d in s_d if d["symbol_12chars"] == s_id][0]
            s_d_ = {"symbolCode": s_d_["symbol_12chars"],
                    "id": s_d_["id"],
                    "marketId": s_d_["market"]["id"]}
            search_list.append(s_d_)
            break
        except IndexError as ie:
            print(ie, "\n", s, "\n", s_d)
            c_ += 1
        except Exception as e:
            print(e)
            enigma_agent._refresh_access_token()
            c_ += 1

search_df = pd.DataFrame(search_list)
search_df.to_pickle("c:/users/h.damavandi/desktop/search_df.pkl")

crsr = db_conn.cursor()
for i in tqdm(range(len(search_df))):
    code = search_df["symbolCode"].iloc[i]
    id_ = search_df["id"].iloc[i]
    market_id = search_df["marketId"].iloc[i]
    crsr.execute(f"UPDATE [nooredenadb].[enigma].[symbols] SET id = '{id_}' , marketId = {market_id} WHERE symbolCode='{code}'")
crsr.close()
import random
import requests
import pandas as pd
from tqdm import tqdm

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzgwNzYwMzc3LCJqdGkiOiI1ZTRkMzg0N2Q5NTU0NjZiODc0NmU3OWFlYTYxN2Q5ZCIsInVzZXJfaWQiOjUxNTg1fQ.KKid2nZICpJYFrJR95KppzCvdCnP5tQI8Wj3KHHG7a0"
headers = {
    'accept': 'application/json', 'accept-language': 'fa', 'app-name': 'panel', 'referer': 'https://panel.enigma.ir/',
    'authorization': f'Bearer {token}', 'origin': 'https://panel.enigma.ir', 'pragma': 'no-cache', 'priority': 'u=1, i',
    'sec-ch-ua': '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"', 'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/148.0.0.0 Safari/537.36', 'cache-control': 'no-cache'}

def search_symbol(symbol: str):
    params = {'search': symbol, 'security_type': ['1', '4', '10', '5']}
    response_search = requests.get('https://core.enigma.ir/api/v1/securities/search/', params=params, headers=headers)
    return response_search

response_all = requests.get('https://core.live.enigma.ir/api/v1/market_watch/watchlist/all/', headers=headers)
all_stocks = response_all.json()["initial_data"]
all_stocks = pd.DataFrame([all_stocks[k] for k in all_stocks.keys()])
all_stocks = all_stocks[
    (all_stocks["industry_code"].isin([43, 53])) & (all_stocks["symbol"].str[-1] != "ح")
].reset_index(drop=True, inplace=False)

symbols_enigma_id = []
for s in tqdm(range(len(all_stocks))):
    try:
        s_ = all_stocks['symbol'].iloc[s]
        s_id = all_stocks['symbol_12chars'].iloc[s]
        s_ = s_.replace("ك", "ک").replace("ي", "ی")
        resp = search_symbol(symbol=s_)
        d = resp.json()["data"]
        d = [d_ for d_ in d if d_.get("symbol_12chars") == s_id]
        symbols_enigma_id.append(d[0])
    except Exception as e: print(e, "\n", s_)

l_ = ["دتهران", "دحاوی", "ریشمک", "سفید", "درازک", "داکسیر"]
for s in tqdm(range(len(l_))):
    resp = search_symbol(symbol=l_[s])
    d = resp.json()["data"][0]
    print(d)
    symbols_enigma_id.append(d)

df_list = []
for t in range(len(symbols_enigma_id)):
    tmp = symbols_enigma_id[t]
    tmp["market"] = tmp.get("market").get("name")
    tmp["industry"] = tmp.get("industry").get("name")
    df_list.append(tmp)

# all_stocks = pd.DataFrame(df_list)
# all_stocks.drop_duplicates(inplace=True, ignore_index=True)
# all_stocks.to_pickle("c:/users/h.damavandi/desktop/all_stocks.pkl")
all_stocks = pd.read_pickle("c:/users/h.damavandi/desktop/all_stocks.pkl")

# shareholders = []
# for i in tqdm(range(len(all_stocks))):
#     symbol = all_stocks["symbol"].iloc[i]
#     asset_id = all_stocks["id"].iloc[i]
#     response_symbol = requests.get(url=f'https://core.enigma.ir/api/v1/shareholders/security/{asset_id}/combined/', headers=headers)
#     data = response_symbol.json()["data"]
#     l = [{"symbol": symbol, "name": d.get("name"), "volume": d.get("volume"), "value": d.get("value"), "percent": d.get("percent")} for d in data]
#     shareholders += l
#
# shareholders_df = pd.DataFrame(shareholders)
# shareholders_df.to_pickle("c:/users/h.damavandi/desktop/shareholders.pkl")
shareholders_df = pd.read_pickle("c:/users/h.damavandi/desktop/shareholders.pkl")

shareholders_df = shareholders_df.merge(
    all_stocks.drop_duplicates(subset="symbol", keep="first", inplace=False)[["symbol", "industry"]],
    on="symbol", how="left")
shareholders_df.to_excel("c:/users/h.damavandi/desktop/shareholders.xlsx", index=False)

shareholders_df_ = shareholders_df[["symbol", "percent"]].groupby(["symbol"], as_index=False).max()
shareholders_df_ = shareholders_df_.merge(shareholders_df, on=["symbol", "percent"], how="left")
shareholders_df__ = shareholders_df_.merge(
    all_stocks.drop_duplicates()[["symbol", "name", "industry"]].rename({"name": "symbol_name"}, axis=1, inplace=False),
    on=["symbol"], how="left",
)

shareholders_df__.to_excel("c:/users/h.damavandi/desktop/shareholders_new.xlsx", index=False)

shareholders_df_cement = shareholders_df[shareholders_df["industry"] == "سیمان، آهک و گچ"]
shareholders_df_drug = shareholders_df[shareholders_df["industry"] == "مواد و محصولات دارویی"]
shareholders_df_drug.to_excel("c:/users/h.damavandi/desktop/shareholders_drug.xlsx", index=False)

shareholders_df_cement["symbol"].nunique()
shareholders_df_drug["symbol"].nunique()



