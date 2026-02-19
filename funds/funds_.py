import re
import random
import warnings
import numpy as np
import pandas as pd
import yagooglesearch
from tqdm import tqdm
from time import sleep
from googlesearch import search

from utils.database import make_connection


def search_for_symbol(text: str):
    search_generator = search(query=text + " tse", safe="off", stop=20)
    results_list = []
    for rs in search_generator:
        results_list.append(rs)
        if "tsetmc" in rs:
            break
        # sleep(random.randint(10, 31) / 10)
    results_data = pd.DataFrame(data={"result": results_list})
    return results_data


def id_extract(my_string, pattern):
    ref = re.findall(pattern, my_string)
    if len(ref) == 0:
        return None
    else:
        ref = set(ref[0])
        ref.remove("")
        ref = list(ref)
        return ref[0]


warnings.filterwarnings("ignore")
db_conn = make_connection()
symbols = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols] WHERE active=1", db_conn)

patterns = ["ParTree=151311&i=([0-9]{1,})", "instInfo/([0-9]{1,})", "InstInfo/([0-9]{1,})"]
combined = "|".join(patterns)

funds_trade = pd.read_excel("C:/Users/damavandi/Desktop/funds template.xlsx", sheet_name="trades")
funds_portfolio = pd.read_excel("C:/Users/damavandi/Desktop/funds template.xlsx", sheet_name="portfolio")
# funds_portfolio_ = funds_portfolio.merge(funds_trade.drop(["type", "portfolio_value"], axis=1, inplace=False), on=["date", "fund", "symbol"], how="outer")

syms = pd.DataFrame(data={"name": list(set(funds_trade["symbol"].tolist() + funds_portfolio["symbol"].tolist()))})
syms["name"].replace({"ک": "ك", "ی": "ي", r"\u200c": r"\\u200c"}, regex=True, inplace=True)
syms = syms.merge(symbols[["symbol", "symbol_name", "symbol_id"]].rename({"symbol_name": "name"}, axis=1, inplace=False), on="name", how="left")

syms_find = syms[~syms["symbol"].isin([np.nan])].reset_index(drop=True, inplace=False)
syms_notfind = syms[syms["symbol"].isin([np.nan])].reset_index(drop=True, inplace=False)

syms_notfind["name_"] = syms_notfind["name"].replace({" ": ""}, inplace=False, regex=True)
syms_notfind = syms_notfind[["name", "name_"]].merge(
    symbols[["symbol", "symbol_name", "symbol_id"]].rename({"symbol_name": "name_"}, axis=1, inplace=False).replace(
        {"name_": {r"\\u200c": "", " ": ""}}, regex=True, inplace=False), on="name_", how="left")

syms_find = pd.concat([syms_find, syms_notfind.dropna(subset="symbol", inplace=False).drop("name_", axis=1, inplace=False)], axis=0, ignore_index=True)
syms_notfind = syms_notfind[syms_notfind["symbol"].isna()].reset_index(drop=True, inplace=False).drop("name_", axis=1, inplace=False)


for s in tqdm(range(len(syms_notfind))):
    try:
        result = search_for_symbol(syms_notfind["name"].iloc[s])
    except Exception as e:
        print(e)
    else:
        result["query"] = result["result"].str.contains("tsetmc")
        result = result[result["query"]].reset_index(drop=True, inplace=False)
        if len(result) > 0:
            id_ = id_extract(my_string=result["result"].iloc[0], pattern=combined)
            syms_notfind["symbol_id"].iloc[s] = id_
        else:
            pass
    finally:
        sleep(random.randint(3, 101)/10)


# syms_find = pd.concat([syms_find, syms_notfind[~syms_notfind["symbol_id"].isna()]], axis=0, ignore_index=True)
# syms_notfind = syms_notfind[syms_notfind["symbol_id"].isna()]





syms_notfind_ = syms_notfind[["name", "symbol_id"]].merge(symbols[["symbol", "symbol_name", "symbol_id"]], on="symbol_id", how="left")

"ح . "
"ح . صنايع\\u200c بهشهر(هلدينگ"
"ح . صنايع‌ بهشهر(هلدينگ"

"ح . فجر انرژي خليج فارس"

"ح "
"ح صنايع ماديران"


"ح."
"ح.فولاد آلياژي ايران"


"حق تقدم"
"سنگ آهن گل گهر (حق تقدم) "
"هلدينگ توسعه صنايع بهشهر (حق تقدم) "


####################################################
####################################################
####################################################


def symbol_search(text: str):

    query = text + " tse"

    client = yagooglesearch.SearchClient(
        query=query,
        verbosity=4,
        verbose_output=True,  # False (only URLs) or True (rank, title, description, and URL)
        num=10,
        max_search_result_urls_to_return=10,
        http_429_cool_off_time_in_minutes=1,
        http_429_cool_off_factor=1.1,
        minimum_delay_between_paged_results_in_seconds=2,
        yagooglesearch_manages_http_429s=False,  # Add to manage HTTP 429s.
        # proxy="socks5://127.0.0.1:9050",
    )
    client.assign_random_user_agent()
    urls = client.search()
    urls = pd.DataFrame(urls)
    return urls

s = 27
for s in tqdm(range(22, len(syms_notfind))):
    try:
        result = symbol_search(syms_notfind["name"].iloc[s])
    except Exception as e:
        print(e)
    else:
        if len(result) > 0:
            result["query"] = result["url"].str.contains("tsetmc")
            result = result[result["query"]].reset_index(drop=True, inplace=False)
            if len(result) > 0:
                id_ = id_extract(my_string=result["url"].iloc[0], pattern=combined)
                syms_notfind["symbol_id"].iloc[s] = id_
            else:
                syms_notfind["symbol_id"].iloc[s] = None
        else:
            syms_notfind["symbol_id"].iloc[s] = None
    finally:
        sleep(random.randint(3, 101)/10)

syms_notfind.to_excel("./syms_notfind.xlsx", index=False)
