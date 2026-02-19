import re
import random
import database
import warnings
import numpy as np
import pandas as pd
import yagooglesearch
from tqdm import tqdm
from time import sleep
from googlesearch import search


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
patterns = ["ParTree=151311&i=([0-9]{1,})", "instInfo/([0-9]{1,})", "InstInfo/([0-9]{1,})"]
combined = "|".join(patterns)
db_conn = database.db_conn
symbols = pd.read_sql(
    "SELECT [symbol], [symbol_name], [symbol_id] FROM [nooredenadb].[tsetmc].[symbols] WHERE active=1",
    db_conn)
# funds_data = pd.read_excel("D:/database/funds and invesments/funds_data.xlsx")
# database.insert_to_database(dataframe=funds_data, database_table="[nooredenadb].[funds].[funds_inv_trades_raw]")
funds_data = pd.read_sql("SELECT * FROM [nooredenadb].[funds].[funds_inv_trades_raw]", db_conn)
funds_data_buy = funds_data[["date", "company", "company_type", "symbol_name", "buy_value"]]
funds_data_buy = funds_data_buy[funds_data_buy["buy_value"] > 0]
funds_data_buy["action"] = 1
funds_data_buy.rename({"buy_value": "value"},axis=1, inplace=True)
funds_data_sell = funds_data[["date", "company", "company_type", "symbol_name", "sell_value"]]
funds_data_sell = funds_data_sell[funds_data_sell["sell_value"] > 0]
funds_data_sell["action"] = 2
funds_data_sell.rename({"sell_value": "value"},axis=1, inplace=True)
funds_data = pd.concat([funds_data_buy, funds_data_sell], axis=0, ignore_index=True)
funds_data["symbol_name"].replace({"ک": "ك", "ی": "ي", r"\\u202b": "", r"\\u200c": " "}, inplace=True, regex=True)

###################################################################################################################

funds_data["option"] = funds_data["symbol_name"].str.contains("اختيار")
funds_data = funds_data[~funds_data["option"]].drop(labels=["option"], axis=1, inplace=False)
funds_data["ros"] = (funds_data["symbol_name"].str.contains("ح . ", regex=False)
                     ) | (funds_data["symbol_name"].str.contains("(تقدم)", regex=False))

funds_data[""]

symbol_names = funds_data[["symbol_name"]].drop_duplicates(keep="first", inplace=False, ignore_index=True)
symbol_names["symbol_name"] = symbol_names["symbol_name"].str.rstrip(" ")
symbol_names.replace({"  ": " "}, regex=True, inplace=True)


symbols["symbol_name"] = symbols["symbol_name"].replace(
    {r"\\u200c": " "}, regex=True, inplace=False).replace({"  ": " "}, regex=True, inplace=False).str.rstrip(" ")


symbol_names_ = symbol_names.merge(symbols[["symbol", "symbol_name", "symbol_id"]].rename(
    {"symbol_name": "symbol_name"}, axis=1, inplace=False), on="symbol_name", how="left")
symbol_names_nan = symbol_names_[symbol_names_["symbol_id"].isna()]






# symbol_names_.to_excel("C:/Users/damavandi/Desktop/symbol_names_new.xlsx", index=False)

syms_find = symbol_names_[~symbol_names_["symbol"].isin([np.nan])].reset_index(drop=True, inplace=False)
syms_notfind = symbol_names_[symbol_names_["symbol"].isin([np.nan])].reset_index(drop=True, inplace=False)

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

s = 0
text = syms_notfind["name"].iloc[s]
search_generator = search(query=text + " tse", safe="off", stop=20)
results_list = []
for rs in search_generator:
    results_list.append(rs)
    if "tsetmc" in rs:
        break
    # sleep(random.randint(10, 31) / 10)
results_data = pd.DataFrame(data={"result": results_list})











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
