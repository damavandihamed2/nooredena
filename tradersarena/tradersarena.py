import pandas as pd
import requests as rq
from tqdm import tqdm
from time import sleep
import random, warnings

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
powerbi_database = make_connection()

####################################################################################################

symbols = pd.read_sql("SELECT symbol, symbol_name, symbol_id FROM [nooredenadb].[tsetmc].[symbols] WHERE active=1", powerbi_database)
symbols.replace({"ك": "ک", "ي": "ی"}, regex=True, inplace=True)
search_phrase = symbols["symbol"].unique().tolist() + symbols["symbol_name"].unique().tolist()

search_results = []
for sp in tqdm(range(len(search_phrase))):
    url = f"https://tradersarena.ir/searchData?keyword={search_phrase[sp]}"
    res = rq.get(url=url)
    search_results += res.json()
    sleep(random.randint(5, 16)/10)
search_results_df = pd.DataFrame(data=search_results, columns=["symbol_id", "symbol", "symbol_name"]).drop_duplicates(
    "symbol_id", keep="first", inplace=False).sort_values("symbol", inplace=False, ignore_index=True)

####################################################################################################

tradersarena_symbols = search_results_df.copy()
symbols_df = []
for i in tqdm(range(len(tradersarena_symbols))):

    symbol = tradersarena_symbols["symbol"].iloc[i]
    symbol_name = tradersarena_symbols["symbol_name"].iloc[i]
    symbol_id = tradersarena_symbols["symbol_id"].iloc[i]
    url = f"https://tradersarena.ir/data/{symbol_id}/reports"
    res = rq.get(url=url)
    if res.status_code == 200:
        res = res.json()
        SSup = res["SSup"]
        SRes = res["SRes"]
        del res["SSup"]
        del res["SRes"]
        for k, v in SSup.items():
            res["S" + k] = v["y"]
            res["S" + k + "_date"] = v["x"]
        for k, v in SRes.items():
            res["S" + k] = v["y"]
            res["S" + k + "_date"] = v["x"]
        DSup = res["DSup"]
        DRes = res["DRes"]
        del res["DSup"]
        del res["DRes"]
        res["symbol"] = symbol
        res["symbol_name"] = symbol_name
        res["symbol_id"] = symbol_id
        symbols_df.append(res)

        sleep(random.randint(5, 16)/10)

symbols_df = pd.DataFrame(symbols_df)
symbols_df = symbols_df[["symbol", "symbol_id", "symbol_name", "SS0", "SS0_date", "SR0", "SR0_date"]]

crsr = powerbi_database.cursor()
crsr.execute("TRUNCATE TABLE [nooredenadb].[tradersarena].[symbols]")
crsr.close()
insert_to_database(symbols_df, "[nooredenadb].[tradersarena].[symbols]")
