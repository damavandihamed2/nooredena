import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
import random, logging, warnings, jdatetime

from tsetmc import tsetmc_api
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
print("************** Old Symbols **************" + "\n")
logging.basicConfig(filename="D:/Python Projects/new_bi/log/symbols_old.log",
                    level=logging.ERROR,
                    format='%(asctime)s --- %(levelname)s --- %(message)s --- %(lineno)d')
logger = logging.getLogger(__name__)
db_conn = make_connection()

today = jdatetime.datetime.today()
date = today.strftime(format="%Y-%m-%d")
time = today.strftime("%H:%M:%S")

old_symbols = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols_old] WHERE symbol_id != insCode and "
                          "symbol=lVal18AFC  and lastDate=0", db_conn)
old_symbols.drop_duplicates(subset="insCode", keep="first", inplace=True, ignore_index=True)

#########################################################
#########################################################

ss = ["نبورس", "وقوام", "كبورس", "آكنتور", "شوش", "اكالا", "ومهر", "وكوثر", "كايگچ", "وانصار", "تپمپس"]

closed = pd.DataFrame()
for i in range(len(ss)):
    response = rq.get(url=tsetmc_api.url_search_name + ss[i], headers=tsetmc_api.headers_search_name)
    df = pd.DataFrame(response.json()["instrumentSearch"])
    df = df[df["lVal18AFC"] == ss[i]]
    df["symbol"] = [ss[i]] * len(df)
    closed = pd.concat([closed, df], axis=0, ignore_index=True)

#########################################################
#########################################################

print("\n" + "***** Instrument Info *****" + "\n")
symbols_info = []
for i in tqdm(range(len(closed))):
    while True:
        try:
            r1 = rq.get(url=tsetmc_api.url_instrument_info + f"{closed['insCode'].iloc[i]}",
                        headers=tsetmc_api.headers_instrument_info)
            if r1.status_code == 200: break
        except Exception as e:
            sleep(random.randint(2, 6))
            print(e)
    # sleep(random.randint(10, 31) / 10)
    symbols_info.append(r1.json()["instrumentInfo"])
symbols_info = pd.DataFrame(symbols_info)
symbols_info.drop(labels=["dEven", "contractSize", "lSoc30", "cValMne", "cSocCSAC",
                          "yMarNSC", "sourceID", "underSupervision"], axis=1, inplace=True)

#########################################################

print("\n" + "***** Instrument Identity *****" + "\n")
symbols_identity = []
for i in tqdm(range(len(closed))):
    while True:
        try:
            r2 = rq.get(url=tsetmc_api.url_instrument_identity + f"{closed['insCode'].iloc[i]}",
                          headers=tsetmc_api.headers_instrument_identity)
            if r2.status_code == 200:
                break
        except Exception as e:
            sleep(random.randint(2, 5))
            print(e)
    # sleep(random.randint(10, 31) / 10)
    symbols_identity.append(r2.json()["instrumentIdentity"])
symbols_identity = pd.DataFrame(symbols_identity)
symbols_identity = symbols_identity[["subSector", "cValMne", "cSocCSAC"]]

#########################################################

symbols = pd.concat([symbols_info, symbols_identity], axis=1)

eps = pd.json_normalize(symbols["eps"]).drop(columns="epsValue", inplace=False)
sector = pd.json_normalize(symbols["sector"]).drop(columns="dEven", inplace=False)
statis_threshold = pd.json_normalize(symbols["staticThreshold"]).drop(
    columns=["insCode", "dEven", "hEven"], inplace=False)
sub_sector = pd.json_normalize(symbols["subSector"]).drop(columns=["dEven", "cSecVal"], inplace=False)
sub_sector["cSoSecVal"] = sub_sector["cSoSecVal"].astype("str").str.zfill(4)
symbols.drop(labels=["eps", "sector", "staticThreshold", "subSector"], axis=1, inplace=True)
symbols = pd.concat([symbols, eps, statis_threshold, sector, sub_sector], axis=1)

#########################################################

column_mapper = {
    "minWeek": "min_week", "maxWeek": "max_week", "minYear": "min_year", "maxYear": "max_year", "priceMin": "low_price",
    "qTotCap": "trade_value", "cComVal": "board", "cgrValCot": "category", "cValMne": "symbol_eng", "cSecVal": "sector",
    "lSoSecVal": "subsector_name", "insCode": "symbol_id", "lVal18AFC": "symbol", "topInst": "top_symbol", "nav": "nav",
    "priceMax": "high_price", "priceYesterday": "yesterday_price", "pClosing": "final_price", "lastHEven": "last_time",
    "qTotTran5JAvg": "monthly_mean_volume", "underSupervision": "under_supervision", "cgrValCotTitle": "category_name",
    "zTotTran": "trade_amount", "qTotTran5J": "trade_volume", "kAjCapValCpsIdx": "floating_share", "cEtaval": "status",
    "priceFirst": "open_price", "psr": "psr", "estimatedEPS": "estimated_eps", "sectorPE": "sector_pe", "last": "last",
    "cEtavalTitle": "status_name", "lVal18": "symbol_name_eng", "lVal30": "symbol_name", "instrumentID": "symbol_code",
    "cSocCSAC": "company", "cIsin": "company_code", "lastDate": "last_date", "flow": "flow", "flowTitle": "flow_name",
    "priceChange": "change_price", "iClose": "iclose", "yClose": "yclose", "pDrCotVal": "close_price", "yVal": "yval",
    "faraDesc": "description", "zTitad": "total_share", "baseVol": "base_volume", "finalLastDate": "final_last_date",
    "psGelStaMax": "sttc_thr_max", "psGelStaMin": "sttc_thr_min", "lSecVal": "sector_name", "cSoSecVal": "subsector"
}
symbols = symbols.rename(mapper=column_mapper, axis=1, inplace=False)
symbols = symbols[symbols["yval"].isin(["300", "303", "309", "310"])].reset_index(drop=True, inplace=False)
symbols.replace({"floating_share": {"": None}, "category_name": {"-'": "-"}}, inplace=True)
symbols.replace({"status": {" ": ""}, "sector": {" ": ""}}, regex=True, inplace=True)

symbols["date_"] = [date] * len(symbols)
symbols["time_"] = [time] * len(symbols)
symbols["active"] = [0] * len(symbols)

symbols["query"] = [True] * len(symbols)
for t in range(len(symbols)):
    if ("پذيره" in symbols["symbol"].iloc[t]):
        symbols["query"].iloc[t] = False
    elif ("حذف" in symbols["symbol"].iloc[t]):
        symbols["query"].iloc[t] = False
    elif ("پذيره" in symbols["symbol_name"].iloc[t]):
        symbols["query"].iloc[t] = False
    elif ("حذف" in symbols["symbol_name"].iloc[t]):
        symbols["query"].iloc[t] = False
    elif (symbols["symbol_name"].iloc[t][-2:] == "پذ"):
        symbols["query"].iloc[t] = False
    elif (symbols["symbol_name"].iloc[t][-4:] == "پذير"):
        symbols["query"].iloc[t] = False
    else:
        pass
symbols = symbols[symbols["query"]]
symbols.drop(columns="query", inplace=True)

#########################################################

symbols_server = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols]", db_conn)
insert_to_database(dataframe=symbols, database_table="[nooredenadb].[tsetmc].[symbols]", loading=False)

