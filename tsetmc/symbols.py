import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
import random, logging, warnings, jdatetime

from tsetmc import tsetmc_api
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
print("************** symbols **************" + "\n")
logging.basicConfig(filename="D:/Python Projects/new_bi/log/symbols.log",
                    level=logging.ERROR,
                    format='%(asctime)s --- %(levelname)s --- %(message)s --- %(lineno)d')
logger = logging.getLogger(__name__)
db_conn = make_connection()
today = jdatetime.datetime.today()
date = today.strftime(format="%Y-%m-%d")
time = today.strftime("%H:%M:%S")
sectors = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[sectors] where sector not in ('59', '69', '76', '77', '82')",
                      db_conn)
symbols_server = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols]", db_conn)

#########################################################

print("\n" + "***** Related Companies *****" + "\n")
related_companies = []
for s in tqdm(range(len(sectors))):
    while True:
        try:
            r1 = rq.get(url=tsetmc_api.url_sector_companies + f"{sectors['sector'].iloc[s]}",
                       headers=tsetmc_api.headers_sector_companies)
            if r1.status_code == 200: break
        except Exception as e:
            print(e)
            sleep(random.randint(5, 10))
    related_companies = related_companies + r1.json()["relatedCompany"]

related_companies = pd.DataFrame(related_companies)
related_companies.drop(labels=["instrumentState", "instrument", "lastHEven", "finalLastDate", "nvt", "mop", "pRedTran",
                               "thirtyDayClosingHistory", "priceFirst", "id", "dEven", "hEven"], axis=1, inplace=True)

#########################################################

print("\n" + "***** Commodity Funds *****" + "\n")
while True:
    try:
        r2 = rq.get(url=tsetmc_api.url_commodity_funds, headers=tsetmc_api.headers_commodity_funds)
        if r2.status_code == 200:
            break
    except Exception as e:
        sleep(random.randint(2, 5))
        print(e)
# sleep(random.randint(10, 31) / 10)
commodity_funds = pd.DataFrame(r2.json()["tradeTop"])
commodity_funds.drop(columns=['instrumentState', 'instrument', 'lastHEven', 'finalLastDate', 'nvt', 'mop', 'pRedTran',
                              'thirtyDayClosingHistory', 'priceFirst', 'id', 'dEven', 'hEven'], inplace=True)

#########################################################

related_companies = pd.concat([related_companies, commodity_funds], axis=0, ignore_index=True)

#########################################################

print("\n" + "***** Instrument Info *****" + "\n")
symbols_info = []
for i in tqdm(range(len(related_companies))):
    while True:
        try:
            r3 = rq.get(url=tsetmc_api.url_instrument_info + f"{related_companies['insCode'].iloc[i]}",
                        headers=tsetmc_api.headers_instrument_info)
            if r3.status_code == 200: break
        except Exception as e:
            sleep(random.randint(2, 6))
            print(e)
    # sleep(random.randint(10, 31) / 10)
    symbols_info.append(r3.json()["instrumentInfo"])
symbols_info = pd.DataFrame(symbols_info)
symbols_info.drop(labels=["dEven", "contractSize", "lSoc30", "cValMne", "cSocCSAC",
                          "yMarNSC", "sourceID", "underSupervision"], axis=1, inplace=True)

#########################################################

print("\n" + "***** Instrument Identity *****" + "\n")
symbols_identity = []
for i in tqdm(range(len(related_companies))):
    while True:
        try:
            r4 = rq.get(url=tsetmc_api.url_instrument_identity + f"{related_companies['insCode'].iloc[i]}",
                          headers=tsetmc_api.headers_instrument_identity)
            if r4.status_code == 200:
                break
        except Exception as e:
            sleep(random.randint(2, 5))
            print(e)
    # sleep(random.randint(10, 31) / 10)
    symbols_identity.append(r4.json()["instrumentIdentity"])
symbols_identity = pd.DataFrame(symbols_identity)
symbols_identity = symbols_identity[["lVal18AFC", "subSector", "cValMne", "cSocCSAC"]]

#########################################################

print("\n" + "***** Instrument ClosingPrice Info *****" + "\n")
closing_price_info = []
for i in tqdm(range(len(related_companies))):
    while True:
        try:
            r5 = rq.get(tsetmc_api.url_symbol_price_info + f"{related_companies['insCode'].iloc[i]}",
                          headers=tsetmc_api.headers_symbol_price_info)
            if r5.status_code == 200: break
        except Exception as e:
            sleep(random.randint(2, 6))
            print(e)
    # sleep(random.randint(10, 31) / 10)
    closing_price_info.append(r5.json()["closingPriceInfo"])
closing_price_info = pd.DataFrame(closing_price_info)
closing_price_info = closing_price_info[['instrumentState', 'lastHEven', 'finalLastDate', 'priceFirst']]

#########################################################

symbols = pd.concat([
    related_companies.drop(columns="insCode"),
    symbols_info,
    symbols_identity.drop(columns="lVal18AFC"),
    closing_price_info
], axis=1)

eps = pd.json_normalize(symbols["eps"]).drop(columns="epsValue", inplace=False)
sector = pd.json_normalize(symbols["sector"]).drop(columns="dEven", inplace=False)
statis_threshold = pd.json_normalize(symbols["staticThreshold"]).drop(columns=["insCode", "dEven", "hEven"], inplace=False)
sub_sector = pd.json_normalize(symbols["subSector"]).drop(columns=["dEven", "cSecVal"], inplace=False)
sub_sector["cSoSecVal"] = sub_sector["cSoSecVal"].astype("str").str.zfill(4)
state = pd.json_normalize(symbols["instrumentState"])[["cEtaval", "underSupervision", "cEtavalTitle"]]
symbols.drop(labels=["eps", "sector", "staticThreshold", "subSector", "instrumentState"], axis=1, inplace=True)
symbols = pd.concat([symbols, eps, statis_threshold, sector, sub_sector, state], axis=1)

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
symbols = symbols[symbols["yval"].isin(["300", "303", "305", "309", "313", "380"])].reset_index(drop=True, inplace=False)
symbols.replace({"floating_share": {"": None}, "category_name": {"-'": "-"}}, inplace=True)
symbols.replace({"status": {" ": ""}, "sector": {" ": ""}}, regex=True, inplace=True)
symbols["last"] = symbols["last"].astype(str)
symbols["iclose"] = symbols["last"].astype(str)
symbols["yclose"] = symbols["last"].astype(str)
symbols["date_"] = [date] * len(symbols)
symbols["time_"] = [time] * len(symbols)

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

for i in range(len(symbols_server)):
    sid = symbols_server["symbol_id"].iloc[i]
    if sid not in symbols["symbol_id"].values:
        crsr = db_conn.cursor()
        crsr.execute(f"UPDATE [nooredenadb].[tsetmc].[symbols] SET active=0 WHERE symbol_id='{sid}'")
        crsr.close()


symbols["active"] = [1] * len(symbols)
crsr = db_conn.cursor()
crsr.execute(f"DELETE FROM [nooredenadb].[tsetmc].[symbols] WHERE active=1")
crsr.close()
insert_to_database(dataframe=symbols, database_table="[nooredenadb].[tsetmc].[symbols]", loading=False)
