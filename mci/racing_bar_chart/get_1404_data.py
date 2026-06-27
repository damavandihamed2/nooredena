import warnings
import requests
import pandas as pd
from tqdm import tqdm
from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()


symbols_quer = ("SELECT s.*, sh.last_date FROM (SELECT symbol, symbol_id, final_last_date, active FROM "
                "[nooredenadb].[tsetmc].[symbols] WHERE sector != '68' AND flow != 6 AND "
                "(total_share*final_price) > 150000000000000) AS s LEFT JOIN (select symbol_id, MAX(date) last_date "
                "FROM [nooredenadb].[tsetmc].[symbols_history] WHERE trade_amount <> 0 AND [date] < 20260320 "
                "GROUP BY symbol_id) sh ON s.symbol_id=sh.symbol_id")
symbols = pd.read_sql(symbols_quer, db_conn)
db_conn.close()
symbols.dropna(subset=["last_date"], inplace=True)
symbols = symbols[symbols["final_last_date"] >= 20260101]
symbols = symbols[symbols["last_date"] >= 20260101]
symbols["total_share"] = None
symbols["final_price"] = None

def get_total_share(symbol_id: str, date: int) -> int:
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,fa;q=0.8',
        'cache-control': 'no-cache',
        'origin': 'https://www.tsetmc.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.tsetmc.com/',
        'sec-ch-ua': '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
    }
    response = requests.get(f'https://cdn.tsetmc.com/api/Instrument/GetInstrumentHistory/{symbol_id}/{date}', headers=headers)
    total_share = response.json()["instrumentHistory"].get("zTitad", None)
    return total_share


def get_final_price(symbol_id: str, date: int) -> int:
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,fa;q=0.8',
        'cache-control': 'no-cache',
        'origin': 'https://www.tsetmc.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.tsetmc.com/',
        'sec-ch-ua': '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
    }
    response_ = requests.get(f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDaily/{symbol_id}/{date}', headers=headers)
    final_price = response_.json()["closingPriceDaily"].get("pClosing", None)
    return final_price


for i in tqdm(range(len(symbols))):
    s_id = symbols["symbol_id"].iloc[i]
    s_date = int(symbols["last_date"].iloc[i])
    if symbols["total_share"].iloc[i] is None:
        t_s = get_total_share(s_id, s_date)
        symbols["total_share"].iloc[i] = t_s
    if symbols["final_price"].iloc[i] is None:
        f_p = get_final_price(s_id, s_date)
        symbols["final_price"].iloc[i] = f_p


symbols["market_cap"] = (symbols["total_share"] * symbols["final_price"]) / 1e9
symbols.sort_values("market_cap", ascending=False, inplace=True, ignore_index=True)
symbols["rank"] = range(1, len(symbols) + 1)
symbols["date"] = 1404
symbols = symbols[["symbol", "market_cap", "rank", "date"]]
# symbols.to_excel("c:/users/h.damavandi/desktop/symbols.xlsx", index=False)

