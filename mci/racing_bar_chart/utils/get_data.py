import requests


base_url = "https://cdn.tsetmc.com/api"
headers = {'accept': 'application/json, text/plain, */*', 'accept-language': 'en-US,en;q=0.9,fa;q=0.8',
           'cache-control': 'no-cache','origin': 'https://www.tsetmc.com', 'pragma': 'no-cache', 'priority': 'u=1, i',
           'referer': 'https://www.tsetmc.com/', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty',
           'sec-ch-ua': '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"', 'sec-ch-ua-mobile': '?0',
           'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/149.0.0.0 Safari/537.36', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-site'}

def get_total_share(symbol_id: str, date: int) -> int:
    response = requests.get(f'{base_url}/Instrument/GetInstrumentHistory/{symbol_id}/{date}', headers=headers)
    total_share = response.json()["instrumentHistory"].get("zTitad", None)
    return total_share


def get_final_price(symbol_id: str, date: int) -> int:
    response_ = requests.get(f'{base_url}/ClosingPrice/GetClosingPriceDaily/{symbol_id}/{date}', headers=headers)
    final_price = response_.json()["closingPriceDaily"].get("pClosing", None)
    return final_price
