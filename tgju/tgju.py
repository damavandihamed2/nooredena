import pandas as pd
import requests, datetime, string, random, time


def make_random_str(rand_limit):
    possible = string.ascii_letters + string.digits
    text = ''.join(random.choice(possible) for _ in range(rand_limit))
    return text



url_current = 'https://call{n_}.tgju.org/ajax.json?rev={rev}'
headers_current = {
    'referer': 'https://www.tgju.org/', 'origin': 'https://www.tgju.org', 'accept': '*/*', 'sec-fetch-site': 'same-site',
    'accept-language': 'en-US,en;q=0.9,fa;q=0.8', 'pragma': 'no-cache', 'priority': 'u=1, i',  'sec-fetch-mode': 'cors',
    'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty', 'cache-control': 'no-cache',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 '
                  'Safari/537.36', 'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"'}
def get_current_data():
    r = 1
    while True:
        print(f"Try {r}")
        try:
            response = requests.get(
                url=url_current.format(n_=random.randint(1, 4), rev=make_random_str(60)),
                headers=headers_current,
                timeout=10
            )
            data = response.json()
            break
        except Exception as e:
            print(e)
            print("waiting for 10 seconds")
            time.sleep(random.randint(50, 101) / 10)
        r += 1
    return data



url_symbol_data_table = "https://api.tgju.org/v1/market/indicator/summary-table-data/{symbol}"
headers_symbol_data_table = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.7', 'upgrade-insecure-requests': '1', 'cache-control': 'no-cache',
    'sec-fetch-dest': 'document', 'sec-fetch-mode': 'navigate', 'sec-ch-ua-platform': '"Windows"', 'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"', 'sec-fetch-site': 'none',
    'sec-ch-ua-mobile': '?0', 'sec-fetch-user': '?1', 'pragma': 'no-cache', 'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 '
                  'Safari/537.36'}
def get_symbol_data_table(symbol):
    r = 1
    while True:
        print(f"Try {r}")
        try:
            response = requests.get(url=url_symbol_data_table.format(symbol=symbol),
                                    headers=headers_symbol_data_table, timeout=10)
            break
        except Exception as e:
            print(e)
            print("waiting for 10 seconds")
            time.sleep(random.randint(50, 101) / 10)
        r += 1
    df_data = response.json()
    df_data = pd.DataFrame(df_data["data"])
    df_data = df_data[[0, 1, 2, 3, 6]]
    df_data.columns = ["open_price", "low_price", "high_price", "close_price", "date"]
    df_data['date'] = df_data['date'].replace("/", "-", regex=True, inplace=False)
    df_data.replace(",", "", regex=True, inplace=True)
    df_data = df_data.astype({"open_price": float, "low_price": float, "high_price": float, "close_price": float})
    return df_data



url_symbol_data_chart = 'https://dashboard-api.tgju.org/v1/tv2/history?symbol={symbol}'
headers_symbol_data_chart = headers_symbol_data_table.copy()
def get_symbol_data_chart(symbol):
    r = 1
    while True:
        print(f"Try {r}")
        try:
            response = requests.get(url=url_symbol_data_chart.format(symbol=symbol),
                                    headers=headers_symbol_data_chart, timeout=10)
            break
        except Exception as e:
            print(e)
            print("waiting for 10 seconds")
            time.sleep(random.randint(50, 101) / 10)
        r += 1
    df_data = response.json()
    df_data.pop("v")
    df_data.pop("s")
    df_data = pd.DataFrame(df_data)
    df_data.rename({"t": "date", "c": "close_price", "o": "open_price", "h": "high_price", "l": "low_price"},
                   axis=1, inplace=True)
    df_data['date'] = df_data['date'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
    return df_data

