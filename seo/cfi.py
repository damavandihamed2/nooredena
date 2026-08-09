import pandas as pd
from tqdm import tqdm
import requests as rq


headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Origin': 'https://cfi.seo.ir',
    'Pragma': 'no-cache',
    'Referer': 'https://cfi.seo.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

response = rq.get(url='https://cfi.rbcapi.ir/institutes', headers=headers, verify=False)
dataframe = pd.DataFrame(response.json()['data'])

detail_data = []
for i in tqdm(range(len(dataframe))):
    id_ = dataframe["Id"].iloc[i]
    response_ = rq.get(url=f'https://cfi.rbcapi.ir/institutes/{id_}', headers=headers, verify=False)
    res_json = response_.json()
    if res_json:
        detail_data.append(res_json)
detail_data_df = pd.DataFrame(detail_data)


final_df = dataframe[['AminName', 'InquiryStatus', 'Id']].merge(detail_data_df, how='outer', on='Id')
final_df.to_excel(excel_writer="./seo/cfiseo.xlsx", index=False)
