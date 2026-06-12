import pandas as pd
from tqdm import tqdm
import requests as rq
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
enigma_agent.login(use_old_token=True)


symbols = pd.read_sql("SELECT * FROM [nooredenadb].[enigma].[symbols] WHERE industryId IN (13, 27, 23, 43, 44, 53) AND"
                      " id NOT IN (SELECT distinct(symbolId) FROM [nooredenadb].[enigma].[shareholders])", db_conn)

df_ = pd.DataFrame()
for i in tqdm(range(1, len(symbols))):
    symbol_id = symbols["id"].iloc[i]
    response_shareholders = rq.get(
        url=f'https://core.enigma.ir/api/v1/shareholders/security/{symbol_id}/combined/',
        headers={**enigma_agent.default_headers, "Authorization": f"Bearer {enigma_agent.access_token}"}
    )
    data = response_shareholders.json()["data"]
    l = [{"id": d.get("id"), "name": d.get("name"),
         "volume": d.get("volume"), "value": d.get("value"),
         "sharePercent": d.get("percent"), "symbolId": symbol_id} for d in data]
    l_df = pd.DataFrame(l)
    df_ = pd.concat([df_, l_df], axis=0, ignore_index=True)
    insert_to_database(l_df, "[nooredenadb].[enigma].[shareholders]", loading=False)

