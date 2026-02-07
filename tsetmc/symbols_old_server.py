import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
import random, logging, warnings

from tsetmc import tsetmc_api
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
print("************** symbols **************" + "\n")
logging.basicConfig(filename="C:/Users/Administrator.NOOREDENA/PycharmProjects/bi/log/symbols_old.log",
                    level=logging.ERROR,
                    format='%(asctime)s --- %(levelname)s --- %(message)s --- %(lineno)d')
logger = logging.getLogger(__name__)
powerbi_database = make_connection()

symbols = pd.read_sql("SELECT DISTINCT symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols]", powerbi_database)

for i in tqdm(range(len(symbols))):

    i = 2

    sym = symbols["symbol"].iloc[i]
    sym_id = symbols["symbol_id"].iloc[i]

    while True:
        try:
            res = rq.get(url=tsetmc_api.url_search_name + sym, headers=tsetmc_api.headers_search_name)
            if res.status_code == 200: break
        except Exception as e:
            print(e)
            sleep(random.randint(8, 15))

    df = pd.DataFrame(res.json()["instrumentSearch"])

    if len(df) > 0:
        df["symbol"] = [sym] * len(df)
        df["symbol_id"] = [sym_id] * len(df)
        df["cgrValCotTitle"].replace("-'", "-", inplace=True)
        df.drop(columns=['cIsin', 'zTitad', 'baseVol', 'instrumentID', 'cComVal'], inplace=True)
        insert_to_database(df, "[nooredenadb].[tsetmc].[symbols_old]")

    sleep(random.randint(200, 601) / 100)
