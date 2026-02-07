import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
import random, logging, warnings

from tsetmc import tsetmc_api
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
print("************** symbols **************" + "\n")
logging.basicConfig(filename="D:/Python Projects/new_bi/log/symbols_history.log",
                    level=logging.ERROR, format='%(asctime)s --- %(levelname)s --- %(message)s --- %(lineno)d')
logger = logging.getLogger(__name__)
powerbi_database = make_connection()

col_mapper = {"priceChange": "change_price", "priceMin": "low_price", "priceMax": "high_price", "insCode": "symbol_id",
              "priceYesterday": "yesterday_price", "priceFirst": "open_price", "last": "last", "hEven": "last_time",
              "pDrCotVal": "close_price", "zTotTran": "trade_amount", "pClosing": "final_price", "iClose": "iclose",
              "yClose": "yclose", "dEven": "date", "id": "id", "qTotTran5J": "trade_volume", "qTotCap": "trade_value"}

# today = jdatetime.datetime.today()
# date = today.strftime(format="%Y-%m-%d")
# time = today.strftime("%H:%M:%S")

symbols = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols] WHERE active=1", powerbi_database)

for s in tqdm(range(len(symbols))):
    idx = symbols["symbol_id"].iloc[s]
    response = rq.get(url=tsetmc_api.url_price_history + f"{idx}/0",
                      headers=tsetmc_api.headers_price_history)
    df = pd.DataFrame(response.json()["closingPriceDaily"])
    df.rename(col_mapper, axis=1, inplace=True)
    df.drop(columns=["change_price", "last", "id", "last_time", "iclose", "yclose"], inplace=True)
    insert_to_database(dataframe=df, database_table="[nooredenadb].[tsetmc].[symbols_history]", loading=False)
    sleep(random.randint(1000, 3001) / 1000)
