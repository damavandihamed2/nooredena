import pandas as pd
import requests as rq
from tqdm import tqdm
import logging, warnings, datetime

from tsetmc import tsetmc_api
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
print("************** sectos history **************" + "\n")
logging.basicConfig(filename="D:/Python Projects/new_bi/log/sectors_history.log", level=logging.ERROR,
                    format='%(asctime)s --- %(levelname)s --- %(message)s --- %(lineno)d')
logger = logging.getLogger(__name__)
yesterday = int((datetime.datetime.today() - datetime.timedelta(1)).strftime("%Y%m%d"))
powerbi_database = make_connection()
sectors = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[sectors] WHERE sector_id IS NOT NULL", powerbi_database)

###########################################################################################################

sectors_history = []
for s in tqdm(range(len(sectors))):
    while True:
        try:
            sctr_hist = rq.get(url=tsetmc_api.url_sector_history + sectors["sector_id"].iloc[s], headers=tsetmc_api.headers_sector_history)
            if sctr_hist.status_code == 200:
                break
        except Exception as e:
            print(e)
    sctr_hist = sctr_hist.json()["indexB2"]
    sectors_history += sctr_hist
sectors_history = pd.DataFrame(sectors_history).astype({"insCode": str})
sectors_history.rename({"insCode": "sector_id", "dEven": "date", "xNivInuClMresIbs": "close_price",
                        "xNivInuPbMresIbs": "low_price", "xNivInuPhMresIbs": "high_price"}, axis=1, inplace=True)

sectors_history = sectors_history[sectors_history["date"] == yesterday]

insert_to_database(dataframe=sectors_history, database_table="[nooredenadb].[tsetmc].[sectors_history]")

###########################################################################################################
