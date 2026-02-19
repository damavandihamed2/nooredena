import pandas as pd
import requests as rq
import logging, warnings

from tsetmc import tsetmc_api
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
print("************** sectors **************" + "\n")
logging.basicConfig(filename="D:/Python Projects/new_bi/log/sectors.log", level=logging.ERROR,
                    format='%(asctime)s --- %(levelname)s --- %(message)s --- %(lineno)d')
logger = logging.getLogger(__name__)
dl = ["10523825119011581", "49579049405614711", "62752761908615603", "71704845530629737", "43754960038275285",
      "8384385859414435", "69932667409721265", "5798407779416661", "32097828799138957", "67130298613737946",
      "46342955726788357", "30655480864841493"]
db_conn = make_connection()

###########################################################################################################

while True:
    try:
        static_data = rq.get(tsetmc_api.url_static_data, headers=tsetmc_api.headers_static_data)
        if static_data.status_code == 200:
            break
    except Exception as e:
        print(e)
static_data = pd.DataFrame(static_data.json()["staticData"])
static_data = static_data[static_data["type"] != "PaperType"]
static_data["code"] = static_data["code"].astype("str").str.zfill(2)
static_data["name"] = [" ".join(static_data["name"].iloc[i].split()) for i in range(len(static_data))]
static_data["query"] = [False if "حذف" in static_data["name"].iloc[i] or "غيرفعال" in static_data["name"].iloc[i] else True for i in range(len(static_data))]
static_data = static_data[static_data["query"]]
static_data.drop(labels=["id", "type", "description", "query"], axis=1, inplace=True)
static_data.reset_index(drop=True, inplace=True)
static_data.rename(mapper={"code": "sector", "name": "sector_name"}, axis=1, inplace=True)

###########################################################################################################

while True:
    try:
        indices = rq.get(url=tsetmc_api.url_all_indices, headers=tsetmc_api.headers_all_indices)
        if indices.status_code == 200:
            break
    except Exception as e:
        print(e)
indices = pd.DataFrame(indices.json()["indexB1"]).astype({"insCode": str}).query(f"~insCode.isin({dl})")
indices["sector"] = indices["lVal30"].str.extract('(\d+)')
indices = indices[["insCode", "sector"]].rename({"insCode": "sector_id"}, axis=1)

###########################################################################################################

static_data_ = static_data.merge(indices, on="sector", how="left")

cursor = db_conn.cursor()
cursor.execute("TRUNCATE TABLE [nooredenadb].[tsetmc].[sectors]")
cursor.close()
insert_to_database(dataframe=static_data_, database_table="[nooredenadb].[tsetmc].[sectors]")

###########################################################################################################
