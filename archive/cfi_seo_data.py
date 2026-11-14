import random
import datetime
import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep


t1 = datetime.datetime.now()

h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

res = rq.get(
    url="https://cfi.rbcapi.ir/institutes?offset=12&limit=1200&lng=fa&name=&city=&province=&instituteType=&instituteKind=&activityType=&licenseType=",
    headers=h)
res = res.json()["data"]
df = pd.DataFrame(res)
# df.to_excel("./cfiseo_data.xlsx", index=False)

details_df = []
branches_df = []
licenses_df = []
otas_df = []
pillars_df = []
for i in tqdm(range(len(df))):

    idx = df["Id"].iloc[i]

    details = rq.get(url=f"https://cfi.rbcapi.ir/institutes/{idx}?offset=0&limit=10&lng=fa", headers=h)
    details_df += [details.json()]
    sleep(random.randint(10, 31)/10)

    branches = rq.get(url=f"https://cfi.rbcapi.ir/institutes/{idx}/branches?offset=1&limit=200&lng=fa", headers=h)
    branches_df += branches.json()
    sleep(random.randint(10, 31)/10)

    licenses = rq.get(url=f"https://cfi.rbcapi.ir/institutes/{idx}/licenses?offset=1&limit=200&lng=fa", headers=h)
    licenses_df += licenses.json()
    sleep(random.randint(10, 31)/10)

    otas = rq.get(url=f"https://cfi.rbcapi.ir/institutes/{idx}/onlineTechnicalApprovals?offset=1&limit=200&lng=fa", headers=h)
    otas_df += otas.json()
    sleep(random.randint(10, 31)/10)

    # pillars = rq.get(url=f"https://cfi.rbcapi.ir/institutes/{idx}/pillars?offset=1&limit=200&lng=fa", headers=h)
    # pillars_df += pillars.json()
    # sleep(random.randint(10, 31)/10)

details_df = pd.DataFrame(details_df)
details_df.to_excel("./details_df.xlsx", index=False)

branches_df = pd.DataFrame(branches_df)
branches_df.to_excel("./branches_df.xlsx", index=False)

licenses_df = pd.DataFrame(licenses_df)
licenses_df.to_excel("./licenses_df.xlsx", index=False)

otas_df = pd.DataFrame(otas_df)
otas_df.to_excel("./otas_df.xlsx", index=False)

# pillars_df = pd.DataFrame(pillars_df)
# pillars_df.to_excel("./pillars_df.xlsx", index=False)

t2 = datetime.datetime.now()

print("--------", (t2 - t1).seconds, " Seconds", "--------")
