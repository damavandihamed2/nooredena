import pandas as pd
import requests as rq

header = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"}
response = rq.get(url="https://www.imf.org/external/datamapper/api/v1/indicators", headers=header)
response = response.json()["indicators"]

lst = []
for k in response.keys():
    if k != "":
        tmp_dict = response[k]
        tmp_dict["indicator"] = k
        lst.append(tmp_dict)
indicators = pd.DataFrame(lst)
indicators.to_excel("c:/users/h.damavandi/desktop/indicators.xlsx", index=False)

