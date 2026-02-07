import pandas as pd
import requests as rq


funds = rq.get("https://fund.fipiran.ir/api/v1/fund/fundcompare")

funds = pd.DataFrame(funds.json()["items"])

funds_ = funds[["name", "regNo", "fundType", "websiteAddress"]]
funds_["websiteAddress"].replace("'", "", inplace=True, regex=True)
funds_["websiteAddress"].replace("[", "", inplace=True, regex=True)
funds_["websiteAddress"].replace("]", "", inplace=True, regex=True)

funds_["websiteAddress"]


