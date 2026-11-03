import warnings

from utils.database import make_connection
from fipiran.update_db import upsert_funds_all, upsert_fund_types
from fipiran.tools import get_reg_numbers_unique, remove_duplicates
from fipiran.fetcher import get_funds_all, get_fund, get_fund_types



warnings.filterwarnings("ignore")
db_conn = make_connection()


fund_types = get_fund_types()
upsert_fund_types(fund_types, db_conn)


all_funds = get_funds_all()
upsert_funds_all(funds_data=all_funds, db_connection=db_conn)



reg_numbers = get_reg_numbers_unique(db_connection=db_conn)

funds_list = []
for rn in reg_numbers:

    gi = 0

    print(f"fetching fund (regNo={rn}, groupId={gi})")
    fund_data = get_fund(reg_number=rn, group_id=gi)

    if fund_data:
        funds_list.append(fund_data)

    while True:
        gi += 1
        print(f"fetching fund (regNo={rn}, groupId={gi})")
        fund_data = get_fund(reg_number=rn, group_id=gi)
        if fund_data:
            funds_list.append(fund_data)
        else:
            break


import pickle
import pandas as pd
from fipiran.temptools import inspect_columns_for_sql

# with open('c:/users/h.damavandi/desktop/funds_list.pkl', 'wb') as f:
#     pickle.dump(funds_list, f)

with open('c:/users/h.damavandi/desktop/funds_list.pkl', "rb") as file:
    funds_list = pickle.load(file)

funds_rank = [
    f.pop("fundRank")
    for f in funds_list
    if "fundRank" in f
]


funds_list = remove_duplicates(funds_list)


funds_rank_df = pd.DataFrame(funds_rank)
funds_rank_df.dtypes
funds_rank_df_dup = funds_rank_df[funds_rank_df.duplicated(keep=False)]
funds_rank_df_dup_not = funds_rank_df[~funds_rank_df.duplicated(keep=False)]

report = inspect_columns_for_sql(funds_rank_df)

report_dict = report.to_dict(orient="records")
report.to_json("c:/users/h.damavandi/desktop/schema_profile.json")


import json
from pathlib import Path

output_path = Path("c:/users/h.damavandi/desktop/schema_profile.json")
output_path.write_text(
    json.dumps(
        report_dict,
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    ),
    encoding="utf-8",
)






# import warnings
# import jdatetime
# import pandas as pd
# from utils.database import make_connection
# warnings.filterwarnings("ignore")
# db_conn = make_connection()
#
#
# funds_all = pd.read_sql("SELECT * FROM [nooredenadb].[fipiran].[funds_all]", db_conn)
#
#
# for i in range(len(funds_all)):
#
#     funds_all.date[i] = jdatetime.datetime.fromgregorian(
#         year=funds_all.date[i].year,
#         month=funds_all.date[i].month,
#         day=funds_all.date[i].day
#     ).strftime("%Y/%m/%d")
#
#     funds_all.rankLastUpdate[i] = jdatetime.datetime.fromgregorian(
#         year=funds_all.rankLastUpdate[i].year,
#         month=funds_all.rankLastUpdate[i].month,
#         day=funds_all.rankLastUpdate[i].day
#     ).strftime("%Y/%m/%d")
#
#     try:
#         funds_all.initiationDate[i] = jdatetime.datetime.fromgregorian(
#             year=funds_all.initiationDate[i].year,
#             month=funds_all.initiationDate[i].month,
#             day=funds_all.initiationDate[i].day
#         ).strftime("%Y/%m/%d")
#     except ValueError:
#         funds_all.initiationDate[i] = ""
#
# funds_all.to_excel("c:users/h.damavandi/desktop/funds_all.xlsx", index=False)
#






























data = [

    {"regNo": 100, "groupId": 1, "name": "A"},
    {"regNo": 100, "groupId": 1, "name": "C", "insCode": 98765},

    {"regNo": 200, "groupId": 2, "name": "B", "insCode": 12345},
    {"regNo": 200, "groupId": 2, "name": "D"},

    {"regNo": 300, "groupId": 3, "name": "E"},
    {"regNo": 300, "groupId": 3, "name": "F"},
]

result = remove_duplicates(data)

for row in result:
    print(row)

