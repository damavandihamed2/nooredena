import json
import random
import warnings
import jdatetime
import numpy as np
import pandas as pd
import requests as rq
from time import sleep

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36"}

####################################################################################################################

# header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
#                         "Chrome/116.0.0.0 Safari/537.36"}
# res = rq.get(f"https://search.codal.ir/api/search/v2/q?", headers=header)
# total = json.loads(res.text)["Total"]
# page = json.loads(res.text)["Page"]
#
# codal_df = pd.DataFrame()
# for i in range(82, 100):
#     res = rq.get(f"https://search.codal.ir/api/search/v2/q?PageNumber={i + 1}", headers=header)
#     codal_data = pd.DataFrame(json.loads(res.text)["Letters"])
#     codal_df = pd.concat([codal_df, codal_data], axis=0, ignore_index=True)
#     sleep(random.random() * 3)
#     print(f"{round(100 * (i + 1) / 100, ndigits=1)}%  Completed")
#
# codal_df.to_excel("C:/Users/damavandi/Desktop/codal_df.xlsx", index=False)
#
# codal_df["PdfUrl"] = "https://codal.ir/" + codal_df["PdfUrl"]
# codal_df["AttachmentUrl"] = "https://codal.ir" + codal_df["AttachmentUrl"]
# codal_df["Url"] = "https://codal.ir" + codal_df["Url"]
#
# codal_data.drop(labels="TedanUrl", axis=1, inplace=True)

# "Audited=true"
# "AuditorRef=-1"
# "Category=-1"
# "Childs=true"
# "CompanyState=-1"
# "CompanyType=-1"
# "Consolidatable=true"
# "IsNotAudited=false"
# "Length=-1"
# "LetterType=-1"
# "Mains=true"
# "NotAudited=true"
# "NotConsolidatable=true"
# "PageNumber=1"
# "Publisher=false"
# "TracingNo=-1"
# "search=false"


####################################################################################################################


import warnings

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36"}




# query_tsetmc_codal_mapper = "SELECT * FROM [nooredenadb].[extra].[tsetmc_codal_mapper] WHERE company_id IS NOT NULL"
# query_investing_companies = ("SELECT  *  FROM [nooredenadb].[tsetmc].[symbols] where active=1 and final_last_date > "
#                              "20200101 and sector IN (56, 39) and ([total_share] * [final_price]) > 15e12")
#
# tsetmc_codal_mapper = pd.read_sql(query_tsetmc_codal_mapper, db_conn)
# investing_companies = pd.read_sql(query_investing_companies, db_conn)
#
# companies = investing_companies[["symbol", "symbol_name", "symbol_id"]].merge(
#     tsetmc_codal_mapper[["symbol_id", "company", "company_name", "company_id"]], on="symbol_id", how="left")
# companies["query"] = companies["symbol_name"].str.contains("استان ")
# companies = companies[~companies["query"]].reset_index(drop=True, inplace=False)

# url_letters = "https://search.codal.ir/api/search/v2/q?&Symbol={symbol}&search=true&LetterType=58&PageNumber=1"
# url_report = "https://search.codal.ir/api/search/v2/q?&Symbol={symbol}&search=true&LetterType=58&PageNumber=2"


# df_let = pd.DataFrame()
# for s in tqdm(range(len(companies))):
#     s = 15 # for example ومهان
#     symbol = companies["company"].iloc[s]
#     res = rq.get(url=url_letters.format(symbol=symbol), headers=header)
#     res = res.json()
#     letters = pd.DataFrame(res["Letters"])
#     # if res["Page"] > 1:
#     #     sleep(random.randint(1, 301) / 100)
#     #     res_ = rq.get(url=f"https://search.codal.ir/api/search/v2/q?&Symbol={symbol}&search=true&LetterType=58&PageNumber=2", headers=header)
#     #     res_ = res_.json()
#     #     letters_ = pd.DataFrame(res_["Letters"])
#     #     letters = pd.concat([letters, letters_], axis=0, ignore_index=True)
#     df_let = pd.concat([df_let, letters], axis=0, ignore_index=True)
#     sleep(random.randint(1, 301)/100)
#
#     last_letter = letters.iloc[0]
#     digits.fa_to_en(last_letter["PublishDateTime"][:10])
#
#     last_letter_url = "https://codal.ir/" + last_letter["Url"]
#
#     res_ = rq.get(url=last_letter_url + "&sheetId=4", headers=header)
#     res__ = res_.text.split("var datasource = ")[1].split(";\r\n\r\n\r\n</script>")[0]
#     res__ = json.loads(res__)
#
#     res__ = pd.DataFrame(res__["sheets"][0]["tables"][1]["cells"])
#     res__["col"] = res__["address"].str.extract('(\D+)')
#     res__["row"] = res__["address"].str.extract('(\d+)').astype(int)
#     res__.sort_values(["col", "row"], ignore_index=True, inplace=True)
#
# res__ = res__[res__["isVisible"]].reset_index(drop=True, inplace=False)
# res__ = res__[res__["cssClass"] == ""].reset_index(drop=True, inplace=False)
# table = pd.DataFrame(columns=res__["col"].unique().tolist(), index=range(res__["row"].max()))
# for i in range(len(res__)):
#     c_ = res__["col"].iloc[i]
#     r_ = res__["row"].iloc[i]
#     v_ = res__["value"].iloc[i]
#     table[c_].iloc[r_ - 1] = v_
# table.iloc[0, :].replace({"نام شرکت": "symbol_name", 'سرمایه (میلیون ریال)': "capital_mr",
#                           'ارزش اسمی هر سهم (ریال)': "par_value", 'ابتدای دوره': "start",
#                           'تغییرات': "change", 'انتهای دوره': "end"}, inplace=True)
# table.iloc[1, :].replace({"تعداد سهام": "amount", "بهای تمام شده": "cost", "ارزش بازار": "value",
#                           "درصد مالکیت": "ownership", "بهای تمام شده هر سهم (ریال)": "share_cost",
#                           "ارزش هر سهم (ریال)": "final_price", "افزایش (کاهش)": "surplus_value"}, inplace=True)
# table.columns = (table.iloc[0, :].fillna(method="ffill", inplace=False) + "_" +
#                  table.iloc[1, :].fillna("nan", inplace=False)
#                  ).replace({"nan_": "", "_nan": ""}, inplace=False, regex=True)
# table["symbol_name"].replace('', np.nan, inplace=True, regex=False)
# table.dropna(subset="symbol_name", inplace=True, ignore_index=True)
# table = table[table["symbol_name"] != "symbol_name"].reset_index(drop=True, inplace=False)
