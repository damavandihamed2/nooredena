import json
import random
import warnings
import jdatetime
import numpy as np
import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep
from persiantools import digits

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36"}

def truncate_insert_table(database_table: str, dataframe: pd.DataFrame) -> None:
    crsr = db_conn.cursor()
    crsr.execute(f"TRUNCATE TABLE {database_table}")
    crsr.close()
    database.insert_to_database(dataframe=dataframe, database_table=f"{database_table}")

query_companies = ("SELECT * FROM [nooredenadb].[codal].[companies] WHERE company_type=1 "
                   "AND company_state IN (0, 1, 2) ORDER BY company")
companies = pd.read_sql(query_companies, db_conn)

query_symbols = ("SELECT [symbol] , [symbol_id] FROM [nooredenadb].[tsetmc].[symbols] WHERE active=1"
                 " AND final_last_date!=0 ORDER BY symbol, active, final_last_date")
symbols = pd.read_sql(query_symbols, db_conn)

symbols["company"] = symbols["symbol"].replace({"ي": "ی", "ك": "ک"}, regex=True, inplace=False)

companies_ = companies.merge(symbols, on="company", how="left")
companies_na = companies_[companies_["symbol_id"].isna()]

companies__ = companies_[~companies_["symbol_id"].isna()]
tsetmc_mapper = pd.read_sql("SELECT * FROM [nooredenadb].[codal].[tsetmc_mapper]", db_conn)
tsetmc_mapper = tsetmc_mapper[["company", "symbol"]].rename({"symbol": "tsetmc_symbol"}, axis=1, inplace=False)

companies___ = companies__.merge(tsetmc_mapper, on="company", how="left")

companies____ = companies___[companies___.duplicated(subset=["company"], keep=False)]


companies__.drop_duplicates(subset="company", keep="first", inplace=True, ignore_index=True)

###################################################################################################################

header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36"}
res = rq.get(f"https://search.codal.ir/api/search/v2/q?", headers=header)
total = json.loads(res.text)["Total"]
page = json.loads(res.text)["Page"]

codal_df = pd.DataFrame()
for i in range(82, 100):
    res = rq.get(f"https://search.codal.ir/api/search/v2/q?PageNumber={i + 1}", headers=header)
    codal_data = pd.DataFrame(json.loads(res.text)["Letters"])
    codal_df = pd.concat([codal_df, codal_data], axis=0, ignore_index=True)
    sleep(random.random() * 3)
    print(f"{round(100 * (i + 1) / 100, ndigits=1)}%  Completed")

codal_df.to_excel("C:/Users/damavandi/Desktop/codal_df.xlsx", index=False)

codal_df["PdfUrl"] = "https://codal.ir/" + codal_df["PdfUrl"]
codal_df["AttachmentUrl"] = "https://codal.ir" + codal_df["AttachmentUrl"]
codal_df["Url"] = "https://codal.ir" + codal_df["Url"]

codal_data.drop(labels="TedanUrl", axis=1, inplace=True)

"LetterType=-1"
"Audited=true"
"AuditorRef=-1"
"NotAudited=true"
"IsNotAudited=false"
"Category=-1"
"CompanyState=-1"
"CompanyType=-1"
"Childs=true"
"Consolidatable=true"
"Length=-1"
"Mains=true"
"NotConsolidatable=true"
"Publisher=false"
"TracingNo=-1"
"search=false"
"PageNumber=1"


###################################################################################################################

import json
import random
import warnings
import numpy as np
import pandas as pd
import requests as rq
from tqdm import tqdm
from time import sleep
from persiantools import digits

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36"}

query_tsetmc_codal_mapper = "SELECT * FROM [nooredenadb].[codal].[tsetmc_mapper] WHERE company_id IS NOT NULL"
query_investing_companies = ("SELECT  *  FROM [nooredenadb].[tsetmc].[symbols] where active=1 and final_last_date > "
                             "20200101 and sector IN (56, 39) and ([total_share] * [final_price]) > 15e12")

tsetmc_codal_mapper = pd.read_sql(query_tsetmc_codal_mapper, db_conn)
investing_companies = pd.read_sql(query_investing_companies, db_conn)

companies = investing_companies[["symbol", "symbol_name", "symbol_id"]].merge(
    tsetmc_codal_mapper[["symbol_id", "company", "company_name", "company_id"]], on="symbol_id", how="left")
companies["query"] = companies["symbol_name"].str.contains("استان ")
companies = companies[~companies["query"]].reset_index(drop=True, inplace=False)


url_letters = "https://search.codal.ir/api/search/v2/q?&Symbol={symbol}&search=true&Category=3&LetterType=58&PageNumber=1"

df_let = pd.DataFrame()
for s in tqdm(range(len(companies))):
    s = 15
    symbol = companies["company"].iloc[s]
    res = rq.get(url=url_letters.format(symbol=symbol), headers=header)
    res = res.json()
    if res["Total"] > 0:
        letters = pd.DataFrame(res["Letters"])
        sleep(random.randint(100, 301) / 100)
        res_ = rq.get(url=f"https://search.codal.ir/api/search/v2/q?&Symbol={symbol}&search=true&LetterType=58&PageNumber=2", headers=header)
        res_ = res_.json()
        letters_ = pd.DataFrame(res_["Letters"])
        letters = pd.concat([letters, letters_], axis=0, ignore_index=True)

    df_let = pd.concat([df_let, letters], axis=0, ignore_index=True)
    sleep(random.randint(1, 301)/100)

    last_letter = letters.iloc[0]
    digits.fa_to_en(last_letter["PublishDateTime"][:10])

    last_letter_url = "https://codal.ir/" + last_letter["Url"]

    res_ = rq.get(url=last_letter_url + "&sheetId=4", headers=header)
    res__ = res_.text.split("var datasource = ")[1].split(";\r\n\r\n\r\n</script>")[0]
    res__ = json.loads(res__)

    res__ = pd.DataFrame(res__["sheets"][0]["tables"][1]["cells"])
    res__["col"] = res__["address"].str.extract('(\D+)')
    res__["row"] = res__["address"].str.extract('(\d+)').astype(int)
    res__.sort_values(["col", "row"], ignore_index=True, inplace=True)

res__ = res__[res__["isVisible"]].reset_index(drop=True, inplace=False)
res__ = res__[res__["cssClass"] == ""].reset_index(drop=True, inplace=False)
table = pd.DataFrame(columns=res__["col"].unique().tolist(), index=range(res__["row"].max()))
for i in range(len(res__)):
    c_ = res__["col"].iloc[i]
    r_ = res__["row"].iloc[i]
    v_ = res__["value"].iloc[i]
    table[c_].iloc[r_ - 1] = v_
table.iloc[0, :].replace({"نام شرکت": "symbol_name", 'سرمایه (میلیون ریال)': "capital_mr",
                          'ارزش اسمی هر سهم (ریال)': "par_value", 'ابتدای دوره': "start",
                          'تغییرات': "change", 'انتهای دوره': "end"}, inplace=True)
table.iloc[1, :].replace({"تعداد سهام": "amount", "بهای تمام شده": "cost", "ارزش بازار": "value",
                          "درصد مالکیت": "ownership", "بهای تمام شده هر سهم (ریال)": "share_cost",
                          "ارزش هر سهم (ریال)": "final_price", "افزایش (کاهش)": "surplus_value"}, inplace=True)
table.columns = (table.iloc[0, :].fillna(method="ffill", inplace=False) + "_" +
                 table.iloc[1, :].fillna("nan", inplace=False)
                 ).replace({"nan_": "", "_nan": ""}, inplace=False, regex=True)
table["symbol_name"].replace('', np.nan, inplace=True, regex=False)
table.dropna(subset="symbol_name", inplace=True, ignore_index=True)
table = table[table["symbol_name"] != "symbol_name"].reset_index(drop=True, inplace=False)

####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################

import random
import warnings
import jdatetime
import pandas as pd
from tqdm import tqdm
import requests as rq
from time import sleep

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
db_conn = make_connection()

headers_ = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36"}
col_mapper = {
        "TracingNo": "tracing_number", "Symbol": "company", "IsEstimate": "is_estimate", "Url": "url", "Reasons": "reasons",
        "CompanyName": "company_name", "UnderSupervision": "under_supervision", "Title": "title", "HasHtml": "has_html",
        "HasExcel": "has_excel", "HasPdf": "has_pdf", "HasAttachment": "has_attachment", "AttachmentUrl": "attachment_url",
        "PdfUrl": "pdf_url", "ExcelUrl": "excel_url", "AdditionalInfo": "additional_info", "LetterCode": "letter_code"}


def get_pages_number():
    url_ = "https://search.codal.ir/api/search/v2/q?"
    response = rq.get(url=url_, headers=headers_)
    response_json = response.json()
    return {"total": response_json["Total"], "pages": response_json["Page"]}


def get_reports_by_pages(page: int = 1):
    url_ = f"https://search.codal.ir/api/search/v2/q?&search=false&PageNumber={page}"
    while True:
        try:
            response = rq.get(url=url_, headers=headers_)
            if response.status_code == 200:
                break
            else:
                print(f"Response Status Code is: {response.status_code} Trying again...")
                sleep(random.randint(500, 1001) / 100)
        except Exception as e:
            print(e)
            sleep(random.randint(500, 1001) / 100)
    response_json = response.json()
    total_, pages_, letters_ = response_json["Total"], response_json["Page"], response_json["Letters"]
    return {"total": total_, "pages": pages_, "letters": letters_}


def cleaning_reports(reports_list: list[dict]):
    letters_df = pd.DataFrame(reports_list)
    letters_df["sent_date"] = letters_df["SentDateTime"].apply(
        lambda x: jdatetime.datetime.strptime(x, "%Y/%m/%d %H:%M:%S").strftime("%Y/%m/%d"))
    letters_df["sent_time"] = letters_df["SentDateTime"].apply(
        lambda x: jdatetime.datetime.strptime(x, "%Y/%m/%d %H:%M:%S").strftime("%H:%M:%S"))
    letters_df["publish_date"] = letters_df["PublishDateTime"].apply(
        lambda x: jdatetime.datetime.strptime(x, "%Y/%m/%d %H:%M:%S").strftime("%Y/%m/%d"))
    letters_df["publish_time"] = letters_df["PublishDateTime"].apply(
        lambda x: jdatetime.datetime.strptime(x, "%Y/%m/%d %H:%M:%S").strftime("%H:%M:%S"))
    letters_df.drop(
        columns=["SentDateTime", "PublishDateTime", "TedanUrl", "UnderSupervision", "HasXbrl", "XbrlUrl"],
        inplace=True
    )
    super_vision = pd.DataFrame(letters_df["SuperVision"].values.tolist())
    super_vision["Reasons"] = super_vision["Reasons"].apply(lambda x: " _ ".join(x) if x != [] else "")
    letters_df = pd.concat([letters_df, super_vision], axis=1)
    letters_df.drop(columns=["SuperVision"], inplace=True)
    letters_df.rename(columns=col_mapper, inplace=True)
    dtype_mapper = {"has_html": int, "has_excel": int, "has_pdf": int, "is_estimate": int, "has_attachment": int}
    letters_df = letters_df.astype(dtype_mapper)
    return letters_df

total_ = get_pages_number()["pages"]
# total_["pages"] + 1

letters_list = []
for p in tqdm(range(9201, 26401 + 1)):
    reports_ = get_reports_by_pages(page=p)
    if len(reports_["letters"]) == 0:
        break
    else:
        letters_list.extend(reports_["letters"])
        if len(letters_list) >= 1000:
            letters_df = cleaning_reports(reports_list=letters_list)
            insert_to_database(dataframe=letters_df, database_table="[nooredenadb].[codal].[all_reports]", loading=False)
            letters_list = []
        else:
            pass
        # sleep(random.randint(100, 301)/100)

# letters_df.to_pickle("c:/users/h.damavandi/desktop/letters.pkl")
# letters_df = pd.read_pickle("c:/users/h.damavandi/desktop/letters.pkl")


# letters_df = cleaning_reports(reports_list=letters_list)
# database.insert_to_database(dataframe=letters_df, database_table="[nooredenadb].[codal].[all_reports]")


####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################

# if __name__ == '__main__':
#
#     financialYears = rq.get("https://search.codal.ir/api/search/v1/financialYears", headers=header)
#     financialYears = pd.DataFrame(financialYears.json())
#     financialYears.columns = ["financial_year"]
#     truncate_insert_table(database_table="[nooredenadb].[codal].[financial_year]", dataframe=financialYears)
#
#     auditors = rq.get("https://search.codal.ir/api/search/v1/auditors", headers=header)
#     auditors = pd.DataFrame(auditors.json())
#     auditors.rename({"n": "auditor_name", "c": "auditor_id"}, axis=1, inplace=True)
#     truncate_insert_table(database_table="[nooredenadb].[codal].[auditors]", dataframe=auditors)
#
#     industry_group = rq.get("https://search.codal.ir/api/search/v1/IndustryGroup", headers=header)
#     industry_group = pd.DataFrame(industry_group.json())
#     industry_group.rename({"Name": "industry_name", "Id": "industry_id"}, axis=1, inplace=True)
#     truncate_insert_table(database_table="[nooredenadb].[codal].[industry_group]", dataframe=industry_group)
#
#     companies = rq.get("https://search.codal.ir/api/search/v1/companies", headers=header)
#     companies = pd.DataFrame(companies.json())
#     companies.rename({"sy": "company", "n": "company_name", "i": "company_id", "t": "company_type",
#                       "st": "company_state", "IG": "industry_id", "RT": "report_type"}, axis=1, inplace=True)
#     database.insert_to_database(dataframe=companies, database_table="[nooredenadb].[codal].[companies]")
#     truncate_insert_table(database_table="[nooredenadb].[codal].[companies]", dataframe=companies)
#     # [sy (company) : نماد] [n (company_name) : نام] [i (company_id) : id] [t (company_type) : نوع شزکت]
#     # [st (company_state) : وضعیت ناشر] [IG (industry_id): گروه صنعتی] [RT (report_type): ماهیت شرکت]
#
#     categories = rq.get("https://search.codal.ir/api/search/v1/categories", headers=header)
#     categories = pd.DataFrame(categories.json())
#     categories_df = pd.DataFrame()
#     for i in range(1, len(categories)):
#         pub_lett_type = pd.DataFrame(categories["PublisherTypes"].iloc[i]).rename(
#             {"Code": "publisher_code", "Name": "publisher_name"}, axis=1, inplace=False)
#         temp = pd.DataFrame()
#         for j in range(len(pub_lett_type)):
#             lett_type = pd.DataFrame(pub_lett_type["LetterTypes"].iloc[j]).rename(
#                 {"Id": "letter_id", "Name": "letter_name", "Code": "letterCode"}, axis=1, inplace=False)
#             lett_type["publisher_code"] = pub_lett_type["publisher_code"].iloc[j]
#             lett_type["publisher_name"] = pub_lett_type["publisher_name"].iloc[j]
#             lett_type = lett_type[["publisher_code", "publisher_name", "letter_id", "letter_name"]]
#             temp = pd.concat([temp, lett_type], axis=0, ignore_index=True)
#         temp[["category_code", "category_name"]] = categories["Code"].iloc[i], categories["Name"].iloc[i]
#         temp = temp[["category_code", "category_name", "publisher_code", "publisher_name", "letter_id", "letter_name"]]
#         categories_df = pd.concat([categories_df, temp], axis=0, ignore_index=True)
#     truncate_insert_table(database_table="[nooredenadb].[codal].[categories]", dataframe=categories_df)
#
#     company_types_df = pd.DataFrame([
#         {"company_type": -1, "company_type_name": "شرکت های بخش عمومی و سایر"},
#         {"company_type": 1, "company_type_name": "ناشران"},
#         {"company_type": 2, "company_type_name": "کارگزاران"},
#         {"company_type": 3, "company_type_name": "نهاد مالی"},
#         {"company_type": 4, "company_type_name": "نهاد عمومی"},
#         {"company_type": 5, "company_type_name": "شرکت دولتی"}
#     ])
#     truncate_insert_table(database_table="[nooredenadb].[codal].[company_type]", dataframe=company_types_df)
#
#     company_states_df = pd.DataFrame([
#         {'company_state': -1, 'company_state_name': 'همه'},
#         {'company_state': 0, 'company_state_name': 'پذیرفته شده در بورس تهران'},
#         {'company_state': 1, 'company_state_name': 'پذیرفته شده در فرابورس ایران'},
#         {'company_state': 2, 'company_state_name': 'ثبت شده پذیرفته نشده'},
#         {'company_state': 3, 'company_state_name': 'ثبت نشده نزد سازمان'},
#         {'company_state': 4, 'company_state_name': 'پذیرفته شده در بورس کالای ایران'},
#         {'company_state': 5, 'company_state_name': 'پذیرفته شده در بورس انرژی ایران'}
#     ])
#     truncate_insert_table(database_table="[nooredenadb].[codal].[company_state]", dataframe=company_states_df)
#
#     report_types_df = pd.DataFrame([
#         {'report_type': 1000000, 'report_type_name': 'تولیدی'},
#         {'report_type': 1000001, 'report_type_name': 'ساختمانی'},
#         {'report_type': 1000002, 'report_type_name': 'سرمایه گذاری'},
#         {'report_type': 1000003, 'report_type_name': 'بانک'},
#         {'report_type': 1000004, 'report_type_name': 'لیزینگ'},
#         {'report_type': 1000005, 'report_type_name': 'خدماتی'},
#         {'report_type': 1000006, 'report_type_name': 'بیمه'},
#         {'report_type': 1000007, 'report_type_name': 'حمل  نقل ریلی'},
#         {'report_type': 1000008, 'report_type_name': 'کشاورزی'},
#         {'report_type': 1000009, 'report_type_name': 'تامین سرمایه'}
#     ])
#     truncate_insert_table(database_table="[nooredenadb].[codal].[report_type]", dataframe=report_types_df)
#
