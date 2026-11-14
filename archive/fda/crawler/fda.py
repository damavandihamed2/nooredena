import random
import logging
import warnings
import requests as rq
from time import sleep
from typing import Optional
from pyodbc import Connection
from bs4 import BeautifulSoup, ResultSet


warnings.filterwarnings('ignore')

fda_url = "https://irc.fda.gov.ir"
fda_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.7', 'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
    'Pragma': 'no-cache', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-User': '?1', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'none', 'Upgrade-Insecure-Requests': '1',
    'sec-ch-ua': '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"', 'sec-ch-ua-platform': '"Windows"',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/150.0.0.0 Safari/537.36', 'sec-ch-ua-mobile': '?0',
}


def label_fa_to_en(label: str) -> str:
    label = " ".join(label.replace(":", "").split())
    match label:
        case "کد ژنریک": return "generic_code"
        case "قیمت هر بسته": return "package_price"
        case "بسته بندی": return "package"
        case "صاحب پروانه": return "license_owner"
        case "صاحب برند": return "brand_owner"
        case 'کد فرآورده': return "product_code"
        case "نام عمومی": return "general_name"
        case "نام": return "name"
        case "شکل دارویی": return "dosage_form"
        case "نحوه مصرف": return "roa"
        case "تولید کننده": return "producer"
        case "تاریخ اعتبار پروانه": return "license_date"
        case "قیمت مصرف کننده هر بسته": return "consumer_package_price"
        case "تعداد در بسته": return "package"
        case "قیمت واحد": return "price"
        case "GTIN": return "GTIN"
        case "IRC": return "IRC"
        case _: raise ValueError(f"Invalid label: {label}")


def get_fda_cookies() -> dict:
    response = rq.get(f'{fda_url}/NFI', headers=fda_headers)
    cookie_dict = response.cookies.get_dict()
    return cookie_dict


def request_handler(url: str, cookies: dict, time_out: int, max_tries: int, logger: logging.Logger):
    url_id = url.split("/")[-1]
    base_delay = 1
    for attempt in range(1, max_tries + 1):
        try:
            response = rq.get(url=url, headers=fda_headers, cookies=cookies, timeout=time_out)
            if response.status_code in (200, 500):
                return response
        except Exception as e:
            logger.error(f"{url_id=} | {attempt=} | {e}")
        if attempt < max_tries:
            sleep_time = (base_delay * (2 ** (attempt - 1))) * random.uniform(0.5, 1.5)
            logger.info(f"{url_id=} | sleeping {sleep_time:.2f}s before retry")
            sleep(sleep_time)
    return None


def search_atc(
        atc: str, cookies: dict, logger: logging.getLogger,
        page: int = 1, page_size: int = 9999,
        max_tries: int = 5, time_out: int = 15
) -> Optional[list[dict]]:

    link = f"{fda_url}/NFI/SearchByATC?Term={atc}&PageNumber={page}&PageSize={page_size}"
    response = request_handler(url=link, cookies=cookies, logger=logger, max_tries=max_tries, time_out=time_out)

    if response is None:
        print(f"Unable to get data for ATC ({atc})")
        return None

    try:
        response_text = response.text
        soup = BeautifulSoup(response_text, features="html.parser")
        trs = soup.find_all("tr")
        tmp_medicine = []
        for tr in trs:
            temp = {}
            persian_name = tr.find_all(
                "span", class_="col-lg-12 col-md-12 col-sm-12 col-xs-12 titleSearch-Link-RtlAlter")
            latin_name = tr.find_all(
                "span", class_="col-lg-12 col-md-12 col-sm-12 col-xs-12 titleSearch-Link-ltrAlter")
            rows = tr.find_all("div", class_="col-lg-4 col-md-4 col-sm-6 col-xs-12")
            temp["link"] = fda_url + persian_name[0].find_all("a")[0].attrs["href"]
            temp["name_persian"] = " ".join(persian_name[0].text.split())
            temp["name_latin"] = " ".join(latin_name[0].text.split())
            for j in range(len(rows)):
                label = " ".join(rows[j].find_all("label")[0].text.replace(":", "").split())
                label = label_fa_to_en(label)
                if len(rows[j].find_all("span")) > 0: value = " ".join(rows[j].find_all("span")[0].text.split())
                else: value = " ".join(rows[j].find_all("bdo")[0].text.split())
                temp[label] = value
            tmp_medicine.append(temp)
    except Exception as e:
        print(f"Unable to parse data for ATC ({atc})")
        logger.error(e)
        return None

    return tmp_medicine


def get_page_divs(soup_obj: BeautifulSoup) -> dict[str, ResultSet]:
    div_1 = soup_obj.find_all(name="div", class_="col-lg-5 col-md-5 col-sm-6 col-xs-12")
    div_2 = soup_obj.find_all(name="div", class_="col-lg-7 col-md-7 col-sm-12 col-xs-12")
    div_3 = soup_obj.find_all(name="div", class_="col-lg-5 col-md-5 col-sm-12 col-xs-12")
    atc = soup_obj.find_all(name="div", class_="row graphBox")
    if (len(atc) > 0) and (len(div_1) > 0) and (len(div_2) > 0) and (len(div_3) > 0):
        return {"div_1": div_1, "div_2": div_2, "div_3": div_3, "atc": atc}

def get_link_divs(response: rq.Response, logger: logging.getLogger):
    if not response:
        return None
    if response.status_code != 200:
        return None
    else:
        try:
            soup = BeautifulSoup(response.text, features="html.parser")
            divs = get_page_divs(soup_obj=soup)
            return divs
        except Exception as e:
            logger.error(e)
            return None

def parse_div_1(div: ResultSet) -> dict[str, str]:
    data = {}
    for i in range(len(div) - 1):
        label = label_fa_to_en(div[i].find_all("label")[0].text)
        has_span = len(div[i].find_all("span")) > 0
        span = div[i].find_all("span")[0].text if has_span else div[i].find_all("bdo")[0].text
        span = " ".join(span.split())
        data[label] = span
    return data

def parse_div_2_3(div: ResultSet) -> dict[str, str]:
    data = {}
    label = label_fa_to_en(div[0].find_all("label")[0].text)
    has_span = len(div[0].find_all("span")) > 0
    span = div[0].find_all("span")[0].text if has_span else div[0].find_all("bdo")[0].text
    span = " ".join(span.split())
    data[label] = span
    return data

def parse_atc(atc: ResultSet) -> dict[str, str]:
    data = {"ATC": atc[-1].find_all("a")[1].text.upper()}
    for j in range(len(atc)):
        data[f"ATC_{j + 1}"] = atc[j].find_all("a")[0].text.upper()
    return data

def parse_page_data(divs: dict[str, ResultSet]):
    if not divs:
        return None
    div_1, div_2, div_3, atc = divs["div_1"], divs["div_2"], divs["div_3"], divs["atc"]
    div_1_data = parse_div_1(div_1)
    div_2_data = parse_div_2_3(div_2)
    div_3_data = parse_div_2_3(div_3)
    atc_data = parse_atc(atc=atc)
    data = {**div_1_data, **div_2_data, **div_3_data, **atc_data}
    return data

def get_medicine_data(link: str, cookies: dict, logger: logging.Logger, max_tries: int = 5, time_out: int = 15):
    response = request_handler(url=link, cookies=cookies, logger=logger, max_tries=max_tries, time_out=time_out)
    divs = get_link_divs(response=response, logger=logger)
    data = parse_page_data(divs=divs)
    if data:
        data["link"] = link
        return data
    else:
        return None

def get_table_last_id(datatable_name: str, db_conn: Connection) -> int:
    name_table = datatable_name.replace("[", "").replace("]", "")
    name_col = name_table[:-1] if name_table[-1] == "s" else name_table
    name_col = name_col if name_col != "ROA" else "roa"
    crsr = db_conn.cursor()
    crsr.execute(f"SELECT MAX({name_col}_id) FROM [nooredenadb].[fda].[{name_table}]")
    max_id = crsr.fetchall()[0][0]
    crsr.close()
    return max_id

def update_brand_owners_table(new_brand_owners_list: list[str], db_conn: Connection) -> None:
    max_id = get_table_last_id(datatable_name="brand_owners", db_conn=db_conn)
    next_id = max_id + 1
    for brand_owner in new_brand_owners_list:
        crsr = db_conn.cursor()
        crsr.execute(f"INSERT INTO [nooredenadb].[fda].[brand_owners] (brand_owner, brand_owner_id) "
                     f"VALUES ('{brand_owner}', {next_id})")
        next_id += 1
    crsr.close()

def update_license_owners_table(new_license_owners_list: list[str], db_conn: Connection) -> None:
    max_id = get_table_last_id(datatable_name="license_owners", db_conn=db_conn)
    next_id = max_id + 1
    for license_owner in new_license_owners_list:
        crsr = db_conn.cursor()
        crsr.execute(f"INSERT INTO [nooredenadb].[fda].[license_owners] (license_owner, license_owner_id) "
                     f"VALUES ('{license_owner}', {next_id})")
        next_id += 1
    crsr.close()

def update_producers_table(new_producers_list: list[str], db_conn: Connection) -> None:
    max_id = get_table_last_id(datatable_name="producers", db_conn=db_conn)
    next_id = max_id + 1
    for producer in new_producers_list:
        crsr = db_conn.cursor()
        crsr.execute(f"INSERT INTO [nooredenadb].[fda].[producers] (producer, producer_id) "
                     f"VALUES ('{producer}', {next_id})")
        next_id += 1
    crsr.close()

def update_roas_table(new_roas_list: list[str], db_conn: Connection) -> None:
    max_id = get_table_last_id(datatable_name="ROAs", db_conn=db_conn)
    next_id = max_id + 1
    for roa in new_roas_list:
        crsr = db_conn.cursor()
        crsr.execute(f"INSERT INTO [nooredenadb].[fda].[ROAs] (roa, roa_id) "
                     f"VALUES ('{roa}', {next_id})")
        next_id += 1
    crsr.close()

def update_dosage_forms_table(new_dosage_forms_list: list[str], db_conn: Connection) -> None:
    max_id = get_table_last_id(datatable_name="dosage_forms", db_conn=db_conn)
    next_id = max_id + 1
    for dosage_form in new_dosage_forms_list:
        crsr = db_conn.cursor()
        crsr.execute(f"INSERT INTO [nooredenadb].[fda].[dosage_forms] (dosage_form, dosage_form_id) "
                     f"VALUES ('{dosage_form}', {next_id})")
        next_id += 1
    crsr.close()

def update_atcs_table(new_atcs_list: list[dict[str, str]], db_conn: Connection) -> None:
    max_id = get_table_last_id(datatable_name="ATCs", db_conn=db_conn)
    next_id = max_id + 1
    for atc in new_atcs_list:
        crsr = db_conn.cursor()
        crsr.execute(
            f"INSERT INTO [nooredenadb].[fda].[ATCs] (ATC_id, ATC, ATC_1, ATC_2, ATC_3, ATC_4, ATC_5) "
            f"VALUES ({next_id}, '{atc["ATC"]}', '{atc["ATC_1"]}', '{atc["ATC_2"]}', "
            f"'{atc["ATC_3"]}', '{atc["ATC_4"]}', '{atc["ATC_5"]}')")
        next_id += 1
    crsr.close()



if __name__ == "__main__":

    import os, sys
    from utils.custom_logger import get_logger
    from utils.database import make_connection

    db_conn = make_connection()

    logger = get_logger(
        db_connection=db_conn,
        file_name=os.path.basename(__file__),
        project_dir=os.path.dirname(os.path.abspath(sys.argv[0]))
    )

    atc_search_result = search_atc(atc="A", page=1, page_size=9999, logger=logger)

    medicine__link = f"{fda_url}/NFI/Detail/1766"
    medicine_data = get_medicine_data(link=medicine__link, logger=logger)

    error500_link = f"{fda_url}/NFI/Detail/5124"
    error500_data = get_medicine_data(link=error500_link, logger=logger)
