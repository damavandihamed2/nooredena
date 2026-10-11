import json
import datetime
import jdatetime
import requests as rq
from typing import Literal, Optional

from utils import auth_token_hadler



sigma_ip = "sigma"

base_header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en,en-US;q=0.9',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    'Cache-Control': 'no-cache'
}

base_header_2 = {
    **base_header,
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': f'http://{sigma_ip}',
    'Host': f'{sigma_ip}',
    "Accept-Encoding": "gzip, deflate",
    'Upgrade-Insecure-Requests': '1'
}

headers_portfolio = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36'
}

report_types_mapper = {"portfolio": "Asset_AssetStatusGrid", "dividend": "Stock_ReceivedDividendEventsGrid",
                       "buysell": "Asset_BuySellReportEventsGrid", "profitloss": "Asset_ProfitLossEventsGrid"}

payload_login = ("ReturnUrl=%2F&Username={username}&Password={password}&captcha={captcha_value}&"
                 "captcha-guid={captcha_guid}&__RequestVerificationToken={payload_token}")
payload_dividend = ("Run=True&WatchListIds=&StartDateTime={start_date}&CompanyId=&EndDateTime={end_date}&MeetingId=&"
                    "SeparateByPortfolio=False&ReportType=&PortfolioIds={portfolio_id}")
payload_buysell = ("ExportType=True&Run=True&WatchListIds=&PortfolioIds={portfolio_id}&StartDateTime={start_date}&"
                   "EndDateTime={end_date}&BuySellReportGroupType=&AssetIds=&SeparateByPortfolio=&BuySellActionType=&"
                   "__RequestVerificationToken={payload_token}")
payload_profitloss = ("Run=True&WatchListIds=&StartDateTime={start_date}&AssetId=&EndDateTime={end_date}&"
                      "ProfitlossReportGroupType=&PortfolioIds={portfolio_id}")


def date_input_handler(input_date):
    errortext1 = "The String Date must be in in these formats ('YYYY-MM-DD'), ('YYYY/MM/DD'), ('YYYYMMDD')"
    errortext2 = "The Numeric Date can't be a Float!"
    errortext3 = "The Numeric Date must be in in this format (YYYYMMDD)"
    errortext4 = "The Date must be an Integer or String!"
    errortext5 = "The Date could not be in the future"
    if type(input_date) == str:
        if ("/" in input_date) or ("-" in input_date):
            if len(input_date) != 10:
                raise ValueError(errortext1)
        else:
            if (not input_date.isdigit()) or (len(input_date) != 8):
                raise ValueError(errortext1)
    elif (type(input_date) == int) or (type(input_date) == float):
        if type(input_date) == float:
            if input_date % 1 > 0:
                raise ValueError(errortext2)
            else:
                input_date = int(input_date)
        input_date = str(input_date)
        if len(input_date) != 8:
            raise ValueError(errortext3)
    else:
        raise ValueError(errortext4)
    input_date = input_date.replace("-", "").replace("/", "")
    if int(input_date) > 15000000:
        input_date = datetime.datetime.strptime(input_date, "%Y%m%d")
        if input_date > datetime.datetime.now():
            raise ValueError(errortext5)
        else:
            input_date = jdatetime.datetime.fromtimestamp(input_date.timestamp())
            input_date = input_date.strftime("%Y/%m/%d")
    else:
        input_date = jdatetime.datetime.strptime(input_date, "%Y%m%d")
        if input_date > jdatetime.datetime.now():
            raise ValueError(errortext5)
        else:
            input_date = input_date.strftime("%Y/%m/%d")
    return input_date

def get_cookies() -> dict[str, str]:
    cookies = auth_token_hadler.get_tokens(app="sigma", web_address="sigma")
    if cookies:
        return cookies
    return {"session_token": "", ".AspNetCore.Antiforgery.k3DVaEHqH6g": ""}

def update_cookies(cookies_dict: dict[str, str]) -> None:
    auth_token_hadler.update_tokens(app="sigma", web_address="sigma", json_data=cookies_dict)


def report_data_parser(
        url: str,
        response: rq.Response,
        report_type: Literal[*list(report_types_mapper.keys())]
) -> Optional[list[dict]]:
    if (response.status_code == 200) and (response.url == url):
        report_type_ = report_types_mapper[report_type]
        data = response.text.split(
            f"DataGrid('Reports_Views_{report_type_}', ")[1].split(", (getFromCache === true")[0]
        data = json.loads(data)["LocalData"]
        data = [{kv['Name']: kv['Value'] for kv in _["GridDataFields"]} for _ in data]
        return data
    else:
        print(f"Get {report_type} data failed, try login again!")
        return None

