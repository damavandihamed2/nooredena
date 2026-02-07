import requests as rq
from bs4 import BeautifulSoup
from typing import Literal, Optional
import json, warnings, jdatetime, datetime

from utils import captcha_handler
from utils import auth_token_hadler
from utils.database import make_connection


warnings.filterwarnings("ignore")
sigma_ip = "172.20.30.62"
base_header = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                         'image/svg+xml,image/*,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
               'Accept-Language': 'en,en-US;q=0.9', 'Connection': 'keep-alive', 'Pragma': 'no-cache',
               'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/130.0.0.0 Safari/537.36", 'Cache-Control': 'no-cache'}
base_header_2 = {**base_header, 'Content-Type': 'application/x-www-form-urlencoded', 'Origin': f'http://{sigma_ip}',
                 'Host': f'{sigma_ip}', "Accept-Encoding": "gzip, deflate", 'Upgrade-Insecure-Requests': '1'}
report_types_mapper = {"portfolio": "Asset_AssetStatusGrid", "dividend": "Stock_ReceivedDividendEventsGrid",
                       "buysell": "Asset_BuySellReportEventsGrid", "profitloss": "Asset_ProfitLossEventsGrid"}


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

def get_session_token() -> str:
    tokens = auth_token_hadler.get_tokens(app="sigma", web_address="sigma")
    if tokens:
        try:
            session_token = tokens["session_token"]
            return session_token
        except Exception:
            pass
    return ""

def update_session_token(session_token: str) -> None:
    json_data = {"session_token": session_token}
    auth_token_hadler.update_tokens(app="kaladade", web_address="kaladade", json_data=json_data)

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



class Agent:
    report_types = ["portfolio", "dividend", "buysell", "profitloss"]
    report_types_mapper = {"portfolio": "Asset_AssetStatusGrid", "dividend": "Stock_ReceivedDividendEventsGrid",
                           "buysell": "Asset_BuySellReportEventsGrid", "profitloss": "Asset_ProfitLossEventsGrid"}

    def __init__(self, username:str, password: str) -> None:

        self.username = username
        self.password = password
        self.status = False

        self.login_page_response = None
        self.login_page_payload_token = None
        self.header_token = None

        self.captcha_guid = self.captcha_id = self.captcha_value = None

        self.login_response = None
        self.session_token = None

    def __login_page(self) -> None:
        url = f"http://{sigma_ip}/login"
        self.login_page_response = rq.get(url=url)
        self.login_page_inputs = BeautifulSoup(
            self.login_page_response.text, features="html.parser").find_all(name="input", type="hidden")
        self.key_value = {}
        for i in range(len(self.login_page_inputs)):
            self.key_value[self.login_page_inputs[i].get("name")] = self.login_page_inputs[i].get("value")
        self.captcha_guid = self.key_value["captcha-guid"]
        self.login_page_payload_token = self.key_value["__RequestVerificationToken"]
        self.header_token = self.login_page_response.headers['set-cookie']

    def __check_old_tokens(self):
        sessiontoken = get_session_token()
        url = f"http://{sigma_ip}/Reports/Asset/AssetStatus/status"
        headers = {**base_header_2, 'Cookie': f'SessionToken={sessiontoken}', 'Referer': f"http://{sigma_ip}/"}
        response = rq.get(url=url, headers=headers)
        if response.status_code == 200:
            if response.url == url:
                self.status = True
                self.session_token = sessiontoken
                self.header_token = response.headers['set-cookie']

    def __get_report_page(self, url: str, payload_token_index: Literal[0, 1]) -> str:
        if payload_token_index not in [0, 1]:
            raise ValueError("payload_token_index must be 0 or 1")
        headers = {**base_header_2, 'Cookie': f'{self.header_token}; SessionToken={self.session_token}',
                   'Referer': f"http://{sigma_ip}/"}
        response = rq.get(url=url, headers=headers)
        inputs = BeautifulSoup(response.text, features="html.parser").find_all(
            name="input", attrs={"name": "__RequestVerificationToken"})
        payload_token = inputs[payload_token_index].attrs["value"]
        return payload_token

    def get_captcha(self) -> None:
        self.captcha_id = str(jdatetime.datetime.now().timestamp())
        url = f"http://{sigma_ip}/captcha" + f"?guid={self.captcha_guid}"
        captcha_headers = {**base_header, 'Cookie': f'{self.header_token}',
                           'Referer': f'http://{sigma_ip}/login?ReturnUrl=%2f'}
        captcha_response = rq.get(url=url, headers=captcha_headers)
        captcha_handler.save_captcha(
            captcha_type="sigma", captcha_image=captcha_response.content, captcha_id=self.captcha_id, b64decode=False)
        captcha_value = captcha_handler.open_captcha(captcha_type="sigma", captcha_id=self.captcha_id)
        self.captcha_value = captcha_value

    def login(self, check_old_token: bool = True) -> None:
        if self.status:
            print("You are already logged in!")
            return None
        if check_old_token:
            self.__check_old_tokens()
        if self.status:
            print("You logged in successfully!")
            return None
        self.__login_page()
        self.get_captcha()
        url = f"http://{sigma_ip}/login"
        payload = (f"ReturnUrl=%2F&Username={self.username}&Password={self.password}&"
                   f"captcha={self.captcha_value}&captcha-guid={self.captcha_guid}&"
                   f"__RequestVerificationToken={self.login_page_payload_token}")
        headers = {**base_header_2, "Cookie": f"{self.header_token}", 'Referer': url}
        self.login_response = rq.post(url=url, headers=headers, data=payload)
        if (self.login_response.status_code == 200) and (self.login_response.url == f"http://{sigma_ip}/"):
            captcha_handler.update_captcha_value(
                captcha_type="sigma", captcha_id=self.captcha_id, captcha_value=self.captcha_value)
            sessiontoken = self.login_response.history[0].cookies["SessionToken"]
            self.session_token = sessiontoken
            update_session_token(session_token=self.session_token)
            self.status = True
            print("You logged in successfully!")
            return None
        elif self.login_response.status_code != 200:
            print("Log In failed, try again!")
            return None
        else:
            soup_ = BeautifulSoup(self.login_response.text, features="html.parser").find_all(
                name="div", class_="validation-summary-errors")[0].find_next("ul").find_next("li").contents[0]
            if "کد امنیتی" in soup_: err_txt = "Log In failed (wrong captcha), try again!"
            elif "کاربری" in soup_: err_txt = "Log In failed (wrong username or password), try again!"
            else: err_txt = "Log In failed, try again!"
            print(err_txt)
            return None

    def get_portfolio(self, date: str, portfolio_id: int = 1) -> Optional[list[dict]]:
        if not self.status:
            print("You are NOT logged in, please login first!")
            return None
        date = date_input_handler(input_date=date)
        url = f"http://{sigma_ip}/Reports/Asset/AssetStatus/status"
        payload_token = self.__get_report_page(
            url=f"http://{sigma_ip}/Reports/Asset/AssetStatus/status", payload_token_index=0)
        payload = (f"__RequestVerificationToken={payload_token}&ExportType=True&Run=True&WatchListIds=&"
                   f"PortfolioIds={portfolio_id}&EndDateTime={date}&SeparateStates=True&SeparateByPortfolio=False")
        headers = {**base_header_2, 'Cookie': f'{self.header_token}; SessionToken={self.session_token}',
                   'Referer': url}
        response = rq.post(url=url, headers=headers, data=payload)
        data = report_data_parser(url=url, response=response, report_type="portfolio")
        return data

    def get_dividend(self, start_date, end_date, portfolio_id: int = 1) -> Optional[list[dict]]:
        if not self.status:
            print("You are NOT logged in, please login first!")
            return None
        start_date = date_input_handler(input_date=start_date).replace("/", "%2F")
        end_date = date_input_handler(input_date=end_date).replace("/", "%2F")
        # payload_token = self.__get_report_page(
        #     url=f"http://{sigma_ip}/Reports/Stock/ReceivedDividendEvents", payload_token_index=1)
        url = f"http://{sigma_ip}/Reports/Stock/ReceivedDividendEvents"
        payload = (f"?Run=True&WatchListIds=&StartDateTime={start_date}&CompanyId=&EndDateTime={end_date}&"
                   f"MeetingId=&SeparateByPortfolio=False&ReportType=&PortfolioIds={portfolio_id}")
        headers = {**base_header_2, 'Cookie': f'{self.header_token}; SessionToken={self.session_token}',
                   'Referer': url}
        response = rq.get(url=url + payload, headers=headers)
        data = report_data_parser(url=url + payload, response=response, report_type="dividend")
        return data

    def get_buysell(self, start_date, end_date, portfolio_id: int = 1) -> Optional[list[dict]]:
        if not self.status:
            print("You are NOT logged in, please login first!")
            return None
        start_date = date_input_handler(input_date=start_date).replace("/", "%2F")
        end_date = date_input_handler(input_date=end_date).replace("/", "%2F")
        url = f"http://{sigma_ip}/Reports/Asset/BuySellReportEvents"
        payload_token = self.__get_report_page(
            url=f"http://{sigma_ip}/reports/asset/buysellreportevents", payload_token_index=0)
        headers = {**base_header_2, 'Cookie': f'{self.header_token}; SessionToken={self.session_token}',
                   'Referer': url}
        payload = (f"__RequestVerificationToken={payload_token}&ExportType=True&Run=True&WatchListIds=&"
                   f"PortfolioIds={portfolio_id}&StartDateTime={start_date}&EndDateTime={end_date}&"
                   f"BuySellReportGroupType=&SeparateByPortfolio=&BuySellActionType=")
        response = rq.post(url=url, headers=headers, data=payload)
        data = report_data_parser(url=url, response=response, report_type="buysell")
        return data

    def get_profitloss(self, start_date, end_date, portfolio_id = 1) -> Optional[list[dict]]:
        if not self.status:
            print("You are NOT logged in, please login first!")
            return None
        start_date = date_input_handler(input_date=start_date).replace("/", "%2F")
        end_date = date_input_handler(input_date=end_date).replace("/", "%2F")
        # payload_token = self.__get_report_page(
        #     url=f"http://{sigma_ip}/reports/asset/ProfitLossEvents/ProfitLoss", payload_token_index=0)
        url = f"http://{sigma_ip}/Reports/Asset/ProfitLossEvents/ProfitLoss"
        headers = {**base_header_2, 'Cookie': f'{self.header_token}; SessionToken={self.session_token}',
                   'Referer': url}
        payload = (f"Run=True&WatchListIds=&StartDateTime={start_date}&AssetId=&EndDateTime={end_date}&"
                   f"ProfitlossReportGroupType=&PortfolioIds={portfolio_id}")
        response = rq.post(url=url, headers=headers, data=payload)
        data = report_data_parser(url=url, response=response, report_type="profitloss")
        return data


if __name__ == '__main__':

    authenticator = [{"username": "damavandi", "password": "Dd@123456"}]
    agent = Agent(username=authenticator[0]["username"], password=authenticator[0]["password"])
    agent.login()

    buysell_data = agent.get_buysell(start_date="20250601", end_date="20250729", portfolio_id=1)
    portfolio_data = agent.get_portfolio(date="14040506", portfolio_id=1)
    dividend_data = agent.get_dividend(start_date="20250601", end_date="20250729", portfolio_id=1)
    profitloss_data = agent.get_profitloss(start_date="20250601", end_date="20250729", portfolio_id=1)
