import warnings
import jdatetime
from pyodbc import Connection
import requests as rq
from bs4 import BeautifulSoup
from typing import (
    Literal,
    Optional
)

from utils import captcha_handler
from sigma.crawler.tools.fetch_tools import (
    sigma_ip,

    base_header,
    base_header_2,
    headers_portfolio,

    payload_login,
    payload_dividend,
    payload_buysell,
    payload_profitloss,

    date_input_handler,
    get_cookies,
    update_cookies,
    report_data_parser
)



warnings.filterwarnings("ignore")


class Agent:
    report_types = ["portfolio", "dividend", "buysell", "profitloss"]

    def __init__(self, username:str, password: str, db_connection: Connection) -> None:

        self.username = username
        self.password = password
        self.db_connection = db_connection

        self.status = False

        self.login_page_response = None
        self.login_page_payload_token = None
        self.header_token = None
        self.response_old_token = None

        self.captcha_guid = self.captcha_id = self.captcha_value = None

        self.login_response = None
        self.session_token = None
        self.antiforgery = None


    def __login_page(self) -> None:
        self.login_page_response = rq.get(url=f"http://{sigma_ip}/login")
        self.login_page_inputs = BeautifulSoup(
            self.login_page_response.text, features="html.parser").find_all(name="input", type="hidden")
        self.key_value = {}
        for i in range(len(self.login_page_inputs)):
            self.key_value[self.login_page_inputs[i].get("name")] = self.login_page_inputs[i].get("value")
        self.captcha_guid = self.key_value["captcha-guid"]
        self.login_page_payload_token = self.key_value["__RequestVerificationToken"]
        self.header_token = self.login_page_response.cookies.get_dict()
        self.antiforgery = self.header_token.get(".AspNetCore.Antiforgery.k3DVaEHqH6g")


    def __check_old_tokens(self):
        cookies = get_cookies()
        sessiontoken = cookies.get("session_token")
        antiforgery = cookies.get(".AspNetCore.Antiforgery.k3DVaEHqH6g")
        url = f"http://{sigma_ip}/spa/reports/portfolio"
        headers = {**base_header_2, 'Cookie': f'SessionToken={sessiontoken}; .AspNetCore.Antiforgery.k3DVaEHqH6g={antiforgery}'}
        self.response_old_token = rq.get(url=url, headers=headers)
        if self.response_old_token.status_code == 200:
            if self.response_old_token.url == url:
                self.status = True
                self.session_token = sessiontoken
                self.antiforgery = antiforgery


    def __get_report_page(
            self,
            url: str,
            payload_token_index: Literal[0, 1]
    ) -> str:

        if payload_token_index not in [0, 1]:
            raise ValueError("payload_token_index must be 0 or 1")

        headers = {
            **base_header_2,
            'Referer': f"http://{sigma_ip}/Reports/Portfolio/OverView?portfolioId=1&"
                       f"dateTime={jdatetime.datetime.today().strftime("%Y%2F%m%2F%d")}"
        }
        cookies = {
            '.AspNetCore.Antiforgery.k3DVaEHqH6g': self.antiforgery,
            'SessionToken': self.session_token,
        }
        self.response_payload_token = rq.get(url=url, headers=headers, cookies=cookies)

        inputs = BeautifulSoup(self.response_payload_token.text, features="html.parser").find_all(
            name="input", attrs={"name": "__RequestVerificationToken"})
        payload_token = inputs[payload_token_index].attrs["value"]

        return payload_token


    def get_captcha(self) -> None:

        self.captcha_id = str(jdatetime.datetime.now().timestamp())

        url = f"http://{sigma_ip}/captcha" + f"?guid={self.captcha_guid}"
        captcha_headers = {**base_header, 'Referer': f'http://{sigma_ip}/login?ReturnUrl=%2f'}
        captcha_response = rq.get(url=url, headers=captcha_headers)

        captcha_handler.save_captcha(
            captcha_type="sigma",
            captcha_image=captcha_response.content,
            captcha_id=self.captcha_id,
            b64decode=False,
            db_connection=self.db_connection
        )

        captcha_value = captcha_handler.open_captcha(
            captcha_type="sigma",
            captcha_id=self.captcha_id,
            db_connection=self.db_connection
        )

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

        payload = payload_login.format(username=self.username, password=self.password, captcha_value=self.captcha_value,
                                       captcha_guid=self.captcha_guid, payload_token=self.login_page_payload_token)
        headers = {**base_header_2,
                   "Cookie": f".AspNetCore.Antiforgery.k3DVaEHqH6g={self.antiforgery}",
                   'Referer': url}
        self.login_response = rq.post(url=url, headers=headers, data=payload)

        if (self.login_response.status_code == 200) and (self.login_response.url == f"http://{sigma_ip}/"):
            captcha_handler.update_captcha_value(
                captcha_type="sigma",
                captcha_id=self.captcha_id,
                captcha_value=self.captcha_value,
                db_connection = self.db_connection
            )

            sessiontoken = self.login_response.history[0].cookies["SessionToken"]
            antiforgery = self.header_token.get(".AspNetCore.Antiforgery.k3DVaEHqH6g", None)

            self.session_token = sessiontoken
            self.antiforgery = antiforgery

            update_cookies(cookies_dict={"session_token": self.session_token,
                                         ".AspNetCore.Antiforgery.k3DVaEHqH6g": self.antiforgery})
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
            raise PermissionError("You are NOT logged in, please login first!")

        date = date_input_handler(input_date=date)

        url = f"http://{sigma_ip}/api/v3/reports/asset/portfolio-status"
        cookies = {".AspNetCore.Antiforgery.k3DVaEHqH6g": self.antiforgery, 'SessionToken': self.session_token}
        params = {'end_date': date.replace("/", "").replace("-", "") + "033000", 'separate_states': 'true',
                  'separate_portfolios': 'false', 'portfolio_ids': f'{portfolio_id}'}
        response = rq.get(url=url, params=params, cookies=cookies, headers=headers_portfolio, verify=False)

        data = response.json()["data"]["items"]

        for item in data:
            item.pop("portfolio_ids", None)
            item.pop("watchlist_ids", None)
            if isinstance(item.get("state"), dict):
                item["state"] = item["state"].get("display")

        return data


    def get_dividend(self, start_date, end_date, portfolio_id: int = 1) -> Optional[list[dict]]:

        if not self.status:
            raise PermissionError("You are NOT logged in, please login first!")

        start_date = date_input_handler(input_date=start_date).replace("/", "%2F")
        end_date = date_input_handler(input_date=end_date).replace("/", "%2F")
        url = f"http://{sigma_ip}/Reports/Stock/ReceivedDividendEvents"
        payload = payload_dividend.format(start_date=start_date, end_date=end_date, portfolio_id=portfolio_id)
        headers = {
            **base_header_2,
            'Cookie': f'.AspNetCore.Antiforgery.k3DVaEHqH6g={self.antiforgery}; SessionToken={self.session_token}',
            'Referer': url
        }
        response = rq.get(url=url, data=payload, headers=headers)
        data = report_data_parser(url=f"{url}?{payload}", response=response, report_type="dividend")
        return data


    def get_buysell(self, start_date, end_date, portfolio_id: int = 1) -> Optional[list[dict]]:

        if not self.status:
            raise PermissionError("You are NOT logged in, please login first!")

        start_date = date_input_handler(input_date=start_date).replace("/", "%2F")
        end_date = date_input_handler(input_date=end_date).replace("/", "%2F")

        payload_token = self.__get_report_page(
            url=f"http://{sigma_ip}/Reports/Asset/BuySellReportEvents", payload_token_index=0
        )
        url = f"http://{sigma_ip}/Reports/Asset/BuySellReportEvents"
        headers = {**base_header_2, 'Referer': f'http://{sigma_ip}/Reports/Asset/BuySellReportEvents'}
        cookies = {'.AspNetCore.Antiforgery.k3DVaEHqH6g': self.antiforgery, 'SessionToken': self.session_token}
        payload = payload_buysell.format(
            portfolio_id=portfolio_id, payload_token=payload_token,
            start_date=start_date.replace("/", "%2F"), end_date=end_date.replace("/", "%2F")
        )
        response_buysell = rq.post(url=url, headers=headers, data=payload, cookies=cookies, verify=False)

        data = report_data_parser(url=url, response=response_buysell, report_type="buysell")

        return data


    def get_profitloss(self, start_date, end_date, portfolio_id = 1) -> Optional[list[dict]]:

        if not self.status:
            raise PermissionError("You are NOT logged in, please login first!")

        url = f"http://{sigma_ip}/Reports/Asset/ProfitLossEvents/ProfitLoss"
        headers = {
            **base_header_2,
            'Cookie': f'.AspNetCore.Antiforgery.k3DVaEHqH6g={self.antiforgery}; SessionToken={self.session_token}',
            'Referer': url
        }
        payload = payload_profitloss.format(
            start_date=date_input_handler(input_date=start_date).replace("/", "%2F"),
            end_date=date_input_handler(input_date=end_date).replace("/", "%2F"),
            portfolio_id=portfolio_id
        )
        response = rq.post(url=url, headers=headers, data=payload)

        data = report_data_parser(url=url, response=response, report_type="profitloss")

        return data


if __name__ == '__main__':


    from utils.database import make_connection

    db_conn = make_connection()

    if db_conn:

        authenticator = [{"username": "damavandi", "password": "Dd@123456"}]

        agent = Agent(
            username=authenticator[0]["username"],
            password=authenticator[0]["password"],
            db_connection=db_conn
        )
        agent.login()

        buysell_data = agent.get_buysell(start_date="20250601", end_date="20250729", portfolio_id=1)
        portfolio_data = agent.get_portfolio(date="14040506", portfolio_id=1)
        dividend_data = agent.get_dividend(start_date="20250601", end_date="20250729", portfolio_id=1)
        profitloss_data = agent.get_profitloss(start_date="20250601", end_date="20250729", portfolio_id=1)
