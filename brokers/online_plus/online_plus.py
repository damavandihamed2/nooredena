import requests as rq
import datetime, jdatetime

from utils import captcha_handler
from utils import auth_token_hadler



class OnlinePlusAgent:
    def __init__(self, address: str, portfolio_id: int, username: str, password: str):
        self.portfolio_id = portfolio_id
        self.website_address = address.split(".")[1]
        self.api = "api2" if "bmi" in self.website_address else "api"
        self.username = username
        self.password = password
        self.header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                     "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"}
        self._user_is_login = self.check_token_response = None

        self.url_captcha = f"https://{self.api}.{self.website_address}.ir/Web/V1/Authenticate/GetCaptchaImage/Captcha"
        self.response_captcha = self.captcha_image = self.captcha_key = self.captcha_value = self.captcha_id = None

        self.login_url = f"https://{self.api}.{self.website_address}.ir/Web/V2/Authenticate/Login"
        self.login_payload = self.login_response = self.loginResponseStatus = None
        self.auth_token = self.auth_cookie = self.ls_token = None

        self.get_data_response = self.post_data_response = self._data = None

        self.remain = self.option_portfolio = self.option_settlements = None

    ##################################################

    def _get_old_token(self) -> str:
        tokens = auth_token_hadler.get_tokens(app="online_plus", web_address=self.website_address, portfolio_id=self.portfolio_id)
        if tokens:
            try:
                cookies = tokens["cookies"]
                return cookies
            except Exception:
                pass
        return ""

    def _update_token(self, cookies: str) -> None:
        json_data = {"cookies": cookies}
        auth_token_hadler.update_tokens(app="online_plus", web_address=self.website_address, json_data=json_data, portfolio_id=self.portfolio_id)

    def check_old_tokens(self):
        cookies_ = self._get_old_token()
        try:
            self.check_token_response = rq.get(url=f"https://{self.api}.{self.website_address}.ir/Web/V1/Accounting/Remain",
                                               headers=self.header,cookies={'AuthCookie_OnlineCookie': cookies_})
            if self.check_token_response.status_code == 200:
                self._user_is_login = True
                self.loginResponseStatus = 200
                self.auth_cookie = cookies_
                self.remain = self.check_token_response.json()["Data"]
            else:
                self._user_is_login = False
        except Exception as e:
            raise e

    ##################################################

    def _get_data(self, url: str, **kwargs) -> None:
        if not self._user_is_login:
            raise UserWarning("You are not logged in, Login First!")
        try:
            self.get_data_response = rq.get(
                url=url, headers=self.header, cookies={'AuthCookie_OnlineCookie': self.auth_cookie}, **kwargs)
            if (self.get_data_response.status_code == 200) and self.get_data_response.json()["IsSuccessfull"]:
                self._data = self.get_data_response.json()
        except Exception as e:
            raise Exception(f"Failed to get Data - {e}")

    def _post_data(self, url: str, **kwargs) -> None:
        if not self._user_is_login:
            raise UserWarning("You are not logged in, Login First!")
        try:
            self.post_data_response = rq.post(
                url=url, headers=self.header, cookies={'AuthCookie_OnlineCookie': self.auth_cookie}, **kwargs)
            if (self.post_data_response.status_code == 200) and self.post_data_response.json()["IsSuccessfull"]:
                self._data = self.post_data_response.json()
        except Exception as e:
            raise Exception(f"Failed to get Data - {e}")

    ##################################################

    def get_captcha(self):
        self.captcha_id = str(datetime.datetime.now().timestamp())
        self.response_captcha = rq.get(self.url_captcha)
        if self.response_captcha.status_code == 200 and (self.response_captcha.json()["IsSuccessfull"]):
            self.captcha_image = self.response_captcha.json()["Data"]["Captcha"]
            self.captcha_key = self.response_captcha.json()["Data"]["CaptchaKey"]
            captcha_handler.save_captcha(
                captcha_type="online_plus",
                captcha_id=self.captcha_id,
                captcha_image=self.captcha_image
            )

    def login(self):
        self.check_old_tokens()
        if self._user_is_login:
            pass
        else:
            self.get_captcha()
            self.captcha_value = captcha_handler.open_captcha(captcha_type="online_plus", captcha_id=self.captcha_id)
            self.login_payload = {"UserName": self.username, "Password": self.password,
                                  "CaptchaKey": self.captcha_key, "Captcha": self.captcha_value}
            try:
                self.login_response = rq.post(
                    url=f"https://{self.api}.{self.website_address}.ir/Web/V2/Authenticate/Login",
                    json=self.login_payload,
                    headers=self.header
                )
                if (self.login_response.status_code == 200) and self.login_response.json()["IsSuccessfull"]:
                    self.auth_token = self.login_response.json()["Data"]["Token"]
                    self.ls_token = self.login_response.json()["Data"]["LsToken"]
                    self.auth_cookie = self.login_response.headers["Set-Cookie"].split(
                        "AuthCookie_OnlineCookie=")[1].split(";")[0]
                captcha_handler.update_captcha_value(
                    captcha_type="online_plus",
                    captcha_id=self.captcha_id,
                    captcha_value=self.captcha_value
                )
                self._update_token(cookies=self.auth_cookie)
                self._user_is_login = True
                self.loginResponseStatus = 200
            except Exception as e:
                raise Exception(f"Failed to login - {e}")

    ##################################################

    def get_option_portfolio(self):
        self._post_data(url=f"https://{self.api}.{self.website_address}.ir/Web/V1/Customer/GetCustomerSummaryList", json={})
        self.option_portfolio = self._data["Data"]

    def get_option_settlements(self, start_data: str, end_date: str, page_number: int = 0, page_size: int = 500):
        s_d = jdatetime.datetime.strptime(start_data, "%Y/%m/%d").togregorian().strftime("%Y-%m-%d")
        e_d = jdatetime.datetime.strptime(end_date, "%Y/%m/%d").togregorian().strftime("%Y-%m-%d")
        params = {'FromDate': s_d, 'ToDate': e_d, 'PageNumber': f'{page_number}', 'PageSize': f'{page_size}'}
        self._get_data(
            url=f"https://{self.api}.{self.website_address}.ir/Web/V1/Customer/GetCustomerOptionSettlmentReports",
            params=params)
        self.option_settlements = self._data["Data"]



if __name__ == "__main__":

    import pandas as pd
    from utils.database import insert_to_database

    agent = OnlinePlusAgent(
        address="mellatbroker",
        username="mot30000949",
        password="fx3mn!kzK"
    )
    agent.login()

    agent.get_option_portfolio()
    if agent.option_portfolio:
        option_portfolio = agent.option_portfolio["Data"]
        option_portfolio_df = pd.DataFrame(option_portfolio)
        insert_to_database(dataframe=option_portfolio_df,
                           database_table="[nooredenadb].[brokers].[option_portfolio_online_plus]")

    agent.get_option_settlements(start_data="1404/01/01", end_date="1404/11/06")
    if agent.option_portfolio:
        option_settlements = agent.option_settlements["Data"]
        option_settlements_df = pd.DataFrame(option_settlements)
        insert_to_database(dataframe=option_settlements_df,
                           database_table="[nooredenadb].[brokers].[option_settlements_online_plus]")
