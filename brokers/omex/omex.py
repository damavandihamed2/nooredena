import jdatetime
import requests as rq

from utils import captcha_handler
from utils import auth_token_hadler


def get_api_address(address: str, api_app: str):
    api_address = api_app + "." + ".".join(address.split(".")[-2:])
    if "khobregan" in address:
        api_address = "khobregan-" + api_address
    elif "mebbco" in address:
        api_address = "mebbco-" + api_address
    else:
        pass
    return api_address

class OmexAgent:
    def __init__(self, address: str, portfolio_id: int, username: str, password: str):
        self.portfolio_id = portfolio_id
        self.auth_token_web_address = address.split("//")[1].replace("/", "")
        self.website_address = address
        self.username = username
        self.password = password
        self._user_is_login = False

        self.base_headers = {'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9',
                             'Cache-Control': 'no-cache', 'sec-ch-ua-platform': '"Windows"', 'ngsw-bypass': '',
                             'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-site',
                             'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
                             'Pragma': 'no-cache', 'sec-ch-ua-mobile': '?0', 'Connection': 'keep-alive',
                             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                           '(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                             'Origin':  f'https://{self.website_address}',
                             'Referer': f'https://{self.website_address}/'}
        self.captcha_url = f"https://{get_api_address(self.website_address, "white")}/api/Captcha/get"
        self.get_profile_url = f'https://{get_api_address(self.website_address, "delta")}/api/Customers/GetProfile'
        self.login_url = f"https://{get_api_address(self.website_address, "white")}/api/Accounts/Login"
        self.logout_url = f'https://{get_api_address(self.website_address, "white")}/api/Accounts/Logout'
        self.options_open_positions_url = f"https://{get_api_address(self.website_address, "red")}/api/optionOpenPositions/get"
        self.option_settlements_url = f"https://{get_api_address(self.website_address, "red")}/api/optionSettlements/GetHistory?$top=100&$count=true"

        self.captcha_headers = self.base_headers
        self.captcha_response = self.captcha_id =self.captcha_token = self.captcha_image = self.captcha_value = None

        self.login_headers = self.base_headers
        self.login_payload = self.loginResponseStatus = self.login_response = None

        self.logout_headers = self.logout_payload = self.logout_response = None

        self.access_token = self.headers = self.cookies = None

        self.options_response = self.option_portfolio = None
        self.option_settlements_response = self.option_settlements = None

    ################################################################

    def _get_old_token(self) -> tuple:
        token = auth_token_hadler.get_tokens(
            app="omex", web_address=self.auth_token_web_address, portfolio_id=self.portfolio_id)
        if token:
            try:
                acc_token, cookies = token["access_token"], token["cookies"]
                return acc_token, cookies
            except Exception:
                pass
        return "", {}

    def _update_token(self, access_token: str, cookies: dict) -> None:
        json_data = {"access_token": access_token, "cookies": cookies}
        auth_token_hadler.update_tokens(
            app="omex", web_address=self.auth_token_web_address, json_data=json_data, portfolio_id=self.portfolio_id)

    def check_old_tokens(self):
        access_token_, cookies_ = self._get_old_token()
        check_token_headers = {**self.base_headers, "Authorization": access_token_}
        try:
            check_token_response = rq.get(
                url=self.get_profile_url, headers=check_token_headers, cookies=cookies_)
            if check_token_response.status_code == 200:
                self._user_is_login = True
                self.loginResponseStatus = 200
                self.access_token = access_token_
                self.cookies = cookies_
                self.headers = {**self.base_headers, "Authorization": self.access_token}
            else:
                self._user_is_login = False
        except Exception as e:
            raise e

    ################################################################

    def get_captcha(self):
        self.captcha_id = str(jdatetime.datetime.now().timestamp())
        try:
            self.captcha_response = rq.get(
                url=self.captcha_url,
                headers=self.captcha_headers
            )
            if self.captcha_response.status_code == 200:
                captcha_successful = self.captcha_response.json()["response"]["successful"]
                if captcha_successful:
                    captcha_data = self.captcha_response.json()["response"]["data"]
                    self.captcha_token = captcha_data["token"]
                    self.captcha_image = captcha_data["image"]
                    captcha_handler.save_captcha(
                        captcha_type="omex",
                        captcha_id=self.captcha_id,
                        captcha_image=self.captcha_image
                    )
        except Exception as e:
            raise e


    def login(self):

        self.check_old_tokens()

        if self._user_is_login:
            pass
        else:
            self.get_captcha()
            self.captcha_value = captcha_handler.open_captcha(
                captcha_type="omex",
                captcha_id=self.captcha_id
            )
            self.login_payload = {
                "username": self.username,
                "password": self.password,
                "captchaToken": self.captcha_token,
                "captchaResponseCode": self.captcha_value
            }
            try:
                self.login_response = rq.post(
                    url=self.login_url,
                    headers=self.login_headers,
                    json=self.login_payload
                )
                if self.login_response.status_code == 200:
                    login_response_successful = self.login_response.json()["response"]["successful"]
                    if login_response_successful:
                        self.access_token = self.login_response.json()["response"]["data"]["tokenInfo"]["token"]
                        self.cookies = self.login_response.cookies.get_dict()
                        self.headers = {**self.base_headers, "Authorization": self.access_token}
                        self.loginResponseStatus = 200
                        self._user_is_login = True
                        self._update_token(access_token=self.access_token, cookies=self.cookies)
                        captcha_handler.update_captcha_value(
                            captcha_type="omex",
                            captcha_id=self.captcha_id,
                            captcha_value=self.captcha_value
                        )
                if self.login_response.status_code == 600:
                    self.loginResponseStatus = 600
                    self._user_is_login = False
                    if self.login_response.json()["response"]["errors"][0]["code"] == "CaptchaCodeIsInvalid":
                        raise UserWarning("Captcha is invalid.")
                    if self.login_response.json()["response"]["errors"][0]["code"] == "LoginException":
                        raise UserWarning("Username or Password is incorrect.")
            except Exception as e:
                raise UserWarning(f"Failed to login - {e}")

    def logout(self):
        if not self._user_is_login:
            raise UserWarning("You are not logged in!")
        else:
            try:
                self.logout_response = rq.post(
                    url=self.logout_url,
                    headers=self.headers,
                    cookies=self.cookies,
                    json={}
                )
                self._user_is_login = False
                print("You logged out successfully")
            except Exception as e:
                raise rq.HTTPError(f"Failed to logout - {e}")

    def get_option_portfolio(self):
        if not self._user_is_login:
            raise UserWarning("You are not logged in, Login First!")
        else:
            self.options_response = rq.get(url=self.options_open_positions_url, headers=self.headers, cookies=self.cookies)
            if self.options_response.status_code == 200:
                if self.options_response.json()["response"]["successful"]:
                    self.option_portfolio = self.options_response.json()["response"]["data"]
            else:
                raise rq.HTTPError(f"Failed to get \"option portfolio\" - 401")


    def get_option_settlements(self, start_date: str, end_date: str = None):
        if not self._user_is_login:
            raise UserWarning("You are not logged in, Login First!")
        else:
            start_date = jdatetime.datetime.strptime(
                start_date.replace("-", "/"), "%Y/%m/%d").togregorian().strftime("%Y-%m-%d")
            end_date = jdatetime.datetime.strptime(
                end_date.replace("-", "/"), "%Y/%m/%d").togregorian().strftime("%Y-%m-%d")
            self.option_settlements_url = self.option_settlements_url + f"&startDate={start_date}&endDate={end_date}"
            self.option_settlements_response = rq.get(url=self.option_settlements_url, headers=self.headers, cookies=self.cookies)
            if self.option_settlements_response.status_code == 200:
                if self.option_settlements_response.json()["response"]["successful"]:
                    self.option_settlements = self.option_settlements_response.json()["response"]["data"]
            else:
                raise rq.HTTPError(f"Failed to get \"option portfolio\" - 401")


if __name__ == "__main__":

    import pandas as pd
    from utils.database import insert_to_database

    authenticator = [
        {"address": "nicitrader.nibi.ir", "username": "13380113440", "password": "@Nooredena1091"},
        {"address": "patris.parsianbroker.com", "username": "44280113440", "password": "npdjA5Jmq9"},
        {"address": "khobregan.tsetab.ir", "username": "1166343216", "password": "FJG8nBJc4w"},
    ]

    option_portfolio_df = pd.DataFrame()
    option_settlements_df = pd.DataFrame()
    for i in range(len(authenticator)):
        agent = OmexAgent(
            address=authenticator[i]["address"],
            username=authenticator[i]["username"],
            password=authenticator[i]["password"]
        )
        agent.login()

        agent.get_option_portfolio()
        options_df = pd.DataFrame(agent.option_portfolio)
        option_portfolio_df = pd.concat([option_portfolio_df, options_df], axis=0, ignore_index=True)

        agent.get_option_settlements(start_date="1404/01/01", end_date="1404/11/06")
        settlements_df = pd.DataFrame(agent.option_settlements)
        option_settlements_df = pd.concat([option_settlements_df, settlements_df], axis=0, ignore_index=True)

    option_settlements_df.replace({"rMaximum": {True: 1, False: 0}, "rFraction": {True: 1, False: 0}, "rlp": {True: 1, False: 0}}, inplace=True)
    option_settlements_df.drop(columns=['firstName', 'lastName', 'nationalCode', "customerId"], inplace=True)

    insert_to_database(dataframe=option_settlements_df, database_table="[nooredenadb].[brokers].[option_settlements_omex]")
    insert_to_database(dataframe=options_df, database_table="[nooredenadb].[brokers].[option_portfolio_omex]")

